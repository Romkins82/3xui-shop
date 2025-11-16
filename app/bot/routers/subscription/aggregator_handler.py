import asyncio
import base64
import logging
import ssl
import urllib.parse
from typing import TYPE_CHECKING
import json 

import aiohttp
from aiohttp import TCPConnector
from aiohttp.web import Request, Response, RouteTableDef
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from app.config import Config
from app.db.models import User

if TYPE_CHECKING:
    from app.bot.services import ServerPoolService, VPNService, ServicesContainer
    from app.bot.models import ClientData

logger = logging.getLogger(__name__)
routes = RouteTableDef()


def build_response_headers(encoded_title: str, user_info_header: str = None) -> dict:
    """Создаёт заголовки, которые заставляют Happ и аналогичные клиенты скрывать настройки серверов."""
    headers = {
        # Название профиля
        'Profile-Title': f'base64:{encoded_title}',

        # 🔒 НОВЫЙ ЗАГОЛОВОК для принудительного сворачивания списка
        'Profile-Collapse': 'true',

        # 🔒 Полный запрет редактирования и видимости
        'Profile-Editable': 'false',
        'Profile-Config-Editable': 'false',
        'Profile-Proxies-Editable': 'false',
        'Profile-Rules-Editable': 'false',
        'Profile-Update-Editable': 'false',
        'Editable': 'false',
        'Profile-Visible': 'false',
        'Profile-Show-Detail': 'false',
        'Profile-Show-Config': 'false',
        'Profile-Hidden': 'true',

        # 🔒 Специальные флаги для Happ / Hiddify / Stash
        'X-Hiddify-Profile-Hidden': 'true',
        'X-Hiddify-Locked': 'true',
        'X-Hiddify-Editable': 'false',

        # 🧱 Ограничиваем кэш
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0',

        # Интервал обновления
        'Profile-Update-Interval': '120',

        # Тип профиля
        'Profile-Type': 'subscription',

        # Заголовки CORS
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
    }

    if user_info_header:
        headers['Subscription-Userinfo'] = user_info_header

    return headers




@routes.get("/sub/{vpn_id}")
async def get_aggregated_subscription(request: Request) -> Response:
    try:
        vpn_id = request.match_info.get("vpn_id")
        if not vpn_id:
            return Response(status=400, text="vpn_id is missing")

        session_factory: async_sessionmaker = request.app["session_maker"]
        server_pool: "ServerPoolService" = request.app["server_pool"]
        services: "ServicesContainer" = request.app["services_container"]
        vpn_service: "VPNService" = services.vpn

        # Получаем пользователя
        async with session_factory() as session:
            result = await session.execute(select(User).where(User.vpn_id == vpn_id))
            user = result.scalar_one_or_none()
            if not user:
                return Response(status=404, text="Subscription not found")

        # Получаем информацию о пользователе
        client_data: "ClientData" = await vpn_service.get_client_data(user)

        user_info_header = ""
        if client_data:
            upload = client_data._traffic_up
            download = client_data._traffic_down
            total = client_data._traffic_total # Это будет -1 для безлимита
            expire_ts = client_data._expiry_timestamp

            # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
            # Если total (из _traffic_total) > 0, используем его.
            # Если total = -1 (или 0), что означает "безлимит", 
            # мы должны отправить 0 в заголовке,
            # так как 0 - это стандарт для "безлимита" в Subscription-Userinfo.
            total_for_header = total if total > 0 else 0
            # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

            user_info_parts = [
                f"upload={upload}",
                f"download={download}",
                f"total={total_for_header}",
            ]

            if expire_ts > 0:
                user_info_parts.append(f"expire={expire_ts // 1000}")

            user_info_header = "; ".join(user_info_parts)

        # Получаем список активных серверов
        servers = await server_pool.get_available_servers()
        if not servers:
            return Response(status=503, text="No available servers")

        # Асинхронно собираем конфиги
        tasks = []
        timeout = aiohttp.ClientTimeout(total=10)

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as http_session:
            for server in servers:
                base_url_parts = urllib.parse.urlsplit(server.host)
                correct_netloc = f"{base_url_parts.hostname}:2096"
                subscription_path = f"/sub/{vpn_id}"
                final_sub_url = urllib.parse.urlunsplit(
                    (base_url_parts.scheme, correct_netloc, subscription_path, '', '')
                )

                logger.info(f"Requesting from: {final_sub_url}")
                tasks.append(asyncio.create_task(http_session.get(final_sub_url)))

            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_configs = []
        for i, res in enumerate(results):
            current_server = servers[i]

            if isinstance(res, aiohttp.ClientResponse) and res.status == 200:
                try:
                    content_bytes = await res.read()
                    if not content_bytes:
                        logger.warning(f"Server {current_server.name} returned an empty response body.")
                        continue

                    raw_content = content_bytes.decode('utf-8', errors='ignore').strip()
                    decoded_content = ""

                    try:
                        decoded_content = base64.b64decode(raw_content).decode('utf-8', errors='ignore').strip()
                        logger.info(f"Content from {current_server.name} was Base64.")
                    except Exception:
                        logger.info(f"Content from {current_server.name} is not Base64. Treating as plain text.")
                        decoded_content = raw_content

                    # --- НАЧАЛО БЛОКА ИСПРАВЛЕНИЙ (ПОПЫТКА 5) ---
                    if decoded_content:
                        logger.info(f"Successfully processed content from {current_server.name}.")
                        config_lines = decoded_content.splitlines()
                        for line in config_lines:
                            line = line.strip()
                            if not line:
                                continue
                            
                            new_name = current_server.name  # Имя, которое мы хотим установить

                            if line.startswith("vless://") or line.startswith("trojan://"):
                                # VLESS / Trojan
                                base_line = line.rsplit('#', 1)[0]
                                
                                params = []
                                query_string = ""
                                base_line_no_query = base_line

                                if "?" in base_line:
                                    base_line_no_query, query_string = base_line.split("?", 1)
                                    params = [p for p in query_string.split("&") if p] # Сохраняем существующие
                                
                                # Фильтруем существующие параметры
                                params = [
                                    p for p in params 
                                    if not p.startswith("allowInsecure=") and 
                                       not p.startswith("allow_insecure=") and
                                       not p.startswith("skip-cert-verify=")
                                ]
                                
                                # Добавляем все варианты
                                params.append("allowInsecure=true")
                                params.append("allow_insecure=true")
                                params.append("skip-cert-verify=true") # Добавляем еще один!
                                
                                # Собираем обратно
                                base_line = f"{base_line_no_query}?{'&'.join(params)}"

                                all_configs.append(f"{base_line}#{new_name}")

                            elif line.startswith("vmess://"):
                                # VMESS: Имя - это ключ "ps" внутри Base64
                                try:
                                    base64_part = line[len("vmess://"):]
                                    padding = '=' * (4 - len(base64_part) % 4)
                                    decoded_json_str = base64.b64decode(base64_part + padding).decode('utf-8')
                                    
                                    config_json = json.loads(decoded_json_str)
                                    
                                    # --- НАЧАЛО НОРМАЛИЗАЦИИ (из pipeline.py) ---
                                    
                                    # 1. Меняем 'ps' (имя)
                                    config_json["ps"] = new_name
                                    
                                    # 2. Нормализуем 'port' в строку
                                    if "port" in config_json:
                                        config_json["port"] = str(config_json["port"])
                                    
                                    # 3. Нормализуем 'allowInsecure' в 1 (int), а не true (bool)
                                    allow_insecure_val = config_json.get("allowInsecure", False)
                                    if isinstance(allow_insecure_val, str):
                                        allow_insecure_val = allow_insecure_val.strip().lower() in {"1", "true", "yes", "y", "on"}
                                    
                                    # ВСЕГДА СТАВИМ 1, как в DEFAULTS_IF_MISSING
                                    config_json["allowInsecure"] = 1
                                    
                                    # 4. Добавляем недостающие поля, как в DEFAULTS_IF_MISSING
                                    if "aid" not in config_json:
                                        config_json["aid"] = "0"
                                    if "alpn" not in config_json:
                                        config_json["alpn"] = ""
                                    if "type" not in config_json:
                                        config_json["type"] = ""
                                        
                                    # 5. Убедимся, что 'v' на месте
                                    if "v" not in config_json:
                                        config_json["v"] = "2"
                                        
                                    # --- КОНЕЦ НОРМАЛИЗАЦИИ ---

                                    # 6. (КРИТИЧЕСКИ ВАЖНО) Сортируем ключи, как в pipeline.py
                                    wanted_order = [
                                        "add","aid","allowInsecure","alpn","fp","host","id","net","path",
                                        "port","ps","scy","sni","tls","type","v"
                                    ]
                                    
                                    ordered_config = {}
                                    # Сначала ключи из wanted_order
                                    for k in wanted_order:
                                        if k in config_json:
                                            ordered_config[k] = config_json[k]
                                    
                                    # Добавляем остальные ключи, которых нет в списке
                                    for k, v in config_json.items():
                                        if k not in ordered_config:
                                            ordered_config[k] = v
                                    
                                    # 7. Кодируем отсортированный JSON
                                    new_json_str = json.dumps(ordered_config, ensure_ascii=False, separators=(",", ":"))
                                    new_base64_part = base64.b64encode(new_json_str.encode('utf-8')).decode('utf-8').rstrip('=')
                                    
                                    all_configs.append(f"vmess://{new_base64_part}")
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process vmess link for {new_name}: {e}. Appending name as fallback.")
                                    base_line = line.rsplit('#', 1)[0]
                                    all_configs.append(f"{base_line}#{new_name}")
                            else:
                                # Резервный вариант для других типов
                                base_line = line.rsplit('#', 1)[0]
                                all_configs.append(f"{base_line}#{new_name}")
                    # --- КОНЕЦ БЛОКА ИСПРАВЛЕНИЙ ---
                    else:
                        logger.warning(f"Decoded content from {current_server.name} is empty after processing.")

                except Exception as e:
                    logger.error(f"Critical error processing content from {current_server.name}: {e}")
            else:
                logger.error(f"Failed to fetch config from server {current_server.name}: {res}")

        if not all_configs:
            return Response(status=500, text="Could not fetch any valid configuration")

        # Объединяем все конфиги
        final_subscription = "\n".join(all_configs)
        final_subscription_base64 = base64.b64encode(final_subscription.encode('utf-8')).decode('utf-8')

        subscription_title = "FreeNet VPN"
        encoded_title = base64.b64encode(subscription_title.encode('utf-8')).decode('utf-8')

        # Универсальные заголовки
        response_headers = build_response_headers(encoded_title, user_info_header)

        # Финальный ответ
        return Response(
            status=200,
            text=final_subscription_base64,
            content_type="text/plain",
            headers=response_headers
        )

    except Exception as e:
        logger.exception(f"FATAL unhandled error in aggregator: {e}")
        return Response(status=500, text="Internal Server Error")