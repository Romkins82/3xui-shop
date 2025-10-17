import asyncio
import base64
import logging
import ssl
import urllib.parse
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import TCPConnector
from aiohttp.web import Request, Response, RouteTableDef
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from app.config import Config
from app.db.models import User

if TYPE_CHECKING:
    from app.bot.services import ServerPoolService

logger = logging.getLogger(__name__)
routes = RouteTableDef()


@routes.get("/sub/{vpn_id}")
async def get_aggregated_subscription(request: Request) -> Response:
    try:
        vpn_id = request.match_info.get("vpn_id")
        if not vpn_id:
            return Response(status=400, text="vpn_id is missing")

        session_factory: async_sessionmaker = request.app["session_maker"]
        server_pool: "ServerPoolService" = request.app["server_pool"]
        
        async with session_factory() as session:
            result = await session.execute(select(User).where(User.vpn_id == vpn_id))
            user = result.scalar_one_or_none()
            if not user:
                return Response(status=404, text="Subscription not found")

        servers = await server_pool.get_available_servers()
        if not servers:
            return Response(status=503, text="No available servers")

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
                        
                    raw_content = content_bytes.decode('utf-8', errors='ignore')
                    decoded_content = ""

                    try:
                        decoded_content = base64.b64decode(raw_content).decode('utf-8', errors='ignore').strip()
                        logger.info(f"Content from {current_server.name} was Base64.")
                    except Exception:
                        logger.info(f"Content from {current_server.name} is not Base64. Treating as plain text.")
                        decoded_content = raw_content.strip()

                    if decoded_content:
                        logger.info(f"SUCCESSFULLY processed content from {current_server.name}.")
                        config_lines = decoded_content.splitlines()
                        for line in config_lines:
                            base_line = line.split('#')[0]
                            all_configs.append(f"{base_line}#{current_server.name}")
                    else:
                        logger.warning(f"Decoded content from {current_server.name} is empty after processing.")

                except Exception as e:
                    logger.error(f"CRITICAL ERROR processing content from {current_server.name}: {e}")
            else:
                logger.error(f"FAILED to fetch config from server {current_server.name}: {res}")

        if not all_configs:
            return Response(status=500, text="Could not fetch any valid configuration")

        final_subscription = "\n".join(all_configs)
        final_subscription_base64 = base64.b64encode(final_subscription.encode('utf-8')).decode('utf-8')

        # --- НОВЫЙ КОД: Добавляем название подписки ---
        subscription_title = "FreeNet VPN"  # <-- Можете изменить это название
        encoded_title = base64.b64encode(subscription_title.encode('utf-8')).decode('utf-8')
        response_headers = {
            'Profile-Title': f'base64:{encoded_title}'
        }
        # -------------------------------------------

        return Response(
            status=200, 
            text=final_subscription_base64, 
            content_type="text/plain",
            headers=response_headers  # <-- Добавляем заголовки в ответ
        )

    except Exception as e:
        logger.exception(f"FATAL unhandled error in aggregator: {e}")
        return Response(status=500, text="Internal Server Error")