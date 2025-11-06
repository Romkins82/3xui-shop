# app/bot/services/vpn.py
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Optional # Добавлен Optional
from contextlib import nullcontext # Добавить импорт

from py3xui import Client, Inbound
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker # Добавлен AsyncSession

from app.bot.models import ClientData
from app.bot.utils.formatting import format_remaining_time
from app.bot.utils.time import (
    add_days_to_timestamp,
    days_to_timestamp,
    get_current_timestamp,
)
from app.config import Config
from app.db.models import User, Promocode

if TYPE_CHECKING:
    from .server_pool import Connection, ServerPoolService # Добавлен Connection

logger = logging.getLogger(__name__)


class VPNService:
    def __init__(
        self,
        config: Config,
        session: async_sessionmaker,
        server_pool_service: ServerPoolService,
    ) -> None:
        self.config = config
        self.session = session
        self.server_pool_service = server_pool_service
        logger.info("VPN Service initialized.")

    async def _find_client_on_server(self, user: User, connection: Connection) -> Client | None: # Уточнен тип connection
        """
        Надежный метод для поиска клиента на конкретном сервере.
        Итерирует по всем клиентам всех инбаундов.
        """
        try:
            inbounds = await connection.api.inbound.get_list()
            for inbound in inbounds:
                # ИСПРАВЛЕНО: Получаем клиентов напрямую из настроек инбаунда.
                if hasattr(inbound.settings, 'clients') and inbound.settings.clients:
                    for client in inbound.settings.clients:
                        # ИСПРАВЛЕНИЕ: Искать по email (tg_id), а не по vpn_id
                        if client.email == str(user.tg_id):
                            logger.debug(f"Client {user.tg_id} found on server {connection.server.name} in inbound {inbound.id}")
                            # Добавляем inbound_id к объекту клиента для удобства
                            client.inbound_id = inbound.id
                            return client
        except Exception as e:
            logger.error(f"Error while searching for client {user.tg_id} on server {connection.server.name}: {e}")
        return None

    async def is_client_exists(self, user: User, session: Optional[AsyncSession] = None) -> Client | None: # Добавлен session
        """Проверяет наличие клиента хотя бы на одном из серверов."""
        # Используем переданную сессию, если она есть, иначе создаем новую
        use_session = session if session else self.session()
        _session_context = use_session if not session else nullcontext(use_session)

        async with _session_context as active_session: # Используем async with для сессии
            servers = await self.server_pool_service.get_all_servers(session=active_session) # Передаем сессию
            for server in servers:
                connection = await self.server_pool_service.get_connection_by_server_id(server.id, session=active_session) # Передаем сессию
                if not connection:
                    continue
                # Передаем пользователя и соединение
                client = await self._find_client_on_server(user, connection)
                if client:
                    return client

        logger.warning(f"Client {user.tg_id} not found on any server.")
        return None

    async def get_limit_ip(self, user: User, client: Client, session: Optional[AsyncSession] = None) -> int | None: # Добавлен session
        connection = await self.server_pool_service.get_connection(user, session=session) # Передаем сессию
        if not connection:
            servers = await self.server_pool_service.get_all_servers(session=session) # Передаем сессию
            if not servers: return None
            connection = await self.server_pool_service.get_connection_by_server_id(servers[0].id, session=session) # Передаем сессию

        if not connection: return None

        try:
            inbounds: list[Inbound] = await connection.api.inbound.get_list()
        except Exception as exception:
            logger.error(f"Failed to fetch inbounds: {exception}")
            return None

        for inbound in inbounds:
            if hasattr(inbound.settings, 'clients') and inbound.settings.clients:
                for inbound_client in inbound.settings.clients:
                    if inbound_client.email == client.email:
                        logger.debug(f"Client {client.email} limit ip: {inbound_client.limit_ip}")
                        return inbound_client.limit_ip

        logger.critical(f"Client {client.email} not found in inbounds on server {connection.server.name}.")
        return None


    async def get_client_data(self, user: User, session: Optional[AsyncSession] = None) -> ClientData | None: # Добавлен session
        logger.debug(f"Starting to retrieve client data for {user.tg_id}.")

        client: Client | None = None

        if user.server_id:
            connection = await self.server_pool_service.get_connection(user, session=session) # Передаем сессию
            if connection:
                client = await self._find_client_on_server(user, connection)
                if not client:
                    logger.warning(f"Client {user.tg_id} not found on primary server {connection.server.name}. Searching on others.")

        if not client:
            client = await self.is_client_exists(user, session=session) # Передаем сессию

        if not client:
            logger.info(f"Could not get client data: Client {user.tg_id} not found on ANY server.")
            return None

        try:
            limit_ip = await self.get_limit_ip(user=user, client=client, session=session) # Передаем сессию
            max_devices = -1 if limit_ip == 0 else limit_ip
            traffic_total = client.total
            expiry_time = -1 if client.expiry_time == 0 else client.expiry_time
            if traffic_total <= 0:
                traffic_remaining = -1
                traffic_total = -1
            else:
                traffic_remaining = client.total - (client.up + client.down)
            traffic_used = client.up + client.down
            client_data = ClientData(
                max_devices=max_devices,
                traffic_total=traffic_total,
                traffic_remaining=traffic_remaining,
                traffic_used=traffic_used,
                traffic_up=client.up,
                traffic_down=client.down,
                expiry_timestamp=expiry_time,
                expiry_time_str=format_remaining_time(expiry_time),
            )
            logger.debug(f"Successfully retrieved client data for {user.tg_id}: {client_data}.")
            return client_data
        except Exception as exception:
            logger.error(f"Error processing client data for {user.tg_id}: {exception}")
            return None

    async def get_subscription_url(self, user: User) -> str | None:
        if not user.vpn_id:
            logger.warning(f"User {user.tg_id} has no vpn_id to generate subscription URL.")
            return None

        sub_url = f"{self.config.bot.DOMAIN}/sub/{user.vpn_id}"
        logger.debug(f"Generated subscription URL for {user.tg_id}: {sub_url}")
        return sub_url

    async def _perform_action_on_all_servers(self, user: User, action: str, **kwargs) -> tuple[int, int | None]:
        # Используем сессию по умолчанию, так как этот метод вызывается из других, уже имеющих сессию
        async with self.session() as session:
            servers = await self.server_pool_service.get_all_servers(session=session)
            if not servers:
                logger.error(f"No servers available to perform '{action}' for user {user.tg_id}.")
                return 0, None

            successful_ops = 0
            first_success_server_id = None

            for server in servers:
                connection = await self.server_pool_service.get_connection_by_server_id(server.id, session=session)
                if not connection:
                    logger.warning(f"No connection for server {server.name}, skipping action '{action}'.")
                    continue

                try:
                    client_exists = await self._find_client_on_server(user, connection)

                    if action == 'create':
                        if client_exists:
                            logger.warning(f"Client {user.tg_id} already exists on {server.name}. Forcing update.")
                            update_payload = kwargs['client_settings']
                            await connection.api.client.update(client_uuid=client_exists.sub_id, client=update_payload)
                        else:
                            inbound_id = await self.server_pool_service.get_inbound_id(connection.api)
                            if not inbound_id:
                                raise Exception("No inbound ID found")
                            await connection.api.client.add(inbound_id=inbound_id, clients=[kwargs['client_settings']])

                    elif action == 'update':
                        if client_exists:
                            update_payload = kwargs['update_payload_func'](client_exists)
                            await connection.api.client.update(client_uuid=client_exists.sub_id, client=update_payload)
                        else:
                            logger.warning(f"Client {user.tg_id} not found on {server.name} to update. Creating it instead.")
                            inbound_id = await self.server_pool_service.get_inbound_id(connection.api)
                            if not inbound_id: raise Exception("No inbound ID for creation during update")
                            # Создаем настройки из функции обновления, но для НОВОГО клиента
                            client_settings = kwargs['update_payload_func'](Client(email=str(user.tg_id)))
                            # Устанавливаем ID и sub_id из пользователя
                            client_settings.id = user.vpn_id
                            client_settings.sub_id = user.vpn_id
                            await connection.api.client.add(inbound_id=inbound_id, clients=[client_settings])

                    elif action == 'delete':
                        if client_exists:
                            if not hasattr(client_exists, 'inbound_id') or not client_exists.inbound_id:
                                raise Exception("Cannot delete client: inbound_id is missing.")
                            await connection.api.client.delete(inbound_id=client_exists.inbound_id, client_uuid=client_exists.sub_id)
                        else:
                            logger.warning(f"Client {user.tg_id} not found on {server.name}, skipping delete (already deleted).")

                    logger.info(f"Action '{action}' for user {user.tg_id} on server {server.name} successful.")
                    successful_ops += 1
                    if not first_success_server_id:
                        first_success_server_id = server.id
                except Exception as e:
                    logger.error(f"Action '{action}' for user {user.tg_id} on server {server.name} failed: {e}")

            return successful_ops, first_success_server_id

    async def create_client(self, user: User, devices: int, duration: int, **kwargs) -> User | None:
        """
        Создает клиента на всех серверах и возвращает обновленный объект User с vpn_id.
        """
        temp_vpn_id = str(uuid.uuid4())
        client_settings = Client(
            email=str(user.tg_id),
            enable=True,
            id=temp_vpn_id,
            expiry_time=days_to_timestamp(duration) if duration > 0 else 0,
            flow="xtls-rprx-vision",
            limit_ip=devices,
            sub_id=temp_vpn_id,
            total_gb=0,
        )

        successful_creations, first_server_id = await self._perform_action_on_all_servers(
            user, 'create', client_settings=client_settings
        )

        if successful_creations > 0:
            async with self.session() as session:
                # Получаем реальный vpn_id после создания
                connection = await self.server_pool_service.get_connection_by_server_id(first_server_id, session=session)
                if not connection:
                    logger.error(f"Failed to get connection for server {first_server_id} to retrieve real vpn_id after creation.")
                    return None

                created_client = await self._find_client_on_server(user, connection)
                if not created_client or not hasattr(created_client, 'sub_id'):
                    logger.error(f"Could not find the client {user.tg_id} on server after creation to get real vpn_id.")
                    return None

                real_vpn_id = created_client.sub_id
                logger.info(f"Client for user {user.tg_id} created. Real vpn_id is {real_vpn_id}")

                # Обновляем или создаем пользователя в БД
                user_in_db = await User.get(session, tg_id=user.tg_id)
                if not user_in_db:
                    user_in_db = await User.create(
                        session=session, tg_id=user.tg_id, first_name=user.first_name,
                        username=user.username, server_id=first_server_id, vpn_id=real_vpn_id,
                        language_code=user.language_code # Сохраняем язык
                    )
                else:
                    await User.update(
                        session=session, tg_id=user.tg_id,
                        server_id=first_server_id, vpn_id=real_vpn_id
                    )
                    await session.refresh(user_in_db) # Обновляем объект user_in_db

                # Возвращаем обновленный объект пользователя из БД
                return user_in_db if user_in_db else None
        else:
            logger.error(f"Failed to create client {user.tg_id} on any server.")
            return None


    async def update_client(self, user: User, **kwargs) -> bool:
        def update_payload_func(client: Client) -> Client:
            # Используем get() для безопасного доступа к kwargs
            if 'devices' in kwargs:
                # limit_ip = 0 means unlimited
                client.limit_ip = kwargs.get('devices', 0) if kwargs.get('devices', 0) >= 0 else 0
            if 'duration' in kwargs:
                duration = kwargs.get('duration', 0)
                if duration == 0:
                    client.expiry_time = 0 # 0 means unlimited expiry
                elif duration > 0:
                    replace = kwargs.get('replace_duration', False)
                    current_time = get_current_timestamp()
                    # Если expiry_time None или 0, считаем его как current_time
                    base_expiry = client.expiry_time if client.expiry_time and client.expiry_time > 0 else current_time
                    expiry_to_use = current_time if replace else max(base_expiry, current_time)
                    client.expiry_time = add_days_to_timestamp(expiry_to_use, duration)
            if 'enable' in kwargs:
                client.enable = kwargs.get('enable', True)

            # --- ВАЖНО: Устанавливаем ID и sub_id ---
            client.id = user.vpn_id
            client.sub_id = user.vpn_id
            # --- ---

            return client

        successful_updates, _ = await self._perform_action_on_all_servers(
            user, 'update', update_payload_func=update_payload_func
        )
        return successful_updates > 0

    async def delete_client(self, user: User) -> bool:
        successful_deletions, _ = await self._perform_action_on_all_servers(user, 'delete')
        # Дополнительно очищаем server_id и vpn_id в БД
        if successful_deletions > 0:
            async with self.session() as session:
                await User.update(session, tg_id=user.tg_id, server_id=None, vpn_id=None)
            logger.info(f"Cleared server_id and vpn_id for user {user.tg_id} in DB after deletion.")
        return successful_deletions > 0

    async def ensure_client_exists_on_server(self, user: User, server_id: int, session: AsyncSession) -> bool:
        """
        Проверяет наличие клиента на конкретном сервере и создает его, если он отсутствует.
        Использует текущие данные клиента с другого сервера, если возможно.
        """
        connection = await self.server_pool_service.get_connection_by_server_id(server_id, session=session)
        if not connection:
            logger.error(f"Cannot ensure client {user.tg_id} exists on server {server_id}: Connection failed.")
            return False

        # 1. Проверяем, существует ли клиент
        existing_client = await self._find_client_on_server(user, connection)
        if existing_client:
            logger.debug(f"Client {user.tg_id} already exists on server {connection.server.name}. Skipping creation.")
            return True

        logger.info(f"Client {user.tg_id} not found on server {connection.server.name}. Attempting to create...")

        # 2. Получаем актуальные данные клиента (с любого сервера, где он есть)
        client_data = await self.get_client_data(user, session=session)
        if not client_data:
            logger.warning(f"Could not fetch current data for client {user.tg_id} to create on server {server_id}. Skipping.")
            return False # Пропускаем создание, если не смогли получить данные

        # 3. Готовим настройки для создания
        if not user.vpn_id:
             logger.error(f"Cannot create client {user.tg_id} on server {server_id}: user.vpn_id is missing.")
             return False

        # Конвертируем ClientData обратно в параметры для py3xui Client
        devices_for_creation = 0 if client_data.max_devices == "-1" or client_data.max_devices == "∞" else int(client_data.max_devices)
        expiry_for_creation = 0 if client_data.expiry_timestamp == -1 else client_data.expiry_timestamp
        is_enabled = expiry_for_creation == 0 or expiry_for_creation > get_current_timestamp() # Включаем, если подписка бессрочная или не истекла

        client_settings = Client(
            email=str(user.tg_id),
            enable=is_enabled,
            id=user.vpn_id, # Используем существующий ID!
            expiry_time=expiry_for_creation,
            flow="xtls-rprx-vision", # Можно взять из конфига или оставить значение по умолчанию
            limit_ip=devices_for_creation,
            sub_id=user.vpn_id, # Используем существующий ID!
            total_gb=0, # total_gb из client_data обычно не используется для создания
        )

        # 4. Создаем клиента на ЭТОМ сервере
        try:
            inbound_id = await self.server_pool_service.get_inbound_id(connection.api)
            if not inbound_id:
                raise Exception(f"No inbound ID found on server {server_id}")

            await connection.api.client.add(inbound_id=inbound_id, clients=[client_settings])
            logger.info(f"Successfully created client {user.tg_id} (vpn_id: {user.vpn_id}) on server {connection.server.name} ({server_id}).")
            # Увеличиваем счетчик клиентов на сервере в пуле (если нужно, но _add_server/refresh_server должны это делать)
            # connection.server.users.append(user) # Не лучший способ, лучше обновить из БД
            return True
        except Exception as e:
            logger.error(f"Failed to create client {user.tg_id} on server {server_id}: {e}")
            return False

    async def create_subscription(self, user: User, devices: int, duration: int, **kwargs) -> bool:
        created_user = await self.create_client(user, devices, duration)
        return created_user is not None

    async def extend_subscription(self, user: User, devices: int, duration: int) -> bool:
        return await self.update_client(user, devices=devices, duration=duration, replace_duration=False)

    async def change_subscription(self, user: User, devices: int, duration: int) -> bool:
        return await self.update_client(user, devices=devices, duration=duration, replace_duration=True)

    async def process_bonus_days(self, user: User, duration: int, devices: int, **kwargs) -> bool:
        if await self.is_client_exists(user): # is_client_exists использует свою сессию
            return await self.update_client(user, duration=duration, replace_duration=False)
        else:
            created_user = await self.create_client(user, devices=devices, duration=duration)
            return created_user is not None

    async def activate_promocode(self, user: User, promocode: Promocode, **kwargs) -> bool:
        async with self.session() as session:
            activated = await Promocode.set_activated(session=session, code=promocode.code, user_id=user.tg_id)
        if not activated: return False

        success = await self.process_bonus_days(user, promocode.duration, self.config.shop.BONUS_DEVICES_COUNT)
        if success: return True

        async with self.session() as session:
            await Promocode.set_deactivated(session=session, code=promocode.code)
        return False

    async def enable_client(self, user: User) -> bool:
        return await self.update_client(user, enable=True)

    async def disable_client(self, user: User) -> bool:
        return await self.update_client(user, enable=False)

    async def change_client_location(self, user: User, **kwargs) -> bool:
        logger.warning("change_client_location has no effect in aggregated mode.")
        return True