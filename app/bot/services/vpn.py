from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from py3xui import Client, Inbound
from sqlalchemy.ext.asyncio import async_sessionmaker

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
    from .server_pool import ServerPoolService

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

    async def _find_client_on_server(self, user: User, connection) -> Client | None:
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
                        if client.email == str(user.tg_id):
                            logger.debug(f"Client {user.tg_id} found on server {connection.server.name} in inbound {inbound.id}")
                            client.inbound_id = inbound.id
                            return client
        except Exception as e:
            logger.error(f"Error while searching for client {user.tg_id} on server {connection.server.name}: {e}")
        return None

    async def is_client_exists(self, user: User) -> Client | None:
        """Проверяет наличие клиента хотя бы на одном из серверов."""
        servers = await self.server_pool_service.get_all_servers()
        for server in servers:
            connection = await self.server_pool_service.get_connection_by_server_id(server.id)
            if not connection:
                continue
            client = await self._find_client_on_server(user, connection)
            if client:
                return client
        
        logger.warning(f"Client {user.tg_id} not found on any server.")
        return None

    async def get_limit_ip(self, user: User, client: Client) -> int | None:
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            servers = await self.server_pool_service.get_all_servers()
            if not servers: return None
            connection = await self.server_pool_service.get_connection_by_server_id(servers[0].id)

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

    async def get_client_data(self, user: User) -> ClientData | None:
        logger.debug(f"Starting to retrieve client data for {user.tg_id}.")
        
        client: Client | None = None
        
        if user.server_id:
            connection = await self.server_pool_service.get_connection(user)
            if connection:
                client = await self._find_client_on_server(user, connection)
                if not client:
                    logger.warning(f"Client {user.tg_id} not found on primary server {connection.server.name}. Searching on others.")
        
        if not client:
            client = await self.is_client_exists(user)

        if not client:
            logger.info(f"Could not get client data: Client {user.tg_id} not found on ANY server.")
            return None

        try:
            limit_ip = await self.get_limit_ip(user=user, client=client)
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
        servers = await self.server_pool_service.get_all_servers()
        if not servers:
            logger.error(f"No servers available to perform '{action}' for user {user.tg_id}.")
            return 0, None
        
        successful_ops = 0
        first_success_server_id = None

        for server in servers:
            connection = await self.server_pool_service.get_connection_by_server_id(server.id)
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
                        client_settings = kwargs['update_payload_func'](Client(email=str(user.tg_id)))
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

    async def create_client(self, user: User, devices: int, duration: int, **kwargs) -> bool:
        """
        ИСПРАВЛЕНО: Эта функция теперь создает клиента, затем находит его на сервере,
        чтобы получить реальный ID, который присвоила панель X-UI, и сохраняет
        в базу данных именно его. Это решает проблему рассинхронизации ID.
        """
        # Шаг 1: Создаем клиента в X-UI, используя tg_id как уникальный идентификатор
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
            # Шаг 2: Теперь, когда клиент создан, найдем его, чтобы получить НАСТОЯЩИЙ ID
            connection = await self.server_pool_service.get_connection_by_server_id(first_server_id)
            if not connection:
                logger.error(f"Failed to get connection for server {first_server_id} to retrieve real vpn_id.")
                return False

            created_client = await self._find_client_on_server(user, connection)
            if not created_client or not hasattr(created_client, 'sub_id'):
                logger.error(f"Could not find the client {user.tg_id} on server after creation to get real vpn_id.")
                return False
            
            # Шаг 3: Получаем реальный ID, присвоенный панелью X-UI
            real_vpn_id = created_client.sub_id
            logger.info(f"Client for user {user.tg_id} created. Real vpn_id is {real_vpn_id}")

            # Шаг 4: Сохраняем в нашу базу данных правильный ID
            async with self.session() as session:
                user_in_db = await User.get(session, tg_id=user.tg_id)
                # Устанавливаем vpn_id в объекте user, чтобы другие функции его видели
                user.vpn_id = real_vpn_id
                if not user_in_db:
                    await User.create(
                        session=session, tg_id=user.tg_id, first_name=user.first_name,
                        username=user.username, server_id=first_server_id, vpn_id=real_vpn_id
                    )
                else: 
                    await User.update(
                        session=session, tg_id=user.tg_id,
                        server_id=first_server_id, vpn_id=real_vpn_id
                    )
            return True
            
        return False

    async def update_client(self, user: User, **kwargs) -> bool:
        def update_payload_func(client: Client) -> Client:
            if 'devices' in kwargs:
                client.limit_ip = kwargs['devices']
            if 'duration' in kwargs:
                duration = kwargs['duration']
                if duration == 0:
                    client.expiry_time = 0
                else:
                    replace = kwargs.get('replace_duration', False)
                    current_time = get_current_timestamp()
<<<<<<< HEAD
                    expiry_to_use = current_time if replace else max(client.expiry_time, current_time)
                    client.expiry_time = add_days_to_timestamp(expiry_to_use, duration)
            if 'enable' in kwargs:
                client.enable = kwargs['enable']
            return client
=======
                    if not replace_duration:
                        expiry_time_to_use = max(client.expiry_time, current_time)
                    else:
                        expiry_time_to_use = current_time
                    client.expiry_time = add_days_to_timestamp(timestamp=expiry_time_to_use, days=duration)
            
            client.enable = enable
            client.flow = flow
            client.total_gb = total_gb
            
            client_uuid_for_update = client.sub_id
            client.id = client_uuid_for_update

            await connection.api.client.update(client_uuid=client_uuid_for_update, client=client)
            logger.info(f"Client {user.tg_id} updated successfully.")
            return True
        except Exception as exception:
            logger.error(f"Error updating client {user.tg_id}: {exception}", exc_info=True)
            return False
>>>>>>> e5ba3e606e45ca9065201ccd8a6e4faa4c7c1521

        successful_updates, _ = await self._perform_action_on_all_servers(
            user, 'update', update_payload_func=update_payload_func
        )
        return successful_updates > 0

    async def delete_client(self, user: User) -> bool:
        successful_deletions, _ = await self._perform_action_on_all_servers(user, 'delete')
        return successful_deletions > 0

    async def create_subscription(self, user: User, devices: int, duration: int, **kwargs) -> bool:
        return await self.create_client(user, devices, duration)

    async def extend_subscription(self, user: User, devices: int, duration: int) -> bool:
        return await self.update_client(user, devices=devices, duration=duration, replace_duration=False)

    async def change_subscription(self, user: User, devices: int, duration: int) -> bool:
        return await self.update_client(user, devices=devices, duration=duration, replace_duration=True)

    async def process_bonus_days(self, user: User, duration: int, devices: int, **kwargs) -> bool:
        if await self.is_client_exists(user):
            return await self.update_client(user, duration=duration, replace_duration=False)
        else:
            return await self.create_client(user, devices=devices, duration=duration)

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