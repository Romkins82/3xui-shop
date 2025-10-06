from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .server_pool import ServerPoolService

import logging

from py3xui import Client, Inbound
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.models import ClientData
from app.bot.utils.formatting import format_remaining_time
from app.bot.utils.network import extract_base_url
from app.bot.utils.time import (
    add_days_to_timestamp,
    days_to_timestamp,
    get_current_timestamp,
)
from app.config import Config
from app.db.models import Promocode, User, Server

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

    async def is_client_exists(self, user: User) -> Client | None:
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return None
        try:
            # Try to get the client by email, which might fail if the library is buggy
            client = await connection.api.client.get_by_email(str(user.tg_id))
            if client:
                logger.debug(f"Client {user.tg_id} exists on server {connection.server.name}.")
                return client
        except ValueError as e:
            # If it fails with a known error, try to find the client by iterating through inbounds
            if "Inbound Not Found For Email" in str(e):
                logger.debug(f"Client {user.tg_id} not found by email without inbound_id. Trying with inbounds.")
                try:
                    inbounds = await connection.api.inbound.get_list()
                    for inbound in inbounds:
                        try:
                            client = await connection.api.client.get_by_email(str(user.tg_id), inbound_id=inbound.id)
                            if client:
                                logger.debug(f"Client {user.tg_id} found in inbound {inbound.id}")
                                return client
                        except ValueError:
                            continue # Not in this inbound
                except Exception as e_inbound:
                    logger.error(f"Error while searching client in inbounds: {e_inbound}", exc_info=True)
            else:
                logger.warning(f"Could not check if client exists for user {user.tg_id}: {e}")
        
        logger.warning(f"Client {user.tg_id} not found on server {connection.server.name}.")
        return None

    async def get_limit_ip(self, user: User, client: Client) -> int | None:
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return None
        try:
            inbounds: list[Inbound] = await connection.api.inbound.get_list()
        except Exception as exception:
            logger.error(f"Failed to fetch inbounds: {exception}")
            return None
        for inbound in inbounds:
            for inbound_client in inbound.settings.clients:
                if inbound_client.email == client.email:
                    logger.debug(f"Client {client.email} limit ip: {inbound_client.limit_ip}")
                    return inbound_client.limit_ip
        logger.critical(f"Client {client.email} not found in inbounds.")
        return None

    async def get_client_data(self, user: User) -> ClientData | None:
        logger.debug(f"Starting to retrieve client data for {user.tg_id}.")
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return None
        try:
            client = await self.is_client_exists(user) # Use the corrected is_client_exists
            if not client:
                logger.critical(f"Client {user.tg_id} not found on server {connection.server.name}.")
                return None
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
            logger.error(f"Error retrieving client data for {user.tg_id}: {exception}")
            return None

    async def get_key(self, user: User) -> str | None:
        async with self.session() as session:
            user = await User.get(session=session, tg_id=user.tg_id)
        if not user.server_id:
            logger.debug(f"Server ID for user {user.tg_id} not found.")
            return None
        subscription = extract_base_url(
            url=user.server.host,
            port=self.config.xui.SUBSCRIPTION_PORT,
            path=self.config.xui.SUBSCRIPTION_PATH,
        )
        key = f"{subscription}{user.vpn_id}"
        logger.debug(f"Fetched key for {user.tg_id}: {key}.")
        return key

    async def create_client(
        self,
        user: User,
        devices: int,
        duration: int,
		server_id: int | None = None,
        enable: bool = True,
        flow: str = "xtls-rprx-vision",
        total_gb: int = 0,
        **kwargs
    ) -> User | None:
        location_name = kwargs.get("location_name")
        session = kwargs.get("session")

        logger.info(f"Creating new client for user {user.tg_id} | {devices} devices, {duration} days.")

        if not user.server_id:
            assigned_server = None
            if server_id:
                async with self.session() as temp_session:
                    assigned_server = await Server.get_by_id(temp_session, server_id)
            elif location_name:
                assigned_server = await self.server_pool_service.get_available_server_by_location(location_name)
            else:
                 assigned_server = await self.server_pool_service.get_available_server()
            
            if not assigned_server:
                logger.error(f"No available server found to create client for user {user.tg_id}.")
                return None
            user.server_id = assigned_server.id

        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return None

        import uuid
        user.vpn_id = user.vpn_id or str(uuid.uuid4())
        
        new_client = Client(
            email=str(user.tg_id),
            enable=enable,
            id=user.vpn_id,
            expiry_time=days_to_timestamp(duration) if duration > 0 else 0,
            flow=flow,
            limit_ip=devices,
            sub_id=user.vpn_id,
            total_gb=total_gb,
        )
        inbound_id = await self.server_pool_service.get_inbound_id(connection.api)
        try:
            # If user doesn't exist in bot's DB, create it.
            async with self.session() as manage_user_session:
                if not await User.exists(manage_user_session, tg_id=user.tg_id):
                    await User.create(
                        session=manage_user_session,
                        tg_id=user.tg_id,
                        first_name=user.first_name,
                        username=user.username,
                        server_id=user.server_id,
                        vpn_id=user.vpn_id
                    )
                else: # update server_id and vpn_id if user exists
                    await User.update(
                        session=manage_user_session,
                        tg_id=user.tg_id,
                        server_id=user.server_id,
                        vpn_id=user.vpn_id
                    )

            await connection.api.client.add(inbound_id=inbound_id, clients=[new_client])
            logger.info(f"Successfully created client for {user.tg_id} on server {connection.server.name}")
            return user
        except Exception as exception:
            logger.error(f"Error creating client for {user.tg_id}: {exception}")
            return None

    async def update_client(
        self,
        user: User,
        devices: int = -1,
        duration: int = -1,
        replace_devices: bool = False,
        replace_duration: bool = False,
        enable: bool = True,
        flow: str = "xtls-rprx-vision",
        total_gb: int = 0,
    ) -> bool:
        logger.info(f"Updating client {user.tg_id} | devices={devices}, duration={duration}, replace_devices={replace_devices}, replace_duration={replace_duration}")
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return False
        try:
            client = await self.is_client_exists(user)
            if client is None:
                logger.critical(f"Client {user.tg_id} not found for update.")
                return False

            if devices != -1:
                if not replace_devices:
                    current_device_limit = await self.get_limit_ip(user=user, client=client)
                    client.limit_ip = (current_device_limit or 0) + devices
                else:
                    client.limit_ip = devices
            
            if duration != -1:
                if duration == 0: # Set to unlimited
                    client.expiry_time = 0
                else:
                    current_time = get_current_timestamp()
                    if not replace_duration:
                        expiry_time_to_use = max(client.expiry_time, current_time)
                    else:
                        expiry_time_to_use = current_time
                    client.expiry_time = add_days_to_timestamp(timestamp=expiry_time_to_use, days=duration)
            
            client.enable = enable
            client.flow = flow
            client.total_gb = total_gb
            
            await connection.api.client.update(client_uuid=client.id, client=client)
            logger.info(f"Client {user.tg_id} updated successfully.")
            return True
        except Exception as exception:
            logger.error(f"Error updating client {user.tg_id}: {exception}", exc_info=True)
            return False

    async def create_subscription(self, user: User, devices: int, duration: int, server_id: int | None = None) -> bool:
        if not await self.is_client_exists(user):
            created_user = await self.create_client(user=user, devices=devices, duration=duration, server_id=server_id)
            return created_user is not None
        return False

    async def extend_subscription(self, user: User, devices: int, duration: int) -> bool:
        return await self.update_client(
            user=user,
            devices=devices,
            duration=duration,
            replace_devices=True,
        )

    async def change_subscription(self, user: User, devices: int, duration: int) -> bool:
        if await self.is_client_exists(user):
            return await self.update_client(
                user,
                devices,
                duration,
                replace_devices=True,
                replace_duration=True,
            )
        return False

    async def process_bonus_days(self, user: User, duration: int, devices: int, server_id: int | None = None) -> bool:
        if await self.is_client_exists(user):
            updated = await self.update_client(user=user, devices=0, duration=duration, replace_devices=False)
            if updated:
                logger.info(f"Updated client {user.tg_id} with additional {duration} days(-s).")
                return True
        else:
            created = await self.create_client(user=user, devices=devices, duration=duration, server_id=server_id)
            if created:
                logger.info(f"Created client {user.tg_id} with additional {duration} days(-s)")
                return True
        return False

    async def activate_promocode(self, user: User, promocode: Promocode, server_id: int | None = None) -> bool:
        async with self.session() as session:
            activated = await Promocode.set_activated(
                session=session,
                code=promocode.code,
                user_id=user.tg_id,
            )
        if not activated:
            logger.critical(f"Failed to activate promocode {promocode.code} for user {user.tg_id}.")
            return False
        logger.info(f"Begun applying promocode ({promocode.code}) to a client {user.tg_id}.")
        success = await self.process_bonus_days(
            user,
            duration=promocode.duration,
            devices=self.config.shop.BONUS_DEVICES_COUNT,
            server_id=server_id,
        )
        if success:
            return True
        async with self.session() as session:
            await Promocode.set_deactivated(session=session, code=promocode.code)
        logger.warning(f"Promocode {promocode.code} not activated due to failure.")
        return False

    async def enable_client(self, user: User) -> bool:
        return await self.update_client(user, enable=True, replace_devices=False)

    async def disable_client(self, user: User) -> bool:
        return await self.update_client(user, enable=False, replace_devices=False)

    async def delete_client(self, user: User) -> bool:
        logger.info(f"Deleting client {user.tg_id}")
        connection = await self.server_pool_service.get_connection(user)
        if not connection:
            return False

        try:
            inbounds = await connection.api.inbound.get_list()
            if not inbounds:
                logger.warning(f"No inbounds found on server {connection.server.name}")
                return False

            client_uuid_to_delete = None
            inbound_id_for_client = None

            # Find the client and the inbound it belongs to
            for inbound in inbounds:
                if inbound.settings and hasattr(inbound.settings, 'clients'):
                    for client_setting in inbound.settings.clients:
                        if client_setting.email == str(user.tg_id):
                            client_uuid_to_delete = client_setting.id
                            inbound_id_for_client = inbound.id
                            break
                if client_uuid_to_delete:
                    break
            
            if not client_uuid_to_delete or not inbound_id_for_client:
                logger.warning(f"Client {user.tg_id} not found on server {connection.server.name} in any inbound. Considering as success.")
                return True # If not found, it's already "deleted"

            # Use the found client UUID and inbound ID to delete
            await connection.api.client.delete(inbound_id=inbound_id_for_client, client_uuid=client_uuid_to_delete)
            
            logger.info(f"Successfully deleted client {user.tg_id} from server {connection.server.name}")
            return True
        except ValueError as e:
            if "Client Not Found" in str(e):
                 logger.warning(f"Client {user.tg_id} was not found on the server, likely already deleted. Considering as success.")
                 return True
            logger.error(f"Error deleting client {user.tg_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while deleting client {user.tg_id}: {e}", exc_info=True)
            return False

    async def change_client_location(self, user: User, new_location_name: str, current_devices: int, session: AsyncSession) -> bool:
        logger.info(f"Changing location for user {user.tg_id} to {new_location_name}")
        client_data = await self.get_client_data(user)
        if not client_data:
            logger.error(f"Failed to get current client data for {user.tg_id}")
            return False

        if not await self.delete_client(user):
            logger.error(f"Failed to delete client from old server for user {user.tg_id}")
            return False

        new_server = await self.server_pool_service.get_available_server_by_location(new_location_name)
        if not new_server:
            logger.error(f"No available servers found in location {new_location_name}")
            return False
        
        user.server_id = new_server.id
        await User.update(session=session, tg_id=user.tg_id, server_id=new_server.id)
        
        remaining_days = 0
        if not client_data.has_subscription_expired and client_data.expiry_timestamp != -1:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            expiry_dt = datetime.fromtimestamp(client_data.expiry_timestamp / 1000, timezone.utc)
            remaining_days = (expiry_dt - now).days
            if remaining_days < 0: remaining_days = 0
        
        created_user = await self.create_client(user=user, devices=current_devices, duration=remaining_days)
        return created_user is not None