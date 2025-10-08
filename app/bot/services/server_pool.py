import logging
from dataclasses import dataclass

from py3xui import AsyncApi
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Config
from app.db.models import Server, User

logger = logging.getLogger(__name__)


@dataclass
class Connection:
    server: Server
    api: AsyncApi


class ServerPoolService:
    def __init__(self, config: Config, session: async_sessionmaker) -> None:
        self.config = config
        self.session = session
        self._servers: dict[int, Connection] = {}
        logger.info("Server Pool Service initialized.")

    async def _add_server(self, server: Server) -> None:
        if server.id not in self._servers:
            api = AsyncApi(
                host=server.host,
                username=self.config.xui.USERNAME,
                password=self.config.xui.PASSWORD,
                token=self.config.xui.TOKEN,
                # use_tls_verify=False, # Раскомментируйте, если используете самоподписанные SSL
                logger=logging.getLogger(f"xui_{server.name}"),
            )
            try:
                await api.login()
                server.online = True
                server_conn = Connection(server=server, api=api)
                self._servers[server.id] = server_conn
                logger.info(f"Server {server.name} ({server.host}) added to pool successfully.")
            except Exception as exception:
                server.online = False
                logger.error(f"Failed to add server {server.name} ({server.host}): {exception}")

            async with self.session() as session:
                await Server.update(session=session, name=server.name, online=server.online)

    def _remove_server(self, server: Server) -> None:
        if server.id in self._servers:
            try:
                del self._servers[server.id]
            except Exception as exception:
                logger.error(f"Failed to remove server {server.name}: {exception}")

    async def refresh_server(self, server: Server) -> None:
        if server.id in self._servers:
            self._remove_server(server)
        await self._add_server(server)
        logger.info(f"Server {server.name} reinitialized successfully.")

    async def get_inbound_id(self, api: AsyncApi) -> int | None:
        try:
            inbounds = await api.inbound.get_list()
        except Exception as exception:
            logger.error(f"Failed to fetch inbounds: {exception}")
            return None
        return inbounds[0].id if inbounds else None

    async def get_connection(self, user: User) -> Connection | None:
        if not user.server_id:
            logger.debug(f"User {user.tg_id} not assigned to any server.")
            return None
        connection = self._servers.get(user.server_id)
        if not connection:
            logger.critical(f"Server {user.server_id} not found in pool.")
            async with self.session() as session:
                server = await Server.get_by_id(session=session, id=user.server_id)
            if server:
                logger.debug(f"Server {server.name} ({server.host}) found in database.")
            else:
                logger.error(f"Server {user.server_id} not found in database.")
            return None
        async with self.session() as session:
            server = await Server.get_by_id(session=session, id=user.server_id)
        connection.server = server
        return connection

    async def sync_servers(self) -> None:
        async with self.session() as session:
            db_servers = await Server.get_all(session)
        if not db_servers and not self._servers:
            logger.warning("No servers found in the database.")
            return
        db_server_map = {server.id: server for server in db_servers}
        for server_id in list(self._servers.keys()):
            if server_id not in db_server_map:
                self._remove_server(self._servers[server_id].server)
        for server_id, conn in list(self._servers.items()):
            if db_server := db_server_map.get(server_id):
                conn.server = db_server
            await self.refresh_server(conn.server)
        for server in db_servers:
            if server.id not in self._servers:
                await self._add_server(server)
        logger.info(f"Sync complete. Currently active servers: {len(self._servers)}")

    async def assign_server_to_user(self, user: User, server_id: int | None = None) -> Server | None:
        if server_id:
            async with self.session() as session:
                server = await Server.get_by_id(session, server_id)
        else:
            server = await self.get_available_server()

        if server:
            user.server_id = server.id
            async with self.session() as session:
                await User.update(session=session, tg_id=user.tg_id, server_id=server.id)
            return server
        return None

    async def get_available_server(self) -> Server | None:
        await self.sync_servers()
        servers_with_free_slots = [
            conn.server
            for conn in self._servers.values()
            if conn.server.online and conn.server.current_clients < conn.server.max_clients
        ]
        if servers_with_free_slots:
            server = sorted(servers_with_free_slots, key=lambda s: s.current_clients)[0]
            return server
        logger.critical("No available servers with free slots found in pool")
        return None

    async def get_available_server_by_location(self, location_name: str) -> Server | None:
        await self.sync_servers()
        servers_in_location = [c.server for c in self._servers.values() if c.server.location == location_name and c.server.online]
        if not servers_in_location: return None
        
        servers_with_free_slots = [s for s in servers_in_location if s.current_clients < s.max_clients]
        if servers_with_free_slots:
            server = sorted(servers_with_free_slots, key=lambda s: s.current_clients)[0]
            return server

        logger.warning(f"No available servers with free slots found in location {location_name}")
        return None


    async def get_all_servers(self) -> list[Server]:
        await self.sync_servers()
        return [conn.server for conn in self._servers.values()]
    
    async def get_available_servers(self) -> list[Server]:
        await self.sync_servers()
        return [
            conn.server
            for conn in self._servers.values()
            if conn.server.online and conn.server.current_clients < conn.server.max_clients
        ]