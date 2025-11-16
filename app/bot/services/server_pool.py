# app/bot/services/server_pool.py
import logging
from dataclasses import dataclass
from contextlib import nullcontext # Добавить импорт
from typing import TYPE_CHECKING, Optional # Добавить Optional

from py3xui import AsyncApi
# --- ИСПРАВЛЕНИЕ: Добавить импорт select ---
from sqlalchemy import select
# --- КОНЕЦ ИСПРАВЛЕНИЯ ---
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload # Добавить импорт selectinload

from app.config import Config
from app.db.models import Server, User
# Добавить импорт VPNService для type hinting и User
if TYPE_CHECKING:
    from app.bot.services.vpn import VPNService # Используем TYPE_CHECKING

logger = logging.getLogger(__name__)


@dataclass
class Connection:
    server: Server
    api: AsyncApi


class ServerPoolService:
    # Добавляем vpn_service в __init__ и сохраняем его
    def __init__(self, config: Config, session: async_sessionmaker, vpn_service: Optional['VPNService'] = None) -> None: # Добавили vpn_service
        self.config = config
        self.session = session
        self._servers: dict[int, Connection] = {}
        # Сохраняем vpn_service.
        self.vpn_service: Optional['VPNService'] = vpn_service
        logger.info("Server Pool Service initialized.")

    # Метод для установки vpn_service после инициализации (чтобы избежать циклического импорта)
    def set_vpn_service(self, vpn_service: 'VPNService'):
        self.vpn_service = vpn_service
        logger.info("VPNService linked to ServerPoolService.")

    async def _add_server(self, server: Server) -> None:
        if server.id not in self._servers:
            api = AsyncApi(
                host=server.host,
                username=self.config.xui.USERNAME,
                password=self.config.xui.PASSWORD,
                token=self.config.xui.TOKEN,
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

            # Обновляем статус online в БД только если он изменился
            async with self.session() as session:
                db_server = await Server.get_by_id(session=session, id=server.id)
                # Проверяем существование db_server перед доступом к атрибуту online
                if db_server and db_server.online != server.online:
                    # Используем update по ID, так как имя могло измениться
                    # Передаем name из db_server, так как update работает по name в текущей реализации Server.update
                    await Server.update(session=session, name=db_server.name, id=server.id, online=server.online)
                    logger.debug(f"Updated online status for {server.name} to {server.online} in DB.")
                elif not db_server:
                     logger.warning(f"Server {server.name} ({server.id}) not found in DB during _add_server status update.")


    def _remove_server(self, server: Server) -> None:
        if server.id in self._servers:
            try:
                del self._servers[server.id]
                logger.info(f"Server {server.name} ({server.id}) removed from pool.")
            except Exception as exception:
                logger.error(f"Failed to remove server {server.name}: {exception}")

    async def refresh_server(self, server: Server) -> None:
        if server.id in self._servers:
            # Не удаляем полностью, а пытаемся переподключиться
            connection = self._servers[server.id]
            api = connection.api
            original_online_status = server.online # Сохраняем текущий статус из пула

            try:
                await api.login()
                server.online = True # Обновляем статус в объекте сервера в пуле
                logger.info(f"Server {server.name} ({server.host}) reconnected successfully.")
            except Exception as exception:
                server.online = False # Обновляем статус в объекте сервера в пуле
                logger.error(f"Failed to reconnect server {server.name} ({server.host}): {exception}")
                # Если переподключиться не удалось, не удаляем из пула, просто помечаем как offline

            # Обновляем статус online в БД только если он изменился по сравнению с БД
            async with self.session() as session:
                db_server = await Server.get_by_id(session=session, id=server.id)
                if db_server and db_server.online != server.online:
                    # Используем update по ID (нужно убедиться, что Server.update это поддерживает или использовать прямой update)
                    # Передаем name из db_server
                    await Server.update(session=session, name=db_server.name, id=server.id, online=server.online)
                    logger.debug(f"Updated online status for {server.name} to {server.online} in DB during refresh.")
                elif not db_server:
                     logger.warning(f"Server {server.name} ({server.id}) not found in DB during refresh status update.")

        else:
             # Если сервера не было в пуле, просто добавляем его
             logger.info(f"Server {server.name} not in pool during refresh. Adding...")
             await self._add_server(server)


    async def get_inbound_id(self, api: AsyncApi) -> int | None:
        try:
            inbounds = await api.inbound.get_list()
        except Exception as exception:
            logger.error(f"Failed to fetch inbounds: {exception}")
            return None
        # Возвращаем ID ПЕРВОГО инбаунда
        return inbounds[0].id if inbounds else None


    async def get_connection(self, user: User, session: Optional[AsyncSession] = None) -> Connection | None: # Добавлен session
        if not user.server_id:
            logger.debug(f"User {user.tg_id} not assigned to any primary server.")
            # Если основной сервер не назначен, пробуем найти клиента на любом сервере
            servers = await self.get_all_servers(session=session)
            for server in servers:
                conn = await self.get_connection_by_server_id(server.id, session=session)
                if conn:
                    # Проверяем, есть ли vpn_service перед вызовом _find_client_on_server
                    if self.vpn_service:
                         client_on_server = await self.vpn_service._find_client_on_server(user, conn)
                         if client_on_server:
                             logger.debug(f"Found client {user.tg_id} on non-primary server {server.name}. Using this connection.")
                             return conn
                    else:
                         logger.error("VPNService not available in ServerPoolService during get_connection.")
                         return None # Или другая логика обработки ошибки
            logger.warning(f"Client {user.tg_id} not found on any server when primary server_id is None.")
            return None
        # Передаем сессию дальше
        return await self.get_connection_by_server_id(user.server_id, session=session)

    async def get_connection_by_server_id(self, server_id: int, session: Optional[AsyncSession] = None) -> Connection | None: # Добавлен session
        connection = self._servers.get(server_id)
        if not connection:
            logger.debug(f"Server {server_id} not found in pool. Checking database...") # Изменил уровень лога
            # Используем переданную сессию или создаем новую
            use_session = session if session else self.session()
            _session_context = use_session if not session else nullcontext(use_session)
            async with _session_context as active_session:
                server = await Server.get_by_id(session=active_session, id=server_id) # Используем active_session
            if server:
                logger.debug(f"Server {server.name} ({server.host}) found in database. Adding to pool.")
                # _add_server создаст свою сессию, если нужно
                await self._add_server(server)
                # Возвращаем соединение, если оно было успешно добавлено
                return self._servers.get(server.id)
            else:
                logger.error(f"Server {server_id} not found in database.")
                return None

        # --- Обновление данных сервера из БД ---
        use_session = session if session else self.session()
        _session_context = use_session if not session else nullcontext(use_session)
        async with _session_context as active_session:
             server_from_db = await Server.get_by_id(session=active_session, id=server_id) # Используем active_session
        if server_from_db:
             # Обновляем только если данные изменились, чтобы избежать лишнего логгирования
             # Сравниваем основные атрибуты, кроме users, т.к. users могут меняться часто
             if (connection.server.name != server_from_db.name or
                 connection.server.host != server_from_db.host or
                 connection.server.max_clients != server_from_db.max_clients or
                 connection.server.location != server_from_db.location or
                 connection.server.online != server_from_db.online):
                 logger.debug(f"Updating server data in pool from DB for {server_from_db.name} ({server_id}).")
                 # Сохраняем текущий список users, чтобы не потерять его при обновлении
                 current_users = connection.server.users if hasattr(connection.server, 'users') else [] # Проверка на users
                 connection.server = server_from_db # Обновляем данные в существующем соединении
                 connection.server.users = current_users # Восстанавливаем users (хотя они могут быть неактуальны)
        else:
             logger.warning(f"Server {server_id} disappeared from DB while getting connection? Keeping pooled data.")
        # --- Конец обновления ---
        return connection

    async def sync_servers(self) -> None:
        logger.info("Starting server synchronization...")
        if not self.vpn_service: # Проверка, что vpn_service установлен
             logger.warning("VPNService is not set in ServerPoolService. Cannot sync users.") # Изменен уровень на warning

        newly_added_server_ids = set() # Сюда будем записывать ID новых серверов

        # Используем одну сессию для всех операций БД внутри sync_servers
        async with self.session() as session:
            try:
                # Загружаем серверы без пользователей для начальной сверки
                result_servers = await session.execute(select(Server))
                db_servers = result_servers.scalars().all()
            except Exception as e:
                 logger.error(f"Failed to get servers from DB during sync: {e}")
                 return # Прерываем синхронизацию при ошибке БД

            if not db_servers and not self._servers:
                logger.warning("No servers found in the database and pool is empty. Sync finished.")
                return

            db_server_map = {server.id: server for server in db_servers}
            current_pool_ids = set(self._servers.keys())
            db_ids = set(db_server_map.keys())

            # Удаляем из пула серверы, которых больше нет в БД
            ids_to_remove = current_pool_ids - db_ids
            for server_id in ids_to_remove:
                if server_id in self._servers: # Доп. проверка
                    logger.info(f"Server {server_id} not in DB, removing from pool.")
                    self._remove_server(self._servers[server_id].server)

            # Обновляем существующие и переподключаемся
            ids_to_refresh = current_pool_ids.intersection(db_ids)
            for server_id in ids_to_refresh:
                 if server_id in self._servers: # Доп. проверка
                    conn = self._servers[server_id]
                    db_server = db_server_map[server_id] # Берем данные без users
                    
                    # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
                    # Обновляем атрибуты существующего объекта, а не заменяем его
                    if (conn.server.name != db_server.name or
                        conn.server.host != db_server.host or
                        conn.server.max_clients != db_server.max_clients or
                        conn.server.location != db_server.location):
                        
                        logger.debug(f"Updating server core data in pool for {db_server.name} ({server_id}).")
                        conn.server.name = db_server.name
                        conn.server.host = db_server.host
                        conn.server.max_clients = db_server.max_clients
                        conn.server.location = db_server.location
                        # Атрибуты 'online' и 'users' мы не трогаем,
                        # они остаются теми, что были в 'conn.server'
                    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                    # Принудительно переподключаемся для проверки актуальности и статуса online
                    await self.refresh_server(conn.server) # refresh_server обновит статус online в БД

            # Добавляем новые серверы из БД в пул
            ids_to_add = db_ids - current_pool_ids
            for server_id in ids_to_add:
                 server = db_server_map[server_id] # Данные без users
                 logger.info(f"Found new server in DB: {server.name} ({server.id}). Adding to pool.")
                 await self._add_server(server) # _add_server обновит статус online в БД
                 # Запоминаем ID только что добавленного СЕРВЕРА
                 # Проверяем, что он действительно добавился и онлайн
                 if self._servers.get(server.id) and self._servers[server.id].server.online:
                      newly_added_server_ids.add(server.id)

            logger.info(f"Server pool sync complete. Active servers in pool: {len(self._servers)}")

            # --- НАЧАЛО ЛОГИКИ СИНХРОНИЗАЦИИ ПОЛЬЗОВАТЕЛЕЙ ---
            if newly_added_server_ids and self.vpn_service:
                logger.info(f"Found {len(newly_added_server_ids)} newly added online servers. Synchronizing existing users...")

                try:
                    # Получаем ВСЕХ пользователей из БД
                    result_users = await session.execute(select(User))
                    all_users = result_users.scalars().all()
                    users_with_vpn_id = [user for user in all_users if user.vpn_id]
                    logger.info(f"Found {len(users_with_vpn_id)} users with vpn_id to check/create on new servers.")
                except Exception as e:
                    logger.error(f"Failed to get users from DB during user sync: {e}")
                    users_with_vpn_id = [] # Не синхронизируем пользователей при ошибке

                processed_count = 0
                failed_count = 0
                skipped_count = 0

                for user in users_with_vpn_id:
                    logger.debug(f"Processing user {user.tg_id} for new servers.")
                    # Пытаемся получить данные клиента ОДИН раз для этого пользователя
                    # Передаем текущую сессию в get_client_data
                    user_client_data = await self.vpn_service.get_client_data(user, session=session)

                    if not user_client_data:
                        logger.warning(f"Could not get client data for user {user.tg_id}. Skipping sync for this user.")
                        skipped_count += 1
                        continue # Пропускаем пользователя, если не можем получить его данные

                    for server_id in newly_added_server_ids:
                        logger.debug(f"Ensuring user {user.tg_id} exists on new server {server_id}.")
                        # Передаем текущую сессию в ensure_client_exists_on_server
                        success = await self.vpn_service.ensure_client_exists_on_server(user, server_id, session=session)
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1
                            logger.error(f"Failed ensure_client_exists_on_server for user {user.tg_id} on server {server_id}")


                logger.info(f"User synchronization complete for new servers. "
                            f"Successful operations: {processed_count}, Failed: {failed_count}, Skipped users: {skipped_count}.")
            elif newly_added_server_ids and not self.vpn_service:
                 logger.error("Cannot synchronize users to new servers: VPNService is not available.")
            else:
                 logger.info("No new online servers found or no users with vpn_id. Skipping user synchronization.")
            # --- КОНЕЦ ЛОГИКИ СИНХРОНИЗАЦИИ ПОЛЬЗОВАТЕЛЕЙ ---

    async def assign_server_to_user(self, user: User, server_id: int | None = None, session: Optional[AsyncSession] = None) -> Server | None: # Добавлен session
        use_session = session if session else self.session()
        _session_context = use_session if not session else nullcontext(use_session)

        async with _session_context as active_session:
            # Сначала проверяем server_id у самого объекта user, если он уже загружен
            if user.server_id:
                # Проверяем, существует ли сервер с таким ID в пуле или БД
                conn = await self.get_connection_by_server_id(user.server_id, session=active_session)
                if conn:
                    logger.debug(f"User {user.tg_id} already has primary server_id {user.server_id}. Returning.")
                    return conn.server # Возвращаем сервер из соединения

            # Если server_id нет у объекта или сервер не найден, ищем доступный
            server_to_assign = None
            if server_id: # Если ID сервера передан явно
                conn = await self.get_connection_by_server_id(server_id, session=active_session)
                if conn and conn.server.online: # Проверяем, что сервер онлайн
                    server_to_assign = conn.server
                else:
                    logger.warning(f"Explicitly requested server_id {server_id} not found or offline for user {user.tg_id}. Searching for another.")
            # Если ID не передан ИЛИ явно запрошенный сервер недоступен
            if not server_to_assign:
                # Передаем сессию в get_available_server
                server_to_assign = await self.get_available_server(session=active_session)

            if server_to_assign:
                logger.info(f"Assigning primary server {server_to_assign.name} ({server_to_assign.id}) to user {user.tg_id}.")
                user.server_id = server_to_assign.id # Обновляем объект user
                # Обновляем в БД
                await User.update(session=active_session, tg_id=user.tg_id, server_id=server_to_assign.id)
                return server_to_assign
            else:
                logger.error(f"Could not find any available server to assign to user {user.tg_id}.")

        return None

    async def get_available_server(self, session: Optional[AsyncSession] = None) -> Server | None: # Добавлен session
        servers_with_free_slots = []
        async with (session if session else self.session()) as active_session:
            # Получаем актуальные данные о пользователях для серверов в пуле
            online_server_ids = [conn.server.id for conn in self._servers.values() if conn.server.online]
            if not online_server_ids:
                 logger.warning("No online servers currently in the pool.")
                 return None

            # Загружаем серверы с актуальным количеством пользователей
            result = await active_session.execute(
                select(Server)
                .options(selectinload(Server.users)) # Загружаем пользователей для подсчета
                .where(Server.id.in_(online_server_ids))
            )
            online_servers_with_users = result.scalars().all()

            servers_with_free_slots = [
                s for s in online_servers_with_users if hasattr(s, 'users') and s.current_clients < s.max_clients # Проверка на users
            ]

        if servers_with_free_slots:
            # Сортируем по current_clients (которое теперь актуально)
            server = sorted(servers_with_free_slots, key=lambda s: s.current_clients)[0]
            logger.debug(f"Found available server: {server.name} ({server.current_clients}/{server.max_clients})")
            return server

        logger.critical("No available servers with free slots found in pool.")
        return None

    async def get_available_server_by_location(self, location_name: str, session: Optional[AsyncSession] = None) -> Server | None: # Добавлен session
        # Получаем ID серверов в нужной локации из пула
        server_ids_in_location = [
            conn.server.id
            for conn in self._servers.values()
            if conn.server.location == location_name and conn.server.online
        ]
        if not server_ids_in_location:
            logger.warning(f"No online servers found in location {location_name}.")
            return None

        async with (session if session else self.session()) as active_session:
             # Загружаем серверы из БД с актуальным количеством пользователей
            result = await active_session.execute(
                select(Server)
                .options(selectinload(Server.users)) # Загружаем пользователей
                .where(Server.id.in_(server_ids_in_location))
            )
            servers_in_location_with_users = result.scalars().all()

        servers_with_free_slots = [s for s in servers_in_location_with_users if hasattr(s, 'users') and s.current_clients < s.max_clients] # Проверка
        if servers_with_free_slots:
            server = sorted(servers_with_free_slots, key=lambda s: s.current_clients)[0]
            logger.debug(f"Found available server in {location_name}: {server.name} ({server.current_clients}/{server.max_clients})")
            return server

        logger.warning(f"No available servers with free slots found in location {location_name}")
        return None


    async def get_all_servers(self, session: Optional[AsyncSession] = None) -> list[Server]: # Добавлен session
        # Возвращает серверы из пула, данные обновляются при sync_servers
        # Используем list() для создания копии списка серверов
        return [conn.server for conn in list(self._servers.values())]

    async def get_available_servers(self, session: Optional[AsyncSession] = None) -> list[Server]: # Добавлен session
        # Возвращает серверы из пула, но фильтрует по актуальным данным из БД
        available_servers = []
        async with (session if session else self.session()) as active_session:
            online_server_ids = [conn.server.id for conn in self._servers.values() if conn.server.online]
            if not online_server_ids:
                 return []
    
                # Загружаем серверы с пользователями
            result = await active_session.execute(
                select(Server)
                .options(selectinload(Server.users)) # Загружаем пользователей
                .where(Server.id.in_(online_server_ids))
            )
            online_servers_with_users = result.scalars().all()
    
                # Фильтруем по количеству клиентов
            available_servers = [
                s for s in online_servers_with_users if hasattr(s, 'users') and s.current_clients < s.max_clients # Проверка
            ]
        return available_servers