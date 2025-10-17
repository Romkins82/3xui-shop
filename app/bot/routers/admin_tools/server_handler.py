import logging
from typing import Any, TYPE_CHECKING

from aiogram import F, Router, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.i18n import gettext as _
from sqlalchemy.ext.asyncio import AsyncSession

from app.bot.filters import IsDev
from app.bot.models import ServicesContainer
from app.bot.routers.misc.keyboard import back_keyboard
from app.bot.utils.constants import (
    MAIN_MESSAGE_ID_KEY,
    SERVER_HOST_KEY,
    SERVER_MAX_CLIENTS_KEY,
    SERVER_NAME_KEY,
    SERVER_LOCATION_KEY,  # <-- ВОТ ИСПРАВЛЕНИЕ
)
from app.bot.utils.navigation import NavAdminTools
from app.bot.utils.network import ping_url
from app.bot.utils.validation import is_valid_client_count, is_valid_host
from app.db.models import Server, User

from .keyboard import (
    confirm_add_server_keyboard,
    edit_server_keyboard,
    server_keyboard,
    servers_keyboard,
)

if TYPE_CHECKING:
    from aiogram import Bot

logger = logging.getLogger(__name__)
router = Router(name=__name__)


class AddServerStates(StatesGroup):
    name = State()
    host = State()
    max_clients = State()
    location = State()
    confirmation = State()


class EditServerStates(StatesGroup):
    menu = State()
    name = State()
    host = State()
    max_clients = State()
    location = State()


# region Main and Sync
@router.callback_query(F.data == NavAdminTools.SERVER_MANAGEMENT, IsDev())
async def callback_server_management(
    callback: CallbackQuery,
    user: User,
    session: AsyncSession,
    state: FSMContext,
) -> None:
    logger.info(f"Dev {user.tg_id} opened servers management.")
    try:
        await state.clear()
        await state.update_data({MAIN_MESSAGE_ID_KEY: callback.message.message_id})
        text = _("server_management:message:main")
        servers = await Server.get_all(session)

        if not servers:
            text += _("server_management:message:empty")

        await callback.message.edit_text(text=text, reply_markup=servers_keyboard(servers))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified, but answering callback to remove loading state.")
        else:
            logger.error(f"TelegramBadRequest in callback_server_management: {e}")
    finally:
        await callback.answer()


@router.callback_query(F.data == NavAdminTools.SYNC_SERVERS, IsDev())
async def callback_sync_servers(
    callback: CallbackQuery,
    user: User,
    session: AsyncSession,
    state: FSMContext,
    services: ServicesContainer,
) -> None:
    logger.info(f"Dev {user.tg_id} initiated server sync.")
    await services.server_pool.sync_servers()
    await callback_server_management(
        callback=callback, user=user, session=session, state=state
    )
    # This popup will now correctly fire after the message is edited and callback is answered.
    await services.notification.show_popup(
        callback=callback,
        text=_("server_management:popup:synced"),
    )

async def show_add_server(message: Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    data = await state.get_data()
    main_message_id = data.get(MAIN_MESSAGE_ID_KEY)

    text = _("server_management:message:add")
    name = _("server_management:message:name").format(server_name=data.get(SERVER_NAME_KEY, ""))
    host = _("server_management:message:host").format(server_host=data.get(SERVER_HOST_KEY, ""))
    max_clients = _("server_management:message:max_clients").format(
        server_max_clients=data.get(SERVER_MAX_CLIENTS_KEY, "")
    )

    location = _("server_management:message:location").format(
        server_location=data.get(SERVER_LOCATION_KEY, "")
    )

    reply_markup = back_keyboard(NavAdminTools.ADD_SERVER_BACK)

    match current_state:
        case AddServerStates.name:
            text += _("server_management:message:enter_name")
            reply_markup = back_keyboard(NavAdminTools.SERVER_MANAGEMENT)
        case AddServerStates.host:
            text += name
            text += _("server_management:message:enter_host")
        case AddServerStates.max_clients:
            text += name + host
            text += _("server_management:message:enter_max_clients")

        case AddServerStates.location:
            text += name + host + max_clients
            text += _("server_management:message:enter_location")

        case AddServerStates.confirmation:
            text += name + host + max_clients + location
            text += _("server_management:message:confirm")
            reply_markup = confirm_add_server_keyboard()

    await message.bot.edit_message_text(
        text=text,
        chat_id=message.chat.id,
        message_id=main_message_id,
        reply_markup=reply_markup,
    )

@router.callback_query(StateFilter(AddServerStates), F.data == NavAdminTools.ADD_SERVER_BACK, IsDev())
async def callback_add_server_back(callback: CallbackQuery, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state == AddServerStates.host:
        await state.set_state(AddServerStates.name)
    elif current_state == AddServerStates.max_clients:
        await state.set_state(AddServerStates.host)
    elif current_state == AddServerStates.location:
        await state.set_state(AddServerStates.max_clients)
    elif current_state == AddServerStates.confirmation:
        await state.set_state(AddServerStates.location)
    await show_add_server(message=callback.message, state=state)


@router.callback_query(F.data == NavAdminTools.ADD_SERVER, IsDev())
async def callback_add_server(callback: CallbackQuery, user: User, state: FSMContext) -> None:
    logger.info(f"Dev {user.tg_id} started adding server.")
    await state.set_state(AddServerStates.name)
    await show_add_server(message=callback.message, state=state)


@router.message(AddServerStates.name, IsDev())
async def message_add_name(message: Message, session: AsyncSession, state: FSMContext, services: ServicesContainer) -> None:
    server_name = message.text.strip()
    if await Server.get_by_name(session=session, name=server_name):
        await services.notification.notify_by_message(message, _("server_management:ntf:name_exists"), duration=5)
        return
    await state.set_state(AddServerStates.host)
    await state.update_data({SERVER_NAME_KEY: server_name})
    await show_add_server(message=message, state=state)


@router.message(AddServerStates.host, IsDev())
async def message_add_host(message: Message, state: FSMContext, services: ServicesContainer) -> None:
    server_host = message.text.strip()
    if not is_valid_host(server_host):
        await services.notification.notify_by_message(message, _("server_management:ntf:invalid_host"), duration=5)
        return
    await state.set_state(AddServerStates.max_clients)
    await state.update_data({SERVER_HOST_KEY: server_host})
    await show_add_server(message=message, state=state)


@router.message(AddServerStates.max_clients, IsDev())
async def message_add_max_clients(message: Message, state: FSMContext, services: ServicesContainer) -> None:
    server_max_clients = message.text.strip()
    if not is_valid_client_count(server_max_clients):
        await services.notification.notify_by_message(message, _("server_management:ntf:invalid_max_clients"), duration=5)
        return
    await state.set_state(AddServerStates.location)
    await state.update_data({SERVER_MAX_CLIENTS_KEY: int(server_max_clients)})
    await show_add_server(message=message, state=state)

@router.message(AddServerStates.location, IsDev())
async def message_add_location(message: Message, state: FSMContext, services: ServicesContainer) -> None:
    server_location = message.text.strip()
    # Простая проверка, что поле не пустое
    if not server_location:
        await services.notification.notify_by_message(message, "❌ Локация не может быть пустой.", duration=5)
        return
    await state.set_state(AddServerStates.confirmation)
    await state.update_data({SERVER_LOCATION_KEY: server_location})
    await show_add_server(message=message, state=state)
	
@router.callback_query(AddServerStates.confirmation, F.data == NavAdminTools.СONFIRM_ADD_SERVER, IsDev())
async def callback_add_confirmation(callback: CallbackQuery, user: User, session: AsyncSession, state: FSMContext, services: ServicesContainer) -> None:
    data = await state.get_data()
    server = await Server.create(
        session=session,
        name=data.get(SERVER_NAME_KEY),
        host=data.get(SERVER_HOST_KEY),
        max_clients=data.get(SERVER_MAX_CLIENTS_KEY),
        location=data.get(SERVER_LOCATION_KEY),
    )
    if server:
        await services.server_pool.sync_servers()
        await callback_server_management(callback, user, session, state)
        await services.notification.show_popup(callback, _("server_management:popup:added_success"))
    else:
        await services.notification.show_popup(callback, _("server_management:popup:add_failed"))

async def _show_server_view(
    bot: "Bot", chat_id: int, message_id: int, session: AsyncSession, server_id: int, state: FSMContext
):
    """Helper to display the server view and update state."""
    await state.clear()
    await state.update_data({MAIN_MESSAGE_ID_KEY: message_id})
    server = await Server.get_by_id(session=session, id=server_id)
    if not server:
        await bot.edit_message_text("❌ Сервер не найден.", chat_id=chat_id, message_id=message_id)
        return

    status = (
        _("server_management:message:status_online")
        if server.online
        else _("server_management:message:status_offline")
    )
    text = _("server_management:message:server_info").format(
        server_name=server.name,
        host=server.host,
        status=status,
        clients=server.current_clients,
        max_clients=server.max_clients,
    )
    if server.location:
        text += f"\n<b>Локация:</b> {server.location}"
        
    await bot.edit_message_text(
        text=text,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=server_keyboard(server.id),
    )


@router.callback_query(F.data.startswith(f"{NavAdminTools.SHOW_SERVER}:"), IsDev())
async def callback_show_server(
    callback: CallbackQuery, user: User, session: AsyncSession, state: FSMContext
):
    server_id = int(callback.data.split(":")[1])
    server = await Server.get_by_id(session, id=server_id)
    if not server:
        await callback.answer("❌ Сервер не найден.", show_alert=True)
        return
    
    logger.info(f"Dev {user.tg_id} opened server view for '{server.name}'.")
    await _show_server_view(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        session=session,
        server_id=server_id,
        state=state,
    )


@router.callback_query(F.data.startswith(f"{NavAdminTools.EDIT_SERVER}:"), IsDev())
async def callback_edit_server(
    callback: CallbackQuery, user: User, state: FSMContext, session: AsyncSession
):
    server_id = int(callback.data.split(":")[1])
    server = await Server.get_by_id(session, id=server_id)
    if not server:
        await callback.answer("❌ Сервер не найден.", show_alert=True)
        return

    logger.info(f"Dev {user.tg_id} entered edit menu for server '{server.name}'.")
    await state.set_state(EditServerStates.menu)
    await state.update_data(server_id_to_edit=server_id)
    await callback.message.edit_text(
        f"✏️ <b>Редактирование сервера:</b> {server.name}",
        reply_markup=edit_server_keyboard(server_id),
    )


# --- Handlers to enter edit states ---
@router.callback_query(F.data.startswith(f"{NavAdminTools.EDIT_SERVER_NAME}:"), EditServerStates.menu, IsDev())
async def callback_edit_name_prompt(callback: CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(EditServerStates.name)
    await callback.message.edit_text(
        "Введите новое название сервера:",
        reply_markup=back_keyboard(f"{NavAdminTools.EDIT_SERVER}:{server_id}"),
    )


@router.callback_query(F.data.startswith(f"{NavAdminTools.EDIT_SERVER_HOST}:"), EditServerStates.menu, IsDev())
async def callback_edit_host_prompt(callback: CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(EditServerStates.host)
    await callback.message.edit_text(
        "Введите новый хост сервера (URL):",
        reply_markup=back_keyboard(f"{NavAdminTools.EDIT_SERVER}:{server_id}"),
    )


@router.callback_query(F.data.startswith(f"{NavAdminTools.EDIT_SERVER_MAX_CLIENTS}:"), EditServerStates.menu, IsDev())
async def callback_edit_max_clients_prompt(callback: CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(EditServerStates.max_clients)
    await callback.message.edit_text(
        "Введите новое максимальное количество клиентов:",
        reply_markup=back_keyboard(f"{NavAdminTools.EDIT_SERVER}:{server_id}"),
    )
	
@router.callback_query(F.data.startswith(f"{NavAdminTools.EDIT_SERVER_LOCATION}:"), EditServerStates.menu, IsDev())
async def callback_edit_location_prompt(callback: CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(EditServerStates.location)
    await callback.message.edit_text(
        "Введите новую локацию сервера:",
        reply_markup=back_keyboard(f"{NavAdminTools.EDIT_SERVER}:{server_id}"),
    )

# --- Message handlers for receiving new data ---
@router.message(EditServerStates.name, IsDev())
async def message_edit_name(message: Message, session: AsyncSession, state: FSMContext, services: ServicesContainer):
    new_name = message.text.strip()
    if await Server.get_by_name(session, name=new_name):
        await services.notification.notify_by_message(message, _("server_management:ntf:name_exists"), duration=5)
        return
    await handle_server_edit(message, state, session, services, "name", new_name)


@router.message(EditServerStates.host, IsDev())
async def message_edit_host(message: Message, state: FSMContext, session: AsyncSession, services: ServicesContainer):
    new_host = message.text.strip()
    if not is_valid_host(new_host):
        await services.notification.notify_by_message(message, _("server_management:ntf:invalid_host"), duration=5)
        return
    await handle_server_edit(message, state, session, services, "host", new_host)


@router.message(EditServerStates.max_clients, IsDev())
async def message_edit_max_clients(message: Message, state: FSMContext, session: AsyncSession, services: ServicesContainer):
    new_max_clients = message.text.strip()
    if not is_valid_client_count(new_max_clients):
        await services.notification.notify_by_message(message, _("server_management:ntf:invalid_max_clients"), duration=5)
        return
    await handle_server_edit(message, state, session, services, "max_clients", int(new_max_clients))

@router.message(EditServerStates.location, IsDev())
async def message_edit_location(message: Message, state: FSMContext, session: AsyncSession, services: ServicesContainer):
    new_location = message.text.strip()
    if not new_location:
        await services.notification.notify_by_message(message, "❌ Локация не может быть пустой.", duration=5)
        return
    await handle_server_edit(message, state, session, services, "location", new_location)


async def handle_server_edit(message: Message, state: FSMContext, session: AsyncSession, services: ServicesContainer, field_to_update: str, new_value: Any):
    data = await state.get_data()
    server_id = data.get("server_id_to_edit")
    main_message_id = data.get(MAIN_MESSAGE_ID_KEY)

    server_to_edit = await Server.get_by_id(session, id=server_id)
    if not server_to_edit:
        await services.notification.notify_by_message(message, "❌ Не удалось найти сервер для обновления.", duration=5)
        return

    # Update the server object directly and commit
    setattr(server_to_edit, field_to_update, new_value)
    await session.commit()

    await services.server_pool.sync_servers()
    await services.notification.notify_by_message(message, "✅ Сервер успешно обновлен!", duration=3)

    await _show_server_view(
        bot=message.bot,
        chat_id=message.chat.id,
        message_id=main_message_id,
        session=session,
        server_id=server_id,
        state=state,
    )


# --- Other server actions ---
@router.callback_query(F.data.startswith(f"{NavAdminTools.PING_SERVER}:"), IsDev())
async def callback_ping_server(callback: CallbackQuery, session: AsyncSession, services: ServicesContainer):
    server_id = int(callback.data.split(":")[1])
    server = await Server.get_by_id(session=session, id=server_id)
    if not server:
        await callback.answer("❌ Сервер не найден.", show_alert=True)
        return
        
    ping = await ping_url(server.host)
    if ping:
        await services.notification.show_popup(callback, _("server_management:popup:ping").format(server_name=server.name, ping=ping))
    else:
        await services.notification.show_popup(callback, _("server_management:popup:ping_failed").format(server_name=server.name))


@router.callback_query(F.data.startswith(f"{NavAdminTools.DELETE_SERVER}:"), IsDev())
async def callback_delete_server(callback: CallbackQuery, user: User, session: AsyncSession, state: FSMContext, services: ServicesContainer):
    server_id = int(callback.data.split(":")[1])
    server = await Server.get_by_id(session=session, id=server_id)
    if not server:
        await callback.answer("❌ Сервер не найден.", show_alert=True)
        return
        
    logger.info(f"Dev {user.tg_id} deleting server '{server.name}'.")
    deleted = await Server.delete(session=session, name=server.name) # Keep using name for now, or refactor delete
    if deleted:
        await services.server_pool.sync_servers()
        await services.notification.show_popup(callback, _("server_management:popup:deleted_success"))
        await callback_server_management(callback, user, session, state)
    else:
        await services.notification.show_popup(callback, _("server_management:popup:delete_failed"))
