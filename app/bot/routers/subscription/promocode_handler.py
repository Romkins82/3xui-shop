import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.i18n import gettext as _
from sqlalchemy.ext.asyncio import AsyncSession

from app.bot.models import ServicesContainer, SubscriptionData
from app.bot.utils.constants import MAIN_MESSAGE_ID_KEY
from app.bot.utils.formatting import format_subscription_period
from app.bot.utils.navigation import NavSubscription
from app.db.models import Promocode, User

from .keyboard import multiconfig_keyboard, promocode_keyboard

logger = logging.getLogger(__name__)
router = Router(name=__name__)


class ActivatePromocodeStates(StatesGroup):
    promocode_input = State()
    selecting_server = State()


@router.callback_query(F.data == NavSubscription.PROMOCODE)
async def callback_promocode(callback: CallbackQuery, user: User, state: FSMContext) -> None:
    logger.info(f"User {user.tg_id} started activating promocode.")
    await state.set_state(ActivatePromocodeStates.promocode_input)
    await callback.message.edit_text(
        text=_("promocode:message:main"),
        reply_markup=promocode_keyboard(),
    )


@router.message(ActivatePromocodeStates.promocode_input)
async def handle_promocode_input(
    message: Message,
    user: User,
    session: AsyncSession,
    state: FSMContext,
    services: ServicesContainer,
) -> None:
    input_promocode = message.text.strip()
    logger.info(f"User {user.tg_id} entered promocode: {input_promocode} for activating.")

    promocode = await Promocode.get(session=session, code=input_promocode)
    if promocode and not promocode.is_activated:
        servers = await services.server_pool.get_available_servers()
        if not servers:
            await services.notification.notify_by_message(
                message=message,
                text=_("promocode:ntf:no_available_servers"),
                duration=5,
            )
            return

        await state.update_data(promocode=input_promocode)
        await state.set_state(ActivatePromocodeStates.selecting_server)
        
        callback_data = SubscriptionData(state=NavSubscription.PROMOCODE_SERVER, user_id=user.tg_id)
        await message.answer(
            text=_("subscription:message:server"),
            reply_markup=multiconfig_keyboard(callback_data),
        )

    else:
        text = _("promocode:ntf:activate_invalid").format(promocode=input_promocode)
        await services.notification.notify_by_message(message=message, text=text, duration=5)


@router.callback_query(
    SubscriptionData.filter(F.state == NavSubscription.PROMOCODE_SERVER),
    ActivatePromocodeStates.selecting_server,
)
async def callback_promocode_server_selected(
    callback: CallbackQuery,
    user: User,
    session: AsyncSession,
    state: FSMContext,
    services: ServicesContainer,
    callback_data: SubscriptionData,
) -> None:
    data = await state.get_data()
    promocode_code = data.get("promocode")
    
    servers = await services.server_pool.get_available_servers()
    if not servers:
        await services.notification.show_popup(
            callback=callback, text=_("promocode:ntf:no_available_servers")
        )
        return

    server_id = servers[0].id
    logger.info(f"User {user.tg_id} auto-selected server {server_id} for promocode {promocode_code}.")

    promocode = await Promocode.get(session=session, code=promocode_code)
    success = await services.vpn.activate_promocode(user=user, promocode=promocode, server_id=server_id)
    
    main_message_id = await state.get_value(MAIN_MESSAGE_ID_KEY)
    
    await callback.message.delete()

    if success:
        await callback.bot.edit_message_text(
            text=_("promocode:message:activated_success").format(
                promocode=promocode_code,
                duration=format_subscription_period(promocode.duration),
            ),
            chat_id=callback.message.chat.id,
            message_id=main_message_id,
            reply_markup=promocode_keyboard(),
        )
    else:
        text = _("promocode:ntf:activate_failed")
        await callback.message.answer(text=text)

    await state.set_state(None)