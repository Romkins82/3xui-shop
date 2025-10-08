import logging
import traceback

from aiogram import Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import ExceptionTypeFilter
from aiogram.types import BufferedInputFile, ErrorEvent
from aiogram.utils.formatting import Bold, Code, Text

from app.bot.models import ServicesContainer
from app.bot.utils.misc import split_text
from app.config import Config

logger = logging.getLogger(__name__)
router = Router(name=__name__)

@router.errors(ExceptionTypeFilter(Exception))
async def errors_handler(event: ErrorEvent, config: Config, services: ServicesContainer) -> bool:
    if isinstance(event.exception, TelegramForbiddenError):
        # Эта часть в порядке, оставляем как есть
        # Предполагаем, что блокировка происходит после отправки сообщения
        if event.update.message and event.update.message.from_user:
            logger.info(f"User {event.update.message.from_user.id} blocked the bot.")
        return True

    # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
    if isinstance(event.exception, TelegramBadRequest):
        user_id = "Unknown"
        # Пытаемся получить ID пользователя сначала из callback_query
        if event.update.callback_query and event.update.callback_query.from_user:
            user_id = event.update.callback_query.from_user.id
        # Если не получилось, пытаемся получить из message
        elif event.update.message and event.update.message.from_user:
            user_id = event.update.message.from_user.id
        
        logger.warning(f"User {user_id} caused a bad request for edit/send message.")
        return True
    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    logger.exception(f"Update: {event.update}\nException: {event.exception}")

    if not config.bot.DEV_ID:
        return True

    try:
        text = Text(Bold((type(event.exception).__name__)), f": {str(event.exception)[:1021]}...")
        await services.notification.notify_developer(
            text=text.as_html(),
            document=BufferedInputFile(
                file=traceback.format_exc().encode(),
                filename=f"error_{event.update.update_id}.txt",
            ),
        )

        update_json = event.update.model_dump_json(indent=2, exclude_none=True)
        for chunk in split_text(update_json):
            await services.notification.notify_developer(
                text=Code(chunk).as_html(),
            )

    except TelegramBadRequest as exception:
        logger.warning(f"Failed to send error details: {exception}")
    except Exception as exception:
        logger.error(f"Unexpected error in error handler: {exception}")

    return True
