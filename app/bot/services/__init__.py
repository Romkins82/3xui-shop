# app/bot/services/__init__.py
from aiogram import Bot
from sqlalchemy.ext.asyncio import async_sessionmaker
from typing import Optional # Добавить Optional

from app.bot.models import ServicesContainer
from app.config import Config

from .invite_stats import InviteStatsService
from .notification import NotificationService
from .payment_stats import PaymentStatsService
from .plan import PlanService
from .referral import ReferralService
from .server_pool import ServerPoolService
from .subscription import SubscriptionService
from .vpn import VPNService


async def initialize(
    config: Config,
    session: async_sessionmaker,
    bot: Bot,
) -> ServicesContainer:
    # Инициализируем VPNService *перед* ServerPoolService
    vpn = VPNService(config=config, session=session, server_pool_service=None) # Пока None
    # Теперь передаем vpn_service в ServerPoolService
    server_pool = ServerPoolService(config=config, session=session, vpn_service=vpn)
    # И теперь устанавливаем server_pool_service в уже созданный vpn_service
    vpn.server_pool_service = server_pool
    # server_pool.set_vpn_service(vpn) # Альтернативный вариант через set_vpn_service

    plan = PlanService()
    notification = NotificationService(config=config, bot=bot)
    referral = ReferralService(config=config, session_factory=session, vpn_service=vpn)
    subscription = SubscriptionService(config=config, session_factory=session, vpn_service=vpn)
    payment_stats = PaymentStatsService(session_factory=session)
    invite_stats = InviteStatsService(session_factory=session, payment_stats_service=payment_stats)

    return ServicesContainer(
        server_pool=server_pool,
        plan=plan,
        vpn=vpn,
        notification=notification,
        referral=referral,
        subscription=subscription,
        payment_stats=payment_stats,
        invite_stats=invite_stats,
    )