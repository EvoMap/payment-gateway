"""
订阅核心服务（创建/取消/恢复/暂停/升降级）
"""

import asyncio
import hashlib
import struct
import uuid
from datetime import datetime, timedelta, UTC

import structlog
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.models import App, Customer, Plan, Subscription
from gateway.core.constants import SubscriptionStatus, ProrationMode
from gateway.core.settings import get_settings
from gateway.core.exceptions import (
    NotFoundException,
    BadRequestException,
    ConflictException,
)
from gateway.core.schemas import (
    CreateSubscriptionRequest,
    ChangePlanRequest,
    CancelSubscriptionRequest,
)
from gateway.db import get_session_ctx
from gateway.providers import get_adapter
from gateway.providers.base import SubscriptionProviderMixin

logger = structlog.get_logger(__name__)
settings = get_settings()


class SubscriptionService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def _get_or_create_customer(
        self,
        customer_session: AsyncSession,
        app: App,
        req: CreateSubscriptionRequest,
        adapter: SubscriptionProviderMixin,
        provider,
    ) -> Customer:
        """查找/创建 Customer（独立事务，不受外层回滚影响）"""
        stmt = select(Customer).where(
            Customer.app_id == app.id,
            Customer.external_user_id == req.external_user_id,
            Customer.provider == provider,
        )
        result = await customer_session.execute(stmt)
        customer = result.scalar_one_or_none()

        if customer:
            if req.email and customer.email != req.email:
                customer.email = req.email
            return customer

        provider_customer_id = await adapter.create_customer(
            email=req.email,
            metadata={
                "app_id": str(app.id),
                "external_user_id": req.external_user_id,
            },
        )

        customer = Customer(
            id=uuid.uuid4(),
            app_id=app.id,
            provider=provider,
            external_user_id=req.external_user_id,
            provider_customer_id=provider_customer_id,
            email=req.email,
        )
        customer_session.add(customer)
        await customer_session.flush()
        logger.info(
            "Customer 创建完成",
            customer_id=str(customer.id),
            provider_customer_id=provider_customer_id,
        )
        return customer

    async def _stripe_cleanup_remote_for_force(self, sub: Subscription) -> None:
        """仅调用 Stripe 侧清理（不写 DB）。供 force_cleanup 并发执行。"""
        sid = str(sub.id)
        log = logger.bind(subscription_id=sid)

        if sub.status == SubscriptionStatus.incomplete.value:
            if sub.provider_checkout_session_id:
                adapter = get_adapter(sub.provider)
                try:
                    await adapter.cancel_payment(
                        merchant_order_no=sid,
                        provider_txn_id=sub.provider_checkout_session_id,
                    )
                    log.info("incomplete_checkout_session_expired")
                except Exception as e:
                    log.warning(
                        "incomplete_checkout_expire_failed", error=str(e)
                    )
            return

        allowed = {
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
            SubscriptionStatus.past_due.value,
            SubscriptionStatus.paused.value,
        }
        if sub.status not in allowed:
            return

        if not sub.provider_subscription_id:
            log.warning("force_cleanup_skip_no_provider_subscription_id")
            return

        adapter = get_adapter(sub.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        if sub.provider_schedule_id:
            try:
                await adapter.release_subscription_schedule(
                    sub.provider_schedule_id
                )
            except Exception as e:
                log.warning(
                    "释放降级 Schedule 失败，继续取消",
                    error=str(e),
                    schedule_id=sub.provider_schedule_id,
                )

        await adapter.cancel_subscription(
            sub.provider_subscription_id, immediate=True
        )

    async def _force_cleanup_conflicting_subscriptions(
        self, app: App, customer_id: uuid.UUID
    ) -> None:
        """
        取消/过期该客服下所有会阻塞新订阅创建的记录（incomplete + 活跃类）。
        Stripe 调用并发执行；ORM 更新在同一会话内顺序提交。
        """
        cleanup_statuses = [
            SubscriptionStatus.incomplete.value,
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
            SubscriptionStatus.past_due.value,
            SubscriptionStatus.paused.value,
        ]
        stmt = (
            select(Subscription)
            .where(
                Subscription.app_id == app.id,
                Subscription.customer_id == customer_id,
                Subscription.status.in_(cleanup_statuses),
            )
            .order_by(Subscription.created_at.asc())
        )
        result = await self.session.execute(stmt)
        subs = list(result.scalars().all())
        if not subs:
            return

        log = logger.bind(
            app_id=str(app.id),
            customer_id=str(customer_id),
            count=len(subs),
        )
        log.info("force_cleanup_subscriptions_start")

        # 主线程先触达列，避免并发协程中懒加载命中 async session
        for s in subs:
            _ = (
                s.provider,
                s.status,
                s.provider_checkout_session_id,
                s.provider_subscription_id,
                s.provider_schedule_id,
            )

        outcomes = await asyncio.gather(
            *[self._stripe_cleanup_remote_for_force(s) for s in subs],
            return_exceptions=True,
        )

        allowed_active = {
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
            SubscriptionStatus.past_due.value,
            SubscriptionStatus.paused.value,
        }
        now = datetime.now(UTC)
        for sub, outcome in zip(subs, outcomes):
            if isinstance(outcome, Exception):
                logger.error(
                    "force_cleanup_stripe_failed",
                    subscription_id=str(sub.id),
                    error=str(outcome),
                )
                continue

            if sub.status == SubscriptionStatus.incomplete.value:
                sub.status = SubscriptionStatus.incomplete_expired.value
                sub.canceled_at = now
                sub.ended_at = now
            elif sub.status in allowed_active:
                if sub.provider_schedule_id:
                    sub.pending_plan_id = None
                    sub.pending_plan_change_at = None
                    sub.provider_schedule_id = None
                sub.status = SubscriptionStatus.canceled.value
                sub.canceled_at = now
                sub.ended_at = now
                sub.cancel_at_period_end = False

        await self.session.flush()
        log.info("force_cleanup_subscriptions_done")

    async def create_subscription(
        self, app: App, req: CreateSubscriptionRequest
    ) -> tuple[Subscription, str]:
        """
        创建订阅，返回 (subscription, checkout_url)

        事务安全：Customer 使用独立事务（不受外层回滚影响），
        Stripe Checkout Session 创建在 DB 写入之前执行。
        """
        log = logger.bind(
            app_id=str(app.id),
            external_user_id=req.external_user_id,
            plan_id=str(req.plan_id),
        )

        plan_stmt = select(Plan).where(
            Plan.id == req.plan_id, Plan.app_id == app.id
        )
        plan = (await self.session.execute(plan_stmt)).scalar_one_or_none()
        if not plan or not plan.is_active:
            raise BadRequestException(message="目标计划不存在或已停用", code=4003)
        if not plan.provider_price_id:
            raise BadRequestException(
                message="目标计划尚未完成渠道同步", code=4005
            )
        if plan.provider.value not in settings.allowed_providers:
            raise BadRequestException(
                message=f"支付渠道 {plan.provider.value} 未启用", code=4004
            )

        adapter = get_adapter(plan.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(
                message=f"渠道 {plan.provider.value} 不支持订阅", code=4007
            )

        async with get_session_ctx() as customer_session:
            customer = await self._get_or_create_customer(
                customer_session, app, req, adapter, plan.provider
            )
            customer_id = customer.id
            provider_customer_id = customer.provider_customer_id

        lock_key = struct.unpack(
            ">q",
            hashlib.sha256(
                f"{app.id}:customer:{customer_id}".encode()
            ).digest()[:8],
        )[0]
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key}
        )

        if req.force_cleanup:
            await self._force_cleanup_conflicting_subscriptions(app, customer_id)

        incomplete_stmt = select(func.count()).where(
            Subscription.customer_id == customer_id,
            Subscription.status == SubscriptionStatus.incomplete.value,
        )
        if (await self.session.execute(incomplete_stmt)).scalar() >= 1:
            raise ConflictException(message="该用户已有未完成的订阅", code=4094)

        if settings.subscription_single_active:
            active_stmt = select(func.count()).where(
                Subscription.customer_id == customer_id,
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.trialing.value,
                    SubscriptionStatus.past_due.value,
                    SubscriptionStatus.paused.value,
                ]),
            )
            if (await self.session.execute(active_stmt)).scalar() >= 1:
                raise ConflictException(
                    message="该用户已有活跃订阅，请先取消后再创建", code=4095
                )

        subscription_id = uuid.uuid4()

        checkout_result = await adapter.create_subscription_checkout(
            customer_id=provider_customer_id,
            price_id=plan.provider_price_id,
            subscription_id=str(subscription_id),
            app_id=str(app.id),
            plan_id=str(plan.id),
            success_url=req.success_url,
            cancel_url=req.cancel_url,
            metadata=req.metadata,
            trial_period_days=req.trial_period_days,
            expire_minutes=settings.subscription_checkout_expire_minutes,
        )

        trial_end = None
        if req.trial_period_days:
            trial_end = datetime.now(UTC) + timedelta(days=req.trial_period_days)

        subscription = Subscription(
            id=subscription_id,
            app_id=app.id,
            provider=plan.provider,
            customer_id=customer_id,
            plan_id=plan.id,
            provider_checkout_session_id=checkout_result.session_id,
            provider_price_id=plan.provider_price_id,
            amount=plan.amount,
            currency=plan.currency,
            status=SubscriptionStatus.incomplete.value,
            cancel_at_period_end=False,
            trial_end=trial_end,
            notify_url=req.notify_url or app.notify_url,
            meta=req.metadata,
        )

        self.session.add(subscription)
        await self.session.flush()
        log.info(
            "订阅已创建",
            subscription_id=str(subscription_id),
            checkout_url=checkout_result.checkout_url,
        )

        return subscription, checkout_result.checkout_url

    async def get_subscription(
        self, app_id: uuid.UUID, subscription_id: uuid.UUID
    ) -> Subscription:
        stmt = select(Subscription).where(
            Subscription.id == subscription_id,
            Subscription.app_id == app_id,
        )
        result = await self.session.execute(stmt)
        sub = result.scalar_one_or_none()
        if not sub:
            raise NotFoundException(message="订阅不存在", code=4049)
        return sub

    async def get_user_active_subscription(
        self, app_id: uuid.UUID, external_user_id: str
    ) -> Subscription | None:
        stmt = (
            select(Subscription)
            .join(Customer, Subscription.customer_id == Customer.id)
            .where(
                Subscription.app_id == app_id,
                Customer.external_user_id == external_user_id,
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.trialing.value,
                    SubscriptionStatus.past_due.value,
                    SubscriptionStatus.paused.value,
                ]),
            )
            .order_by(Subscription.created_at.desc())
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_subscriptions(
        self,
        app_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
        external_user_id: str | None = None,
        status: str | None = None,
    ) -> tuple[list[Subscription], int]:
        conditions: list = [Subscription.app_id == app_id]
        needs_join = False

        if external_user_id:
            conditions.append(Customer.external_user_id == external_user_id)
            needs_join = True

        if status:
            conditions.append(Subscription.status == status)

        count_base = select(func.count()).select_from(Subscription)
        if needs_join:
            count_base = count_base.join(
                Customer, Subscription.customer_id == Customer.id
            )
        count_stmt = count_base.where(*conditions)
        total = (await self.session.execute(count_stmt)).scalar_one()

        base = select(Subscription)
        if needs_join:
            base = base.join(
                Customer, Subscription.customer_id == Customer.id
            )
        stmt = (
            base.where(*conditions)
            .order_by(Subscription.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await self.session.execute(stmt)
        subs = list(result.scalars().all())
        return subs, total

    async def cancel_subscription(
        self,
        app_id: uuid.UUID,
        subscription_id: uuid.UUID,
        req: CancelSubscriptionRequest,
    ) -> Subscription:
        subscription = await self.get_subscription(app_id, subscription_id)
        log = logger.bind(subscription_id=str(subscription_id))

        # Handle incomplete subscriptions: expire the checkout session
        if subscription.status == SubscriptionStatus.incomplete.value:
            if subscription.provider_checkout_session_id:
                adapter = get_adapter(subscription.provider)
                try:
                    await adapter.cancel_payment(
                        merchant_order_no=str(subscription_id),
                        provider_txn_id=subscription.provider_checkout_session_id,
                    )
                    log.info("incomplete_checkout_session_expired")
                except Exception as e:
                    log.warning("incomplete_checkout_expire_failed", error=str(e))

            subscription.status = SubscriptionStatus.incomplete_expired.value
            subscription.canceled_at = datetime.now(UTC)
            subscription.ended_at = datetime.now(UTC)
            await self.session.flush()
            log.info("incomplete_subscription_expired")
            return subscription

        allowed_statuses = {
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
            SubscriptionStatus.past_due.value,
            SubscriptionStatus.paused.value,
        }
        if subscription.status not in allowed_statuses:
            raise BadRequestException(message="当前状态不允许取消", code=4010)

        if not subscription.provider_subscription_id:
            raise BadRequestException(
                message="订阅尚未激活，无法取消", code=4011
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        if subscription.provider_schedule_id:
            try:
                await adapter.release_subscription_schedule(
                    subscription.provider_schedule_id
                )
            except Exception as e:
                log.warning(
                    "释放降级 Schedule 失败，继续取消",
                    error=str(e),
                    schedule_id=subscription.provider_schedule_id,
                )
            subscription.pending_plan_id = None
            subscription.pending_plan_change_at = None
            subscription.provider_schedule_id = None

        result = await adapter.cancel_subscription(
            subscription.provider_subscription_id, immediate=req.immediate
        )

        if req.immediate:
            subscription.status = SubscriptionStatus.canceled.value
            subscription.canceled_at = datetime.now(UTC)
            subscription.ended_at = datetime.now(UTC)
        else:
            subscription.cancel_at_period_end = True
            if result.current_period_end:
                subscription.current_period_end = result.current_period_end

        await self.session.flush()
        log.info("订阅取消完成", immediate=req.immediate)
        return subscription

    async def resume_subscription(
        self, app_id: uuid.UUID, subscription_id: uuid.UUID
    ) -> Subscription:
        subscription = await self.get_subscription(app_id, subscription_id)

        if subscription.status not in (
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
        ):
            raise BadRequestException(message="当前状态不允许恢复", code=4012)

        if not subscription.cancel_at_period_end:
            raise BadRequestException(
                message="订阅未设置周期末取消", code=4013
            )

        now = datetime.now(UTC)
        if (
            subscription.current_period_end
            and subscription.current_period_end <= now
        ):
            raise BadRequestException(message="订阅已过期，请重新创建", code=4014)

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        await adapter.resume_subscription(subscription.provider_subscription_id)
        subscription.cancel_at_period_end = False

        await self.session.flush()
        return subscription

    async def pause_subscription(
        self, app_id: uuid.UUID, subscription_id: uuid.UUID
    ) -> Subscription:
        subscription = await self.get_subscription(app_id, subscription_id)

        if subscription.status not in (
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
        ):
            raise BadRequestException(message="当前状态不允许暂停", code=4015)

        if subscription.cancel_at_period_end:
            raise BadRequestException(
                message="待取消订阅不允许暂停", code=4016
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        await adapter.pause_subscription(subscription.provider_subscription_id)
        subscription.status = SubscriptionStatus.paused.value

        await self.session.flush()
        return subscription

    async def unpause_subscription(
        self, app_id: uuid.UUID, subscription_id: uuid.UUID
    ) -> Subscription:
        subscription = await self.get_subscription(app_id, subscription_id)

        if subscription.status != SubscriptionStatus.paused.value:
            raise BadRequestException(
                message="订阅未处于暂停状态", code=4017
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        await adapter.unpause_subscription(subscription.provider_subscription_id)
        subscription.status = SubscriptionStatus.active.value

        await self.session.flush()
        return subscription

    async def change_plan(
        self,
        app_id: uuid.UUID,
        subscription_id: uuid.UUID,
        req: ChangePlanRequest,
    ) -> dict:
        subscription = await self.get_subscription(app_id, subscription_id)
        log = logger.bind(subscription_id=str(subscription_id))

        allowed_statuses = {
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
            SubscriptionStatus.paused.value,
        }
        if subscription.status not in allowed_statuses:
            raise BadRequestException(
                message="当前状态不允许变更计划", code=4018
            )

        if subscription.pending_plan_id:
            raise BadRequestException(
                message="已有待生效的计划变更", code=4019
            )

        new_plan = await self._get_plan(app_id, req.new_plan_id)
        current_plan = await self._get_plan(app_id, subscription.plan_id)

        if new_plan.id == current_plan.id:
            raise BadRequestException(message="目标计划与当前计划相同", code=4020)

        if not new_plan.is_active:
            raise BadRequestException(message="目标计划已停用", code=4021)

        if new_plan.provider != current_plan.provider:
            raise BadRequestException(
                message="跨渠道变更不支持", code=4022
            )

        if new_plan.tier == current_plan.tier and new_plan.interval == current_plan.interval:
            raise BadRequestException(
                message="同等级同周期计划不允许变更", code=4023
            )

        if not new_plan.provider_price_id:
            raise BadRequestException(
                message="目标计划尚未完成渠道同步", code=4005
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        _interval_rank = {"week": 0, "month": 1, "quarter": 2, "year": 3}
        if new_plan.tier != current_plan.tier:
            is_upgrade = new_plan.tier > current_plan.tier
        else:
            is_upgrade = _interval_rank.get(new_plan.interval, 1) > _interval_rank.get(current_plan.interval, 1)

        if is_upgrade:
            if subscription.status == SubscriptionStatus.paused.value:
                raise BadRequestException(
                    message="暂停状态下不允许升级，请先恢复", code=4024
                )

            if subscription.provider_schedule_id:
                try:
                    await adapter.release_subscription_schedule(
                        subscription.provider_schedule_id
                    )
                except Exception as e:
                    log.warning("释放降级 Schedule 失败", error=str(e))
                subscription.pending_plan_id = None
                subscription.pending_plan_change_at = None
                subscription.provider_schedule_id = None

            proration = (
                req.proration_mode.value
                if req.proration_mode
                else ProrationMode.auto.value
            )

            customer_stmt = select(Customer).where(
                Customer.id == subscription.customer_id
            )
            customer = (
                await self.session.execute(customer_stmt)
            ).scalar_one_or_none()

            upgrade_result = await adapter.change_subscription_plan(
                subscription.provider_subscription_id,
                new_price_id=new_plan.provider_price_id,
                proration_mode=proration,
                credit_amount=req.credit_amount,
                currency=(
                    subscription.currency.value if req.credit_amount else None
                ),
                customer_id=(
                    customer.provider_customer_id
                    if customer and req.credit_amount
                    else None
                ),
            )

            subscription.plan_id = new_plan.id
            subscription.provider_price_id = new_plan.provider_price_id
            subscription.amount = new_plan.amount
            subscription.pending_plan_id = None
            subscription.pending_plan_change_at = None
            subscription.provider_schedule_id = None

            # Update billing cycle from Stripe (reset by billing_cycle_anchor=now)
            if upgrade_result.current_period_end:
                subscription.current_period_start = datetime.now(UTC)
                subscription.current_period_end = upgrade_result.current_period_end

            await self.session.flush()
            log.info("升级完成", new_plan_id=str(new_plan.id))

            return {
                "direction": "upgrade",
                "effective": "immediate",
                "current_plan": new_plan,
                "pending_plan": None,
                "pending_plan_change_at": None,
                "status": subscription.status,
                "current_period_end": subscription.current_period_end,
            }

        else:
            if subscription.status == SubscriptionStatus.paused.value:
                if req.proration_mode and req.proration_mode != ProrationMode.custom:
                    raise BadRequestException(
                        message="暂停状态下降级仅支持 custom 模式", code=4025
                    )

            if not subscription.current_period_end:
                raise BadRequestException(
                    message="订阅缺少周期信息，无法降级", code=4026
                )

            period_end_ts = int(subscription.current_period_end.timestamp())

            schedule_id = await adapter.schedule_subscription_downgrade(
                subscription.provider_subscription_id,
                new_price_id=new_plan.provider_price_id,
                current_period_end=period_end_ts,
            )

            subscription.pending_plan_id = new_plan.id
            subscription.pending_plan_change_at = subscription.current_period_end
            subscription.provider_schedule_id = schedule_id

            await self.session.flush()
            log.info(
                "降级已调度",
                new_plan_id=str(new_plan.id),
                effective_at=str(subscription.current_period_end),
            )

            return {
                "direction": "downgrade",
                "effective": "period_end",
                "current_plan": current_plan,
                "pending_plan": new_plan,
                "pending_plan_change_at": subscription.current_period_end,
                "status": subscription.status,
            }

    async def preview_change(
        self,
        app_id: uuid.UUID,
        subscription_id: uuid.UUID,
        new_plan_id: uuid.UUID,
    ) -> dict:
        """预览变更计划的费用，直接调用 Stripe Invoice.create_preview。"""
        subscription = await self.get_subscription(app_id, subscription_id)

        allowed_statuses = {
            SubscriptionStatus.active.value,
            SubscriptionStatus.trialing.value,
        }
        if subscription.status not in allowed_statuses:
            raise BadRequestException(
                message="当前状态不允许预览变更", code=4018
            )

        new_plan = await self._get_plan(app_id, new_plan_id)
        if not new_plan.is_active:
            raise BadRequestException(message="目标计划已停用", code=4021)
        if not new_plan.provider_price_id:
            raise BadRequestException(
                message="目标计划尚未完成渠道同步", code=4005
            )

        if not subscription.provider_subscription_id:
            raise BadRequestException(
                message="订阅尚未激活，无法预览", code=4011
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        return await adapter.preview_plan_change(
            subscription.provider_subscription_id,
            new_price_id=new_plan.provider_price_id,
        )

    async def cancel_pending_downgrade(
        self, app_id: uuid.UUID, subscription_id: uuid.UUID
    ) -> Subscription:
        subscription = await self.get_subscription(app_id, subscription_id)

        if not subscription.provider_schedule_id:
            raise BadRequestException(message="无待生效的计划变更", code=4006)

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

        await adapter.release_subscription_schedule(
            subscription.provider_schedule_id
        )

        subscription.pending_plan_id = None
        subscription.pending_plan_change_at = None
        subscription.provider_schedule_id = None

        await self.session.flush()
        return subscription

    async def _get_plan(self, app_id: uuid.UUID, plan_id: uuid.UUID) -> Plan:
        stmt = select(Plan).where(Plan.id == plan_id, Plan.app_id == app_id)
        result = await self.session.execute(stmt)
        plan = result.scalar_one_or_none()
        if not plan:
            raise NotFoundException(message="计划不存在", code=4048)
        return plan
