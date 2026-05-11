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

from gateway.core.models import App, Customer, Plan, Payment, Subscription
from gateway.core.constants import (
    ALIPAY_SUPPORTED_CURRENCIES,
    WECHAT_PAY_SUPPORTED_CURRENCIES,
    APP_MANAGED_METHODS,
    SubscriptionStatus,
    ProrationMode,
    PaymentMethod,
    PaymentStatus,
)
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

    async def _cancel_pending_renewal_payment(
        self,
        subscription: Subscription,
        *,
        log,
    ) -> None:
        """Cancel the currently linked app-managed renewal payment, if any."""
        if not subscription.renewal_payment_id:
            return

        adapter = get_adapter(subscription.provider)
        try:
            renewal_payment = await self.session.get(
                Payment, subscription.renewal_payment_id
            )
            if renewal_payment and renewal_payment.status == PaymentStatus.pending:
                if renewal_payment.provider_txn_id:
                    await adapter.cancel_payment(
                        merchant_order_no=renewal_payment.merchant_order_no,
                        provider_txn_id=renewal_payment.provider_txn_id,
                    )
                renewal_payment.status = PaymentStatus.canceled
        except Exception as e:
            log.warning("cancel_renewal_payment_failed", error=str(e))

        subscription.renewal_payment_id = None
        subscription.renewal_attempts = 0
        subscription.last_renewal_notified_at = None
        subscription.grace_period_end = None

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
                # Sync to provider so Checkout (card path) pre-fills the new
                # email. Failure is non-fatal — log and keep local row updated;
                # the next subscribe will retry.
                try:
                    await adapter.update_customer_email(
                        customer.provider_customer_id, req.email
                    )
                except NotImplementedError:
                    pass
                except Exception as e:
                    logger.warning(
                        "update_customer_email_failed",
                        customer_id=str(customer.id),
                        provider_customer_id=customer.provider_customer_id,
                        error=str(e),
                    )
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

        if sub.is_app_managed:
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
                if sub.is_app_managed:
                    await self._cancel_pending_renewal_payment(sub, log=logger)
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

        payment_method = req.payment_method.value
        is_app_managed = payment_method in APP_MANAGED_METHODS

        plan_stmt = select(Plan).where(
            Plan.id == req.plan_id, Plan.app_id == app.id
        )
        plan = (await self.session.execute(plan_stmt)).scalar_one_or_none()
        if not plan or not plan.is_active:
            raise BadRequestException(message="目标计划不存在或已停用", code=4003)
        if not is_app_managed and not plan.provider_price_id:
            raise BadRequestException(
                message="目标计划尚未完成渠道同步", code=4005
            )
        if (
            payment_method == PaymentMethod.alipay.value
            and plan.currency.value not in ALIPAY_SUPPORTED_CURRENCIES
        ):
            raise BadRequestException(
                message=(
                    f"alipay 仅支持 {sorted(ALIPAY_SUPPORTED_CURRENCIES)} 币种，"
                    f"当前计划币种: {plan.currency.value}"
                ),
                code=4006,
            )
        if (
            payment_method == PaymentMethod.wechat_pay.value
            and plan.currency.value not in WECHAT_PAY_SUPPORTED_CURRENCIES
        ):
            raise BadRequestException(
                message=(
                    f"wechat_pay 仅支持 {sorted(WECHAT_PAY_SUPPORTED_CURRENCIES)} 币种，"
                    f"当前计划币种: {plan.currency.value}"
                ),
                code=4006,
            )
        if plan.provider.value not in settings.allowed_providers:
            raise BadRequestException(
                message=f"支付渠道 {plan.provider.value} 未启用", code=4004
            )

        adapter = get_adapter(plan.provider)

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

        if is_app_managed and not hasattr(adapter, "create_payment"):
            raise BadRequestException(
                message=f"渠道 {plan.provider.value} 不支持一次性支付", code=4007
            )

        trial_end = None
        if req.trial_period_days:
            trial_end = datetime.now(UTC) + timedelta(days=req.trial_period_days)

        if is_app_managed:
            # App-managed: 用一次性支付替代 Stripe 订阅
            user_meta = {
                k: v for k, v in (req.metadata or {}).items() if k != "_internal"
            }
            sub_meta = {
                **user_meta,
                "_internal": {
                    "success_url": req.success_url,
                    "cancel_url": req.cancel_url,
                    "payment_options": req.payment_options,
                },
            }

            if trial_end:
                now = datetime.now(UTC)
                subscription = Subscription(
                    id=subscription_id,
                    app_id=app.id,
                    provider=plan.provider,
                    customer_id=customer_id,
                    plan_id=plan.id,
                    provider_price_id=plan.provider_price_id,
                    amount=plan.amount,
                    currency=plan.currency,
                    status=SubscriptionStatus.trialing.value,
                    payment_method=payment_method,
                    current_period_start=now,
                    current_period_end=trial_end,
                    cancel_at_period_end=False,
                    trial_start=now,
                    trial_end=trial_end,
                    notify_url=req.notify_url or app.notify_url,
                    meta=sub_meta,
                )
                self.session.add(subscription)
                await self.session.flush()
                checkout_url = ""
            elif plan.amount <= 0:
                # 免费计划直接激活，跳过支付
                now = datetime.now(UTC)
                from gateway.core.billing import calculate_period_end
                period_end = calculate_period_end(
                    now, plan.interval, plan.interval_count
                )
                subscription = Subscription(
                    id=subscription_id,
                    app_id=app.id,
                    provider=plan.provider,
                    customer_id=customer_id,
                    plan_id=plan.id,
                    provider_price_id=plan.provider_price_id,
                    amount=plan.amount,
                    currency=plan.currency,
                    status=SubscriptionStatus.active.value,
                    payment_method=payment_method,
                    current_period_start=now,
                    current_period_end=period_end,
                    cancel_at_period_end=False,
                    notify_url=req.notify_url or app.notify_url,
                    meta=sub_meta,
                )
                self.session.add(subscription)
                await self.session.flush()
                checkout_url = ""
            else:
                payment_id = uuid.uuid4()
                merchant_order_no = f"sub_init_{subscription_id}"

                # Pass customer_email so Stripe Checkout can pre-fill the field
                # for app-managed methods (alipay / wechat_pay), which create a
                # mode=payment session and don't bind to the Stripe Customer.
                payment_metadata = {
                    "subscription_id": str(subscription_id),
                    "app_id": str(app.id),
                }
                if req.email:
                    payment_metadata["customer_email"] = req.email

                payment_result = await adapter.create_payment(
                    currency=plan.currency.value,
                    merchant_order_no=merchant_order_no,
                    quantity=1,
                    unit_amount=plan.amount,
                    product_name=plan.name,
                    notify_url=req.notify_url or app.notify_url,
                    payment_method=req.payment_method,
                    payment_options=req.payment_options,
                    success_url=req.success_url,
                    cancel_url=req.cancel_url,
                    metadata=payment_metadata,
                    app_id=str(app.id),
                    expire_minutes=settings.subscription_checkout_expire_minutes,
                )

                checkout_url = payment_result.payload.get("checkout_url", "")
                if not checkout_url:
                    log.warning(
                        "app_managed_subscription_missing_checkout_url",
                        provider_txn_id=payment_result.provider_txn_id,
                    )
                session_id = payment_result.provider_txn_id

            try:
                if not trial_end and plan.amount > 0:
                    subscription = Subscription(
                        id=subscription_id,
                        app_id=app.id,
                        provider=plan.provider,
                        customer_id=customer_id,
                        plan_id=plan.id,
                        provider_checkout_session_id=session_id,
                        provider_price_id=plan.provider_price_id,
                        amount=plan.amount,
                        currency=plan.currency,
                        status=SubscriptionStatus.incomplete.value,
                        payment_method=payment_method,
                        cancel_at_period_end=False,
                        trial_end=trial_end,
                        notify_url=req.notify_url or app.notify_url,
                        meta=sub_meta,
                    )
                    self.session.add(subscription)
                    await self.session.flush()

                    payment = Payment(
                        id=payment_id,
                        app_id=app.id,
                        merchant_order_no=merchant_order_no,
                        provider=plan.provider,
                        amount=plan.amount,
                        currency=plan.currency,
                        status=PaymentStatus.pending,
                        provider_txn_id=session_id,
                        subscription_id=subscription_id,
                    )
                    self.session.add(payment)
                    await self.session.flush()
            except Exception:
                if not trial_end and plan.amount > 0:
                    try:
                        await adapter.cancel_payment(
                            merchant_order_no=merchant_order_no,
                            provider_txn_id=session_id,
                        )
                    except Exception as cancel_err:
                        log.warning(
                            "compensate_cancel_payment_failed",
                            merchant_order_no=merchant_order_no,
                            error=str(cancel_err),
                        )
                raise
        else:
            # Provider-managed: 走 Stripe Subscription Checkout
            if not isinstance(adapter, SubscriptionProviderMixin):
                raise BadRequestException(
                    message=f"渠道 {plan.provider.value} 不支持订阅", code=4007
                )
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

            checkout_url = checkout_result.checkout_url

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
                payment_method=payment_method,
                cancel_at_period_end=False,
                trial_end=trial_end,
                notify_url=req.notify_url or app.notify_url,
                meta=req.metadata,
            )
            self.session.add(subscription)
            await self.session.flush()
        log.info(
            "subscription_created",
            subscription_id=str(subscription_id),
            checkout_url=checkout_url,
            payment_method=payment_method,
        )

        return subscription, checkout_url

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

        adapter = get_adapter(subscription.provider)

        if subscription.is_app_managed:
            # App-managed: 取消 pending renewal payment，无需调用 provider 取消订阅
            await self._cancel_pending_renewal_payment(subscription, log=log)

            now = datetime.now(UTC)
            if req.immediate or subscription.status == SubscriptionStatus.past_due.value:
                # past_due 状态下非立即取消等同立即（宽限期已清除，无法靠自动化终止）
                subscription.status = SubscriptionStatus.canceled.value
                subscription.canceled_at = now
                subscription.ended_at = now
            else:
                subscription.cancel_at_period_end = True
            subscription.last_event_at = now
        else:
            # Provider-managed: 调用 Stripe 取消订阅
            if not subscription.provider_subscription_id:
                raise BadRequestException(
                    message="订阅尚未激活，无法取消", code=4011
                )

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
                now = datetime.now(UTC)
                subscription.status = SubscriptionStatus.canceled.value
                subscription.canceled_at = now
                subscription.ended_at = now
            else:
                subscription.cancel_at_period_end = True
                if result.current_period_end:
                    subscription.current_period_end = result.current_period_end

        await self.session.flush()
        log.info("subscription_canceled", immediate=req.immediate)
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

        if subscription.is_app_managed:
            subscription.cancel_at_period_end = False
            subscription.last_event_at = now
        else:
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

        if subscription.is_app_managed:
            await self._cancel_pending_renewal_payment(subscription, log=logger)
            subscription.status = SubscriptionStatus.paused.value
            subscription.last_event_at = datetime.now(UTC)
        else:
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

        if subscription.is_app_managed:
            subscription.status = SubscriptionStatus.active.value
            subscription.last_event_at = datetime.now(UTC)
        else:
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

        _interval_rank = {"day": 0, "week": 1, "month": 2, "quarter": 3, "year": 4}
        if new_plan.tier != current_plan.tier:
            is_upgrade = new_plan.tier > current_plan.tier
        else:
            is_upgrade = _interval_rank.get(new_plan.interval, 1) > _interval_rank.get(current_plan.interval, 1)

        if subscription.is_app_managed:
            # App-managed: 升降级统一记录为 pending，在下次续费时生效
            subscription.pending_plan_id = new_plan.id
            subscription.pending_plan_change_at = subscription.current_period_end
            subscription.last_event_at = datetime.now(UTC)

            await self.session.flush()
            log.info(
                "app_managed_plan_change_scheduled",
                direction="upgrade" if is_upgrade else "downgrade",
                new_plan_id=str(new_plan.id),
                effective_at=str(subscription.current_period_end),
            )

            return {
                "direction": "upgrade" if is_upgrade else "downgrade",
                "effective": "next_renewal",
                "current_plan": current_plan,
                "pending_plan": new_plan,
                "pending_plan_change_at": subscription.current_period_end,
                "status": subscription.status,
            }

        if not new_plan.provider_price_id:
            raise BadRequestException(
                message="目标计划尚未完成渠道同步", code=4005
            )

        adapter = get_adapter(subscription.provider)
        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(message="渠道不支持订阅操作", code=4007)

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

        if subscription.is_app_managed:
            return {
                "currency": new_plan.currency.value,
                "total": new_plan.amount,
                "lines": [
                    {
                        "amount": new_plan.amount,
                        "description": f"{new_plan.name} next renewal",
                    }
                ],
            }

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

        if not subscription.pending_plan_id:
            raise BadRequestException(message="无待生效的计划变更", code=4006)

        if subscription.is_app_managed:
            subscription.pending_plan_id = None
            subscription.pending_plan_change_at = None
            subscription.last_event_at = datetime.now(UTC)
        else:
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
