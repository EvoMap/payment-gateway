"""
应用层续费服务（App-Managed Recurring Billing）

对 payment_method 为 wechat_pay/alipay 的订阅，由 Worker 驱动续费生命周期：
- Phase A: 到期前创建续费订单
- Phase B: 到期后宽限期处理
- Phase C: 续费提醒重发
"""

import copy
import uuid
from datetime import datetime, timedelta, UTC

import structlog
from sqlalchemy import and_, join, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.billing import calculate_period_end
from gateway.core.constants import (
    APP_MANAGED_METHODS,
    DeliveryStatus,
    PaymentMethod,
    PaymentStatus,
    SubscriptionStatus,
)
from gateway.core.models import (
    App,
    Customer,
    Payment,
    Plan,
    Subscription,
    WebhookDelivery,
)
from gateway.core.settings import get_settings
from gateway.providers import get_adapter

logger = structlog.get_logger(__name__)


def _calculate_next_period_end(start: datetime, plan: Plan) -> datetime:
    return calculate_period_end(start, plan.interval, plan.interval_count)


class RenewalService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.settings = get_settings()

    async def _cancel_linked_renewal_payment(
        self,
        sub: Subscription,
        *,
        log,
    ) -> None:
        if not sub.renewal_payment_id:
            return

        payment = await self.session.get(Payment, sub.renewal_payment_id)
        if payment and payment.status == PaymentStatus.pending:
            if payment.provider_txn_id:
                try:
                    adapter = get_adapter(sub.provider)
                    await adapter.cancel_payment(
                        merchant_order_no=payment.merchant_order_no,
                        provider_txn_id=payment.provider_txn_id,
                    )
                except Exception as cancel_err:
                    log.warning(
                        "cancel_linked_renewal_payment_failed",
                        payment_id=str(payment.id),
                        error=str(cancel_err),
                    )
            payment.status = PaymentStatus.canceled

        sub.renewal_payment_id = None
        sub.renewal_attempts = 0
        sub.last_renewal_notified_at = None

    async def expire_stale_renewal_payments(self):
        """Phase 0: 清理已进入终态的 renewal payment 关联。

        过期但仍 pending 的支付链接由 reminder 流程替换，避免用相同
        merchant_order_no 重新创建续费单触发唯一约束冲突。
        """
        j = join(
            Subscription, Payment,
            Subscription.renewal_payment_id == Payment.id,
        )
        stmt = (
            select(Subscription, Payment)
            .select_from(j)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.renewal_payment_id != None,  # noqa: E711
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.past_due.value,
                ]),
                Payment.status != PaymentStatus.pending,
            )
            .with_for_update(skip_locked=True, of=Subscription)
            .limit(self.settings.worker_batch_size)
        )

        result = await self.session.execute(stmt)
        rows = result.all()

        changed = False
        for sub, payment in rows:
            try:
                async with self.session.begin_nested():
                    # Best-effort 取消 provider 侧的支付链接（仅对非成功状态执行）
                    if (
                        payment.provider_txn_id
                        and payment.status != PaymentStatus.succeeded
                    ):
                        try:
                            adapter = get_adapter(sub.provider)
                            await adapter.cancel_payment(
                                merchant_order_no=payment.merchant_order_no,
                                provider_txn_id=payment.provider_txn_id,
                            )
                        except Exception as cancel_err:
                            logger.warning(
                                "expire_stale_cancel_provider_session_failed",
                                subscription_id=str(sub.id),
                                payment_id=str(payment.id),
                                error=str(cancel_err),
                            )
                    sub.renewal_payment_id = None
                    sub.renewal_attempts = 0
                    sub.last_renewal_notified_at = None
                    changed = True
            except Exception as e:
                logger.error(
                    "expire_stale_renewal_payment_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )

        if changed:
            await self.session.commit()

    async def scan_and_create_renewals(self):
        """Phase A: 为即将到期的 app-managed 订阅创建续费订单"""
        now = datetime.now(UTC)
        advance_cutoff = now + timedelta(
            days=self.settings.renewal_advance_days
        )

        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.cancel_at_period_end == False,  # noqa: E712
                Subscription.renewal_payment_id == None,  # noqa: E711
                Subscription.current_period_end != None,  # noqa: E711
                or_(
                    and_(
                        Subscription.status == SubscriptionStatus.active.value,
                        Subscription.current_period_end > now,
                        Subscription.current_period_end <= advance_cutoff,
                    ),
                    Subscription.status == SubscriptionStatus.past_due.value,
                ),
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )

        result = await self.session.execute(stmt)
        subscriptions = result.scalars().all()

        for sub in subscriptions:
            try:
                async with self.session.begin_nested():
                    await self._create_renewal_for_subscription(sub)
            except Exception as e:
                logger.error(
                    "create_renewal_payment_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )

        if subscriptions:
            await self.session.commit()

    async def enforce_grace_periods(self):
        """Phase B: 过期处理（trialing→active, active→past_due, past_due→canceled, cancel_at_period_end）

        每个子 phase 独立 commit，确保单个 phase 失败不阻塞其余 phase 执行。
        """
        now = datetime.now(UTC)

        for phase_fn in (
            self._enforce_trial_end,
            self._enforce_past_due,
            self._enforce_grace_expired,
            self._enforce_cancel_at_period_end,
        ):
            try:
                await phase_fn(now)
            except Exception as exc:
                logger.error(
                    "enforce_grace_phase_failed",
                    phase=phase_fn.__name__,
                    error=str(exc),
                    exc_info=True,
                )

    async def _enforce_trial_end(self, now: datetime):
        """B0: trialing → active（trial 到期，立即创建首次续费订单）"""
        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.status == SubscriptionStatus.trialing.value,
                Subscription.cancel_at_period_end == False,  # noqa: E712
                Subscription.current_period_end != None,  # noqa: E711
                Subscription.current_period_end <= now,
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )
        result = await self.session.execute(stmt)
        for sub in result.scalars().all():
            try:
                async with self.session.begin_nested():
                    sub.status = SubscriptionStatus.active.value
                    sub.last_event_at = now
                    # 保留 trial_start/trial_end 作为历史记录，不清除
                    # 将 period 设为零长度，Phase A 会在下次扫描时立即创建续费订单
                    sub.current_period_start = sub.current_period_end
                    # current_period_end 不推进，保持 <= now，触发 Phase A 创建续费
                    # trial_ended 先于 renewal_created 发送，确保商户按逻辑顺序收到
                    await self._create_renewal_webhook(sub, "trial_ended")
                    await self._create_renewal_for_subscription(sub)
                    logger.info(
                        "subscription_trial_ended",
                        subscription_id=str(sub.id),
                    )
            except Exception as e:
                logger.error(
                    "enforce_grace_periods_b0_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )
        await self.session.commit()

    async def _enforce_past_due(self, now: datetime):
        """B1: active → past_due（到期未续费，无有效 pending 续费订单）"""
        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.status == SubscriptionStatus.active.value,
                Subscription.current_period_end != None,  # noqa: E711
                Subscription.current_period_end < now,
                Subscription.cancel_at_period_end == False,  # noqa: E712
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )
        result = await self.session.execute(stmt)
        for sub in result.scalars().all():
            try:
                async with self.session.begin_nested():
                    has_pending_renewal = False
                    if sub.renewal_payment_id:
                        payment = await self.session.get(
                            Payment, sub.renewal_payment_id
                        )
                        has_pending_renewal = (
                            payment is not None
                            and payment.status == PaymentStatus.pending
                        )
                        if not has_pending_renewal:
                            sub.renewal_payment_id = None
                            sub.renewal_attempts = 0
                            sub.last_renewal_notified_at = None
                    sub.status = SubscriptionStatus.past_due.value
                    grace_base = max(sub.current_period_end, now)
                    sub.grace_period_end = grace_base + timedelta(
                        days=self.settings.renewal_grace_period_days
                    )
                    if not has_pending_renewal:
                        await self._create_renewal_for_subscription(sub)
                    await self._create_renewal_webhook(
                        sub, "past_due", grace_period_end=sub.grace_period_end
                    )
                    logger.info(
                        "subscription_moved_to_past_due",
                        subscription_id=str(sub.id),
                        grace_period_end=str(sub.grace_period_end),
                    )
            except Exception as e:
                logger.error(
                    "enforce_grace_periods_b1_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )
        await self.session.commit()

    async def _enforce_grace_expired(self, now: datetime):
        """B2: past_due → canceled（宽限期到期）"""
        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.status == SubscriptionStatus.past_due.value,
                Subscription.grace_period_end != None,  # noqa: E711
                Subscription.grace_period_end < now,
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )
        result = await self.session.execute(stmt)
        for sub in result.scalars().all():
            try:
                async with self.session.begin_nested():
                    sub.status = SubscriptionStatus.canceled.value
                    sub.canceled_at = now
                    sub.ended_at = now
                    await self._cancel_linked_renewal_payment(sub, log=logger)
                    sub.renewal_attempts = 0
                    sub.grace_period_end = None
                    await self._create_renewal_webhook(sub, "canceled")
                    logger.info(
                        "subscription_grace_period_expired",
                        subscription_id=str(sub.id),
                    )
            except Exception as e:
                logger.error(
                    "enforce_grace_periods_b2_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )
        await self.session.commit()

    async def _enforce_cancel_at_period_end(self, now: datetime):
        """B3: cancel_at_period_end 到期"""
        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.trialing.value,
                ]),
                Subscription.cancel_at_period_end == True,  # noqa: E712
                Subscription.current_period_end != None,  # noqa: E711
                Subscription.current_period_end < now,
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )
        result = await self.session.execute(stmt)
        for sub in result.scalars().all():
            try:
                async with self.session.begin_nested():
                    sub.status = SubscriptionStatus.canceled.value
                    sub.canceled_at = now
                    sub.ended_at = now
                    await self._cancel_linked_renewal_payment(sub, log=logger)
                    sub.grace_period_end = None
                    await self._create_renewal_webhook(sub, "canceled")
                    logger.info(
                        "subscription_canceled_at_period_end",
                        subscription_id=str(sub.id),
                    )
            except Exception as e:
                logger.error(
                    "enforce_grace_periods_b3_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )
        await self.session.commit()

    async def send_renewal_reminders(self):
        """Phase C: 对已有续费订单但仍未支付的订阅，重发通知"""
        now = datetime.now(UTC)
        interval_cutoff = now - timedelta(
            hours=self.settings.renewal_notification_interval_hours
        )

        stmt = (
            select(Subscription)
            .where(
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.past_due.value,
                ]),
                Subscription.cancel_at_period_end == False,  # noqa: E712
                Subscription.renewal_payment_id != None,  # noqa: E711
                Subscription.renewal_attempts < self.settings.renewal_max_notifications,
                # 距上次通知超过间隔
                (
                    (Subscription.last_renewal_notified_at == None)  # noqa: E711
                    | (Subscription.last_renewal_notified_at < interval_cutoff)
                ),
            )
            .with_for_update(skip_locked=True)
            .limit(self.settings.worker_batch_size)
        )

        result = await self.session.execute(stmt)
        subscriptions = result.scalars().all()

        for sub in subscriptions:
            try:
                async with self.session.begin_nested():
                    await self._send_reminder_for_subscription(sub)
            except Exception as e:
                logger.error(
                    "send_renewal_reminder_failed",
                    subscription_id=str(sub.id),
                    error=str(e),
                    exc_info=True,
                )

        if subscriptions:
            await self.session.commit()

    # ==================== 内部方法 ====================

    async def _create_renewal_for_subscription(self, sub: Subscription):
        """为单个订阅创建续费 Payment 并通知商户"""
        log = logger.bind(subscription_id=str(sub.id))

        if sub.renewal_payment_id:
            log.warning(
                "renewal_payment_already_exists_skipping",
                existing_payment_id=str(sub.renewal_payment_id),
            )
            return

        plan = await self.session.get(Plan, sub.plan_id)
        if not plan:
            log.error("renewal_subscription_plan_not_found")
            return

        # 如果有 pending plan change，使用新计划的金额
        amount = plan.amount
        currency = plan.currency
        pending_plan = None
        if sub.pending_plan_id:
            pending_plan = await self.session.get(Plan, sub.pending_plan_id)
            if pending_plan:
                amount = pending_plan.amount
                currency = pending_plan.currency

        # 免费计划直接推进周期，跳过支付
        if amount <= 0:
            old_period_end = sub.current_period_end
            effective_plan = (pending_plan if sub.pending_plan_id and pending_plan else plan)
            if sub.pending_plan_id:
                sub.plan_id = effective_plan.id
                sub.amount = effective_plan.amount
                sub.currency = effective_plan.currency
                sub.pending_plan_id = None
                sub.pending_plan_change_at = None
            sub.last_renewed_period_end = old_period_end
            sub.current_period_start = old_period_end
            sub.current_period_end = _calculate_next_period_end(old_period_end, effective_plan)
            sub.renewal_payment_id = None
            sub.renewal_attempts = 0
            sub.grace_period_end = None
            sub.last_event_at = datetime.now(UTC)
            log.info("renewal_auto_advanced_free_plan", new_period_end=str(sub.current_period_end))
            await self._create_renewal_webhook(sub, "renewed")
            return

        period_end_ts = int(sub.current_period_end.timestamp())
        payment_id = uuid.uuid4()
        merchant_order_no = f"sub_renew_{sub.id}_{period_end_ts}"

        adapter = get_adapter(sub.provider)
        payment_method_enum = PaymentMethod(sub.payment_method)

        meta = sub.meta or {}
        internal = meta.get("_internal", {})
        success_url = internal.get("success_url", "")
        cancel_url = internal.get("cancel_url", "")
        stored_payment_options = internal.get("payment_options")

        if not success_url or not cancel_url:
            log.warning(
                "renewal_missing_redirect_urls",
                has_success_url=bool(success_url),
                has_cancel_url=bool(cancel_url),
            )

        notify_url = sub.notify_url
        if not notify_url:
            app = await self.session.get(App, sub.app_id)
            notify_url = app.notify_url if app else None

        if not notify_url:
            log.error("renewal_no_notify_url_skipping")
            return

        payment_result = await adapter.create_payment(
            currency=currency.value,
            merchant_order_no=merchant_order_no,
            quantity=1,
            unit_amount=amount,
            product_name=plan.name,
            notify_url=notify_url,
            payment_method=payment_method_enum,
            payment_options=stored_payment_options,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "subscription_id": str(sub.id),
                "app_id": str(sub.app_id),
                "renewal": "true",
            },
            app_id=str(sub.app_id),
            expire_minutes=self.settings.renewal_payment_expire_minutes,
        )

        checkout_url = payment_result.payload.get("checkout_url", "")
        if not checkout_url:
            log.warning("renewal_payment_missing_checkout_url", provider_txn_id=payment_result.provider_txn_id)

        payment = Payment(
            id=payment_id,
            app_id=sub.app_id,
            merchant_order_no=merchant_order_no,
            provider=sub.provider,
            amount=amount,
            currency=currency,
            status=PaymentStatus.pending,
            provider_txn_id=payment_result.provider_txn_id,
            subscription_id=sub.id,
        )
        self.session.add(payment)

        sub.renewal_payment_id = payment_id
        sub.renewal_attempts = 1
        sub.last_renewal_notified_at = datetime.now(UTC)

        try:
            await self.session.flush()
        except Exception:
            try:
                await adapter.cancel_payment(
                    merchant_order_no=merchant_order_no,
                    provider_txn_id=payment_result.provider_txn_id,
                )
            except Exception as cancel_err:
                log.warning(
                    "compensate_cancel_renewal_payment_failed",
                    merchant_order_no=merchant_order_no,
                    error=str(cancel_err),
                )
            raise

        # 发送 webhook 通知商户
        await self._create_renewal_webhook(
            sub,
            "renewal_created",
            checkout_url=checkout_url,
            payment_id=str(payment_id),
            renewal_due_date=(
                sub.current_period_end.isoformat()
                if sub.current_period_end
                else None
            ),
        )

        log.info(
            "renewal_payment_created",
            payment_id=str(payment_id),
            amount=amount,
            currency=currency.value,
        )

    async def _send_reminder_for_subscription(self, sub: Subscription):
        """为已有续费订单的订阅创建新 Payment + 支付链接并重发通知"""
        if sub.cancel_at_period_end:
            return

        log = logger.bind(subscription_id=str(sub.id))

        stmt = select(Payment).where(Payment.id == sub.renewal_payment_id).with_for_update()
        result = await self.session.execute(stmt)
        old_payment = result.scalar_one_or_none()
        if not old_payment or old_payment.status != PaymentStatus.pending:
            sub.renewal_payment_id = None
            sub.renewal_attempts = 0
            return

        plan = await self.session.get(Plan, sub.plan_id)
        adapter = get_adapter(sub.provider)

        next_attempt = sub.renewal_attempts + 1
        new_payment_id = uuid.uuid4()
        period_end_ts = int(sub.current_period_end.timestamp()) if sub.current_period_end else 0
        new_merchant_order_no = f"sub_renew_{sub.id}_{period_end_ts}_r{next_attempt}"

        internal = (sub.meta or {}).get("_internal", {})
        success_url = internal.get("success_url", "")
        cancel_url = internal.get("cancel_url", "")
        stored_payment_options = internal.get("payment_options")

        notify_url = sub.notify_url
        if not notify_url:
            app = await self.session.get(App, sub.app_id)
            notify_url = app.notify_url if app else None

        if not notify_url:
            log.error("renewal_reminder_no_notify_url_skipping")
            return

        # 先创建新 payment，成功后再 cancel 旧的（避免 cancel 不可逆导致用户无法续费）
        payment_result = await adapter.create_payment(
            currency=old_payment.currency.value,
            merchant_order_no=new_merchant_order_no,
            quantity=1,
            unit_amount=old_payment.amount,
            product_name=plan.name if plan else "订阅续费",
            notify_url=notify_url,
            payment_method=PaymentMethod(sub.payment_method),
            payment_options=stored_payment_options,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "subscription_id": str(sub.id),
                "app_id": str(sub.app_id),
                "renewal": "true",
            },
            app_id=str(sub.app_id),
            expire_minutes=self.settings.renewal_payment_expire_minutes,
        )

        checkout_url = payment_result.payload.get("checkout_url", "")
        if not checkout_url:
            log.warning("renewal_reminder_missing_checkout_url", provider_txn_id=payment_result.provider_txn_id)

        new_payment = Payment(
            id=new_payment_id,
            app_id=sub.app_id,
            merchant_order_no=new_merchant_order_no,
            provider=sub.provider,
            amount=old_payment.amount,
            currency=old_payment.currency,
            status=PaymentStatus.pending,
            provider_txn_id=payment_result.provider_txn_id,
            subscription_id=sub.id,
        )
        self.session.add(new_payment)

        # 记录旧 provider_txn_id 以便延迟回调路由（深拷贝确保 SQLAlchemy 检测到变化）
        if old_payment.provider_txn_id:
            meta = copy.deepcopy(sub.meta) if sub.meta else {}
            internal = meta.get("_internal", {})
            old_txn_ids = internal.get("old_renewal_txn_ids", [])
            if old_payment.provider_txn_id not in old_txn_ids:
                old_txn_ids.append(old_payment.provider_txn_id)
            internal["old_renewal_txn_ids"] = old_txn_ids[-10:]
            meta["_internal"] = internal
            sub.meta = meta

        old_payment.status = PaymentStatus.canceled
        sub.renewal_payment_id = new_payment_id
        sub.renewal_attempts = next_attempt
        sub.last_renewal_notified_at = datetime.now(UTC)

        try:
            await self.session.flush()
        except Exception:
            try:
                await adapter.cancel_payment(
                    merchant_order_no=new_merchant_order_no,
                    provider_txn_id=payment_result.provider_txn_id,
                )
            except Exception as cancel_err:
                log.warning(
                    "compensate_cancel_renewal_reminder_failed",
                    merchant_order_no=new_merchant_order_no,
                    error=str(cancel_err),
                )
            raise

        # Best-effort expire of the old provider session after the replacement is flushed.
        if old_payment.provider_txn_id:
            try:
                await adapter.cancel_payment(
                    merchant_order_no=old_payment.merchant_order_no,
                    provider_txn_id=old_payment.provider_txn_id,
                )
            except Exception as cancel_err:
                log.warning(
                    "cancel_old_renewal_session_failed",
                    payment_id=str(old_payment.id),
                    provider_txn_id=old_payment.provider_txn_id,
                    error=str(cancel_err),
                )

        await self._create_renewal_webhook(
            sub,
            "renewal_reminder",
            checkout_url=checkout_url,
            payment_id=str(new_payment_id),
            attempt_number=sub.renewal_attempts,
        )

        log.info(
            "renewal_reminder_sent",
            attempt=sub.renewal_attempts,
            new_payment_id=str(new_payment_id),
        )

    async def _create_renewal_webhook(
        self,
        sub: Subscription,
        outcome: str,
        **extra_payload,
    ):
        """创建续费相关的 WebhookDelivery 记录"""
        notify_url = sub.notify_url
        if not notify_url:
            app = await self.session.get(App, sub.app_id)
            notify_url = app.notify_url if app else None

        if not notify_url:
            return

        external_user_id = None
        stmt = select(Customer.external_user_id).where(
            Customer.id == sub.customer_id
        )
        result = await self.session.execute(stmt)
        external_user_id = result.scalar_one_or_none()

        period_ts = int(sub.current_period_end.timestamp()) if sub.current_period_end else 0
        if outcome == "renewal_reminder":
            event_id = f"{sub.id}_{outcome}_{period_ts}_a{sub.renewal_attempts}"
        else:
            event_id = f"{sub.id}_{outcome}_{period_ts}"
        event_type = f"subscription.{outcome}"

        normalized_extra = {
            k: v.isoformat() if isinstance(v, datetime) else v
            for k, v in extra_payload.items()
        }

        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "subscription_id": str(sub.id),
            "external_user_id": external_user_id,
            "plan_id": str(sub.plan_id),
            "status": sub.status,
            "amount": sub.amount,
            "currency": sub.currency.value,
            "payment_method": sub.payment_method,
            "current_period_start": (
                sub.current_period_start.isoformat()
                if sub.current_period_start
                else None
            ),
            "current_period_end": (
                sub.current_period_end.isoformat()
                if sub.current_period_end
                else None
            ),
            "cancel_at_period_end": sub.cancel_at_period_end,
            **normalized_extra,
        }

        existing_stmt = select(WebhookDelivery).where(
            WebhookDelivery.app_id == sub.app_id,
            WebhookDelivery.event_id == event_id,
        )
        existing = (await self.session.execute(existing_stmt)).scalar_one_or_none()
        if existing:
            if existing.status in (DeliveryStatus.succeeded, DeliveryStatus.dead):
                return
            existing.payload = payload
            existing.notify_url = notify_url
            existing.status = DeliveryStatus.pending
            existing.attempt_count = 0
            existing.next_attempt_at = datetime.now(UTC)
            return

        delivery = WebhookDelivery(
            id=uuid.uuid4(),
            app_id=sub.app_id,
            event_id=event_id,
            event_type=event_type,
            notify_url=notify_url,
            payload=payload,
            status=DeliveryStatus.pending,
            next_attempt_at=datetime.now(UTC),
            source_type="subscription",
            source_id=sub.id,
        )
        self.session.add(delivery)
