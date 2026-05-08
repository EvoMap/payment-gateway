"""
Callback 处理服务（重构：按 event_category 路由，source_type + source_id 通用模型）
"""

import uuid
from datetime import datetime, timedelta, UTC

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB, insert as pg_insert

from gateway.core.billing import calculate_period_end
from gateway.core.constants import (
    APP_MANAGED_METHODS,
    CallbackStatus,
    PaymentStatus,
    DeliveryStatus,
    RefundStatus,
    EventCategory,
    Currency,
    SubscriptionStatus,
)
from gateway.core.models import (
    App,
    Callback,
    Payment,
    WebhookDelivery,
    Refund,
    Subscription,
    Plan,
    Customer,
)
from gateway.core.schemas import CallbackEvent
from gateway.core.settings import get_settings
from gateway.providers import get_adapter

logger = structlog.get_logger(__name__)


def _get_item_period(event_data: dict) -> tuple[int | None, int | None]:
    """从 webhook event_data 的 items.data[0] 提取 current_period_start/end。

    新版 Stripe API 已将这两个字段从 Subscription 顶层移至订阅项级别。
    """
    items = event_data.get("items", {}).get("data", [])
    if items:
        return (
            items[0].get("current_period_start"),
            items[0].get("current_period_end"),
        )
    return None, None


class CallbackService:
    """回调处理服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def process_callback(self, event: CallbackEvent):
        log = logger.bind(
            provider_event_id=event.provider_event_id,
            outcome=event.outcome,
            event_category=(
                event.event_category.value if event.event_category else None
            ),
        )
        log.info("开始处理回调")

        callback = await self._upsert_callback(event)
        if callback.status == CallbackStatus.processed:
            log.info("回调已处理，跳过重复事件")
            await self.session.commit()
            return

        match event.event_category:
            case EventCategory.payment | None:
                payment = await self._find_payment(event)
                if not payment:
                    log.warning("未找到回调对应的支付")
                    callback.status = CallbackStatus.failed
                    await self.session.commit()
                    return
                event.app_id = payment.app_id
                callback.source_type = "payment"
                callback.source_id = payment.id
                await self._process_payment_callback(payment, event, callback)

            case EventCategory.refund:
                payment = await self._find_payment(event)
                if not payment:
                    log.warning("未找到回调对应的支付")
                    callback.status = CallbackStatus.failed
                    await self.session.commit()
                    return
                event.app_id = payment.app_id
                callback.source_type = "refund"
                await self._process_refund_callback(payment, event, callback)

            case EventCategory.subscription | EventCategory.invoice:
                subscription = await self._find_subscription(event)
                if not subscription:
                    log.warning("未找到回调对应的订阅")
                    callback.status = CallbackStatus.failed
                    await self.session.commit()
                    return
                event.app_id = subscription.app_id
                callback.source_type = "subscription"
                callback.source_id = subscription.id
                try:
                    await self._process_subscription_callback(
                        subscription, event, callback
                    )
                except Exception as e:
                    log.error(
                        "订阅回调处理异常，需人工介入",
                        error=str(e),
                        exc_info=True,
                    )
                    callback.status = CallbackStatus.failed
                    callback.processed_at = datetime.now(UTC)
                    await self.session.commit()

    # ==================== 支付回调 ====================

    async def _process_payment_callback(
        self, payment: Payment, event: CallbackEvent, callback: Callback
    ):
        log = logger.bind(payment_id=str(payment.id))
        await self.session.refresh(payment, with_for_update=True)

        old_status = payment.status
        new_status = self._map_outcome_to_status(event.outcome)

        payment_terminal = {
            PaymentStatus.succeeded,
            PaymentStatus.failed,
            PaymentStatus.canceled,
        }

        if not new_status:
            log.warning(
                "支付回调outcome无法映射到已知状态",
                outcome=event.outcome,
            )
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return
        elif old_status in payment_terminal:
            log.warning(
                "支付已处于终态，忽略状态变更",
                current_status=old_status.value,
                incoming_status=new_status.value,
            )
            # 补偿：app-managed 的 Checkout Session 可能在本地标记取消后仍成功支付。
            # 以 provider 成功回调为准纠正本地状态，再尝试激活/续期。
            if (
                new_status == PaymentStatus.succeeded
                and payment.subscription_id
                and payment.merchant_order_no
                and (
                    payment.merchant_order_no.startswith("sub_init_")
                    or payment.merchant_order_no.startswith("sub_renew_")
                )
            ):
                if old_status != PaymentStatus.succeeded:
                    payment.status = PaymentStatus.succeeded
                    if event.provider_txn_id and not payment.provider_txn_id:
                        payment.provider_txn_id = event.provider_txn_id
                    if not payment.paid_at:
                        payment.paid_at = datetime.now(UTC)
                    await self._create_payment_webhook_delivery(
                        payment, PaymentStatus.succeeded
                    )

                    sub_stmt = select(Subscription).where(
                        Subscription.id == payment.subscription_id
                    )
                    sub_result = await self.session.execute(sub_stmt)
                    sub_obj = sub_result.scalar_one_or_none()
                    if sub_obj and sub_obj.is_app_managed:
                        await self._handle_app_managed_subscription_payment(payment, log)
        elif new_status != old_status:
            payment.status = new_status

            if event.provider_txn_id and not payment.provider_txn_id:
                payment.provider_txn_id = event.provider_txn_id

            if new_status == PaymentStatus.succeeded and not payment.paid_at:
                payment.paid_at = datetime.now(UTC)

            log.info(
                "支付回调推进支付状态",
                old_status=old_status.value,
                new_status=new_status.value,
            )

            if new_status in payment_terminal:
                await self._create_payment_webhook_delivery(payment, new_status)

            # App-managed 订阅：支付成功时处理订阅激活/续期
            if (
                new_status == PaymentStatus.succeeded
                and payment.subscription_id
            ):
                sub_check_stmt = select(Subscription.payment_method).where(
                    Subscription.id == payment.subscription_id
                )
                sub_check_result = await self.session.execute(sub_check_stmt)
                pm = sub_check_result.scalar_one_or_none()
                if pm and pm in APP_MANAGED_METHODS:
                    await self._handle_app_managed_subscription_payment(payment, log)

        callback.status = CallbackStatus.processed
        callback.processed_at = datetime.now(UTC)
        await self.session.commit()

    # ==================== App-Managed 订阅支付处理 ====================

    async def _handle_app_managed_subscription_payment(self, payment: Payment, log):
        """App-managed 订阅支付成功后：激活（首次）或续期（续费）"""
        stmt = (
            select(Subscription)
            .where(Subscription.id == payment.subscription_id)
            .with_for_update()
        )
        result = await self.session.execute(stmt)
        subscription = result.scalar_one_or_none()
        if not subscription or not subscription.is_app_managed:
            return

        if subscription.status == SubscriptionStatus.canceled.value:
            refunded, refund = await self._refund_app_managed_payment_once(
                subscription,
                payment,
                log,
                reason="subscription already canceled before payment completed",
            )
            if refunded:
                await self._create_subscription_webhook_delivery(
                    subscription,
                    "payment_after_canceled_refund_pending",
                    f"pay_{payment.id}_after_canceled_refund_pending",
                )
                return
            log.warning(
                "app_managed_payment_after_canceled",
                subscription_id=str(subscription.id),
                payment_id=str(payment.id),
                refund_id=str(refund.id) if refund else None,
            )
            await self._create_subscription_webhook_delivery(
                subscription,
                "payment_after_canceled",
                f"pay_{payment.id}_after_canceled",
            )
            return

        if subscription.status == SubscriptionStatus.incomplete.value:
            # 首次支付成功 → 激活订阅
            now = datetime.now(UTC)
            if subscription.trial_end and subscription.trial_end > now:
                subscription.status = SubscriptionStatus.trialing.value
                subscription.trial_start = subscription.created_at or now
                subscription.current_period_start = now
                subscription.current_period_end = subscription.trial_end
            else:
                subscription.status = SubscriptionStatus.active.value
                subscription.current_period_start = now
                subscription.current_period_end = await self._calculate_period_end(
                    now, subscription
                )
            subscription.last_event_at = now
            log.info(
                "app_managed_subscription_activated",
                subscription_id=str(subscription.id),
            )
            await self._create_subscription_webhook_delivery(
                subscription, "activated", f"pay_{payment.id}_activated"
            )

        elif (
            subscription.renewal_payment_id == payment.id
            or (
                payment.merchant_order_no
                and payment.merchant_order_no.startswith(f"sub_renew_{subscription.id}")
                and self._renewal_order_matches_current_period(
                    payment.merchant_order_no, subscription
                )
            )
        ):
            # 幂等性：如果该周期已被推进过，退款多余支付
            old_period_end = subscription.current_period_end
            if (
                old_period_end
                and subscription.last_renewed_period_end
                and subscription.last_renewed_period_end >= old_period_end
            ):
                log.warning(
                    "renewal_already_advanced_refunding_duplicate",
                    subscription_id=str(subscription.id),
                    payment_id=str(payment.id),
                    last_renewed_period_end=str(subscription.last_renewed_period_end),
                    current_period_end=str(old_period_end),
                )
                await self._refund_app_managed_payment_once(
                    subscription,
                    payment,
                    log,
                    reason="duplicate renewal payment after period already advanced",
                )
                await self._create_subscription_webhook_delivery(
                    subscription,
                    "duplicate_renewal_refund_pending",
                    f"pay_{payment.id}_dup_refund",
                )
                return

            # 续费支付成功 → 推进周期；若已标记周期末取消，则优先退款且不延长权益。
            if subscription.cancel_at_period_end:
                refund_initiated, refund = await self._refund_app_managed_payment_once(
                    subscription,
                    payment,
                    log,
                    reason="subscription canceled before renewal period started",
                )

                now_ts = datetime.now(UTC)
                subscription.renewal_payment_id = None
                subscription.renewal_attempts = 0
                subscription.last_renewal_notified_at = None
                subscription.last_event_at = now_ts

                subscription.grace_period_end = None

                if refund_initiated:
                    log.info(
                        "app_managed_renewal_refund_pending_keep_cancellation",
                        subscription_id=str(subscription.id),
                        payment_id=str(payment.id),
                        refund_id=str(refund.id) if refund else None,
                    )
                    await self._create_subscription_webhook_delivery(
                        subscription,
                        "renewal_refund_pending",
                        f"pay_{payment.id}_refund_pending",
                    )
                    return
                else:
                    # 退款未能发起时，用户已付款，推进周期并取消周期末终止以保障已付费权益。
                    old_period_end = subscription.current_period_end or now_ts
                    subscription.last_renewed_period_end = old_period_end
                    subscription.current_period_start = old_period_end
                    subscription.current_period_end = await self._calculate_period_end(
                        old_period_end, subscription
                    )
                    subscription.status = SubscriptionStatus.active.value
                    subscription.cancel_at_period_end = False
                    log.error(
                        "app_managed_renewal_refund_failed_keep_paid_period",
                        subscription_id=str(subscription.id),
                        payment_id=str(payment.id),
                        amount=payment.amount,
                    )

                log.warning(
                    "app_managed_renewal_paid_but_cancel_at_period_end",
                    subscription_id=str(subscription.id),
                    payment_id=str(payment.id),
                    amount=payment.amount,
                    currency=payment.currency.value if payment.currency else None,
                    refund_initiated=refund_initiated,
                )
                await self._create_subscription_webhook_delivery(
                    subscription,
                    "renewal_paid_needs_refund",
                    f"pay_{payment.id}_needs_refund",
                )
                return
            now = datetime.now(UTC)
            old_period_end = subscription.current_period_end or now

            # 如果有 pending plan change，使用新计划
            if subscription.pending_plan_id:
                new_plan = await self.session.get(Plan, subscription.pending_plan_id)
                if new_plan:
                    subscription.plan_id = new_plan.id
                    subscription.amount = new_plan.amount
                    subscription.currency = new_plan.currency
                    subscription.provider_price_id = new_plan.provider_price_id
                subscription.pending_plan_id = None
                subscription.pending_plan_change_at = None

            subscription.last_renewed_period_end = old_period_end
            subscription.current_period_start = old_period_end
            subscription.current_period_end = await self._calculate_period_end(
                old_period_end, subscription
            )
            subscription.status = SubscriptionStatus.active.value
            subscription.grace_period_end = None
            subscription.last_event_at = now

            # 取消冗余的 pending renewal payment（防止双重扣费）
            if (
                subscription.renewal_payment_id
                and subscription.renewal_payment_id != payment.id
            ):
                stale_payment = await self.session.get(
                    Payment, subscription.renewal_payment_id
                )
                if stale_payment and stale_payment.status == PaymentStatus.pending:
                    if stale_payment.provider_txn_id:
                        try:
                            adapter = get_adapter(subscription.provider)
                            await adapter.cancel_payment(
                                merchant_order_no=stale_payment.merchant_order_no,
                                provider_txn_id=stale_payment.provider_txn_id,
                            )
                        except Exception as cancel_err:
                            log.warning(
                                "cancel_stale_renewal_after_advance_failed",
                                stale_payment_id=str(stale_payment.id),
                                error=str(cancel_err),
                            )
                    stale_payment.status = PaymentStatus.canceled

            subscription.renewal_payment_id = None
            subscription.renewal_attempts = 0
            subscription.last_renewal_notified_at = None
            log.info(
                "app_managed_subscription_renewed",
                subscription_id=str(subscription.id),
                new_period_end=str(subscription.current_period_end),
            )
            await self._create_subscription_webhook_delivery(
                subscription, "renewed", f"pay_{payment.id}_renewed"
            )
        else:
            # 双重支付场景：payment succeeded 但无法匹配当前周期，主动退款
            if payment.status == PaymentStatus.succeeded and subscription.last_renewed_period_end:
                log.warning(
                    "app_managed_payment_unmatched_attempting_refund",
                    subscription_id=str(subscription.id),
                    payment_id=str(payment.id),
                    subscription_status=subscription.status,
                )
                await self._refund_app_managed_payment_once(
                    subscription,
                    payment,
                    log,
                    reason="duplicate renewal payment after period already advanced",
                )
                await self._create_subscription_webhook_delivery(
                    subscription,
                    "duplicate_payment_refund_pending",
                    f"pay_{payment.id}_duplicate_refund",
                )
            else:
                log.warning(
                    "app_managed_payment_unmatched",
                    subscription_id=str(subscription.id),
                    payment_id=str(payment.id),
                    subscription_status=subscription.status,
                )

    async def _refund_app_managed_payment_once(
        self,
        subscription: Subscription,
        payment: Payment,
        log,
        *,
        reason: str,
    ) -> tuple[bool, Refund | None]:
        existing_stmt = (
            select(Refund)
            .where(Refund.payment_id == payment.id)
            .order_by(Refund.created_at.desc())
            .limit(1)
        )
        existing = (await self.session.execute(existing_stmt)).scalar_one_or_none()
        if existing:
            log.info(
                "app_managed_auto_refund_already_exists",
                subscription_id=str(subscription.id),
                payment_id=str(payment.id),
                refund_id=str(existing.id),
            )
            return True, existing

        if not payment.provider_txn_id:
            log.warning(
                "app_managed_auto_refund_impossible_no_txn_id",
                subscription_id=str(subscription.id),
                payment_id=str(payment.id),
            )
            return False, None

        try:
            adapter = get_adapter(subscription.provider)
            refund_result = await adapter.create_refund(
                txn_id=payment.provider_txn_id,
                merchant_order_no=payment.merchant_order_no,
                refund_amount=payment.amount,
                reason=reason,
            )
            provider_refund_id = None
            if isinstance(refund_result, dict):
                provider_refund_id = (
                    refund_result.get("provider_refund_id")
                    or refund_result.get("refund_id")
                )
            refund = Refund(
                id=uuid.uuid4(),
                payment_id=payment.id,
                refund_amount=payment.amount,
                reason=reason,
                status=RefundStatus.pending,
                provider=subscription.provider,
                provider_refund_id=provider_refund_id,
                notify_url=subscription.notify_url,
            )
            self.session.add(refund)
            log.info(
                "app_managed_auto_refund_initiated",
                subscription_id=str(subscription.id),
                payment_id=str(payment.id),
                refund_id=str(refund.id),
                amount=payment.amount,
            )
            return True, refund
        except Exception as e:
            log.error(
                "app_managed_auto_refund_failed",
                subscription_id=str(subscription.id),
                payment_id=str(payment.id),
                error=str(e),
                exc_info=True,
            )
            return False, None

    async def _calculate_period_end(
        self, start: "datetime", subscription: Subscription
    ) -> "datetime":
        """根据订阅关联 plan 的 interval 计算下一个 period_end"""
        plan = await self.session.get(Plan, subscription.plan_id)
        if plan:
            return calculate_period_end(start, plan.interval, plan.interval_count)
        logger.error(
            "subscription_plan_not_found_using_monthly_fallback",
            subscription_id=str(subscription.id),
            plan_id=str(subscription.plan_id),
        )
        return calculate_period_end(start, "month", 1)

    @staticmethod
    def _renewal_order_matches_current_period(
        merchant_order_no: str, subscription: Subscription
    ) -> bool:
        """验证 renewal payment 的 merchant_order_no 中嵌入的 period_end 时间戳
        与订阅当前 current_period_end 匹配，防止旧周期延迟回调错误推进。"""
        if not subscription.current_period_end:
            return False
        current_ts = str(int(subscription.current_period_end.timestamp()))
        prefix = f"sub_renew_{subscription.id}_"
        suffix = merchant_order_no.removeprefix(prefix) if merchant_order_no.startswith(prefix) else ""
        # suffix 格式: "{ts}" 或 "{ts}_r{n}"
        ts_part = suffix.split("_")[0] if suffix else ""
        return ts_part == current_ts

    # ==================== 退款回调 ====================

    async def _process_refund_callback(
        self, payment: Payment, event: CallbackEvent, callback: Callback
    ):
        log = logger.bind(payment_id=str(payment.id))
        refund_obj = event.raw_payload.get("data", {}).get("object", {})
        provider_refund_id = refund_obj.get("id")

        if not provider_refund_id:
            log.warning("退款回调缺少退款ID")
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return

        provider = event.provider

        stmt = select(Refund).where(Refund.provider_refund_id == provider_refund_id)
        if provider:
            stmt = stmt.where(Refund.provider == provider)

        result = await self.session.execute(stmt)
        refund = result.scalar_one_or_none()

        if not refund:
            log.warning(
                "未找到回调对应的退款", provider_refund_id=provider_refund_id
            )
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return

        callback.source_id = refund.id

        await self.session.refresh(refund, with_for_update=True)

        if payment.id != refund.payment_id:
            log.warning(
                "退款回调关联的Payment与退款记录不一致",
                original_payment_id=str(payment.id),
                refund_payment_id=str(refund.payment_id),
            )
            stmt = select(Payment).where(Payment.id == refund.payment_id)
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                await self.session.refresh(payment, with_for_update=True)
            if not payment:
                callback.status = CallbackStatus.failed
                await self.session.commit()
                return

        outcome_map = {
            "refund_succeeded": RefundStatus.succeeded,
            "refund_failed": RefundStatus.failed,
            "refund_pending": RefundStatus.pending,
            "refund_canceled": RefundStatus.canceled,
        }
        new_status = outcome_map.get(event.outcome)

        refund_terminal = {
            RefundStatus.succeeded,
            RefundStatus.failed,
            RefundStatus.canceled,
        }

        if not new_status:
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return
        elif refund.status in refund_terminal:
            log.warning(
                "退款已处于终态，忽略状态变更",
                refund_id=str(refund.id),
                current_status=refund.status.value,
            )
        elif new_status != refund.status:
            refund.status = new_status
            if new_status == RefundStatus.succeeded and not refund.refunded_at:
                refund.refunded_at = datetime.now(UTC)

            if new_status in refund_terminal:
                await self._create_refund_webhook_delivery(
                    payment, refund, new_status
                )

        if not refund.provider_refund_id:
            refund.provider_refund_id = provider_refund_id

        callback.status = CallbackStatus.processed
        callback.processed_at = datetime.now(UTC)
        await self.session.commit()

    # ==================== 订阅回调 ====================

    async def _process_subscription_callback(
        self,
        subscription: Subscription,
        event: CallbackEvent,
        callback: Callback,
    ):
        log = logger.bind(
            subscription_id=str(subscription.id), outcome=event.outcome
        )
        await self.session.refresh(subscription, with_for_update=True)

        event_data = event.raw_payload.get("data", {}).get("object", {})
        event_created = event.raw_payload.get("created")

        if subscription.last_event_at and event_created:
            event_time = datetime.fromtimestamp(event_created, tz=UTC)
            if event_time < subscription.last_event_at:
                log.debug("事件时间早于已处理事件，跳过")
                callback.status = CallbackStatus.processed
                callback.processed_at = datetime.now(UTC)
                await self.session.commit()
                return

        match event.outcome:
            case "subscription_activated":
                provider_sub_id = (
                    event_data.get("subscription") or event.subscription_id
                )
                if subscription.status == SubscriptionStatus.incomplete.value:
                    subscription.provider_subscription_id = provider_sub_id
                    if (
                        subscription.trial_end
                        and subscription.trial_end > datetime.now(UTC)
                    ):
                        subscription.status = SubscriptionStatus.trialing.value
                    else:
                        subscription.status = SubscriptionStatus.active.value

            case "subscription_created":
                provider_sub_id = (
                    event_data.get("id") or event.subscription_id
                )
                if provider_sub_id:
                    subscription.provider_subscription_id = provider_sub_id
                status_str = event_data.get("status")
                if status_str:
                    try:
                        subscription.status = SubscriptionStatus(status_str).value
                    except ValueError:
                        log.warning("未知的订阅状态", unknown_status=status_str)
                        subscription.meta = {
                            **(subscription.meta or {}),
                            "_unknown_status": status_str,
                            "_unknown_status_at": datetime.now(UTC).isoformat(),
                        }
                if event_data.get("trial_start"):
                    subscription.trial_start = datetime.fromtimestamp(
                        event_data["trial_start"], tz=UTC
                    )
                if event_data.get("trial_end"):
                    subscription.trial_end = datetime.fromtimestamp(
                        event_data["trial_end"], tz=UTC
                    )
                period_start, period_end = _get_item_period(event_data)
                if period_start:
                    subscription.current_period_start = datetime.fromtimestamp(
                        period_start, tz=UTC
                    )
                if period_end:
                    subscription.current_period_end = datetime.fromtimestamp(
                        period_end, tz=UTC
                    )

            case "subscription_pending":
                provider_sub_id = (
                    event_data.get("subscription") or event.subscription_id
                )
                if (
                    subscription.status == SubscriptionStatus.incomplete.value
                    and provider_sub_id
                ):
                    subscription.provider_subscription_id = provider_sub_id

            case "subscription_updated":
                status_str = event_data.get("status")
                if status_str:
                    if (
                        subscription.status == SubscriptionStatus.paused.value
                        and status_str == "active"
                    ):
                        pass
                    else:
                        try:
                            subscription.status = SubscriptionStatus(
                                status_str
                            ).value
                        except ValueError:
                            log.warning(
                                "未知的订阅状态", unknown_status=status_str
                            )
                            subscription.meta = {
                                **(subscription.meta or {}),
                                "_unknown_status": status_str,
                                "_unknown_status_at": datetime.now(
                                    UTC
                                ).isoformat(),
                            }
                period_start, period_end = _get_item_period(event_data)
                if period_start:
                    subscription.current_period_start = datetime.fromtimestamp(
                        period_start, tz=UTC
                    )
                if period_end:
                    subscription.current_period_end = datetime.fromtimestamp(
                        period_end, tz=UTC
                    )
                subscription.cancel_at_period_end = event_data.get(
                    "cancel_at_period_end", False
                )
                items = event_data.get("items", {}).get("data", [])
                if items:
                    new_price_id = items[0].get("price", {}).get("id")
                    if (
                        new_price_id
                        and new_price_id != subscription.provider_price_id
                    ):
                        subscription.provider_price_id = new_price_id
                        new_product_id = items[0].get("price", {}).get(
                            "product"
                        )
                        new_plan = None
                        if new_product_id:
                            plan_stmt = (
                                select(Plan)
                                .where(
                                    Plan.provider_product_id == new_product_id,
                                    Plan.app_id == subscription.app_id,
                                )
                                .limit(1)
                            )
                            plan_result = await self.session.execute(plan_stmt)
                            new_plan = plan_result.scalar_one_or_none()
                        if not new_plan:
                            plan_stmt = (
                                select(Plan)
                                .where(
                                    Plan.provider_price_id == new_price_id,
                                    Plan.app_id == subscription.app_id,
                                )
                                .limit(1)
                            )
                            plan_result = await self.session.execute(plan_stmt)
                            new_plan = plan_result.scalar_one_or_none()
                        if new_plan:
                            subscription.plan_id = new_plan.id
                            if (
                                subscription.pending_plan_id
                                and subscription.pending_plan_id == new_plan.id
                            ):
                                log.info(
                                    "待生效降级已执行，清除 pending 状态",
                                    new_plan_id=str(new_plan.id),
                                )
                                subscription.pending_plan_id = None
                                subscription.pending_plan_change_at = None
                                subscription.provider_schedule_id = None
                        else:
                            log.warning(
                                "Plan 反查失败，仅更新 provider_price_id",
                                new_price_id=new_price_id,
                            )

            case "subscription_canceled":
                subscription.status = SubscriptionStatus.canceled.value
                canceled_ts = event_data.get("canceled_at")
                if canceled_ts:
                    subscription.canceled_at = datetime.fromtimestamp(
                        canceled_ts, tz=UTC
                    )
                ended_ts = (
                    event_data.get("ended_at")
                    or event_data.get("canceled_at")
                    or event_created
                )
                if ended_ts:
                    subscription.ended_at = datetime.fromtimestamp(
                        ended_ts, tz=UTC
                    )
                else:
                    subscription.ended_at = datetime.now(UTC)

            case "subscription_expired":
                if subscription.status == SubscriptionStatus.incomplete.value:
                    subscription.status = (
                        SubscriptionStatus.incomplete_expired.value
                    )

            case "subscription_payment_failed":
                if subscription.status == SubscriptionStatus.incomplete.value:
                    subscription.status = (
                        SubscriptionStatus.incomplete_expired.value
                    )

            case "invoice_paid":
                if subscription.status in (
                    SubscriptionStatus.past_due.value,
                    SubscriptionStatus.unpaid.value,
                ):
                    subscription.status = SubscriptionStatus.active.value

                lines = event_data.get("lines", {}).get("data", [])
                sub_line = next(
                    (ln for ln in lines if ln.get("type") == "subscription"),
                    None,
                )
                target_line = sub_line or (lines[0] if lines else None)
                if target_line:
                    period = target_line.get("period", {})
                    if period.get("start"):
                        subscription.current_period_start = (
                            datetime.fromtimestamp(period["start"], tz=UTC)
                        )
                    if period.get("end"):
                        subscription.current_period_end = (
                            datetime.fromtimestamp(period["end"], tz=UTC)
                        )

                invoice_id = event_data.get("id")
                payment_intent_id = event_data.get("payment_intent")
                invoice_amount = event_data.get("amount_paid", 0)
                invoice_currency = event_data.get("currency", "").upper()
                if invoice_id and invoice_amount > 0:
                    merchant_order_no = f"sub_inv_{invoice_id}"
                    existing_stmt = select(Payment).where(
                        Payment.app_id == subscription.app_id,
                        Payment.merchant_order_no == merchant_order_no,
                    )
                    existing = (
                        await self.session.execute(existing_stmt)
                    ).scalar_one_or_none()
                    if not existing:
                        try:
                            currency_enum = Currency(invoice_currency)
                        except ValueError:
                            log.error(
                                "Invoice 币种不在支持列表，跳过 Payment 创建",
                                invoice_currency=invoice_currency,
                            )
                            currency_enum = None
                        if currency_enum:
                            paid_ts = (
                                event_data.get("status_transitions", {}).get(
                                    "paid_at"
                                )
                                or event_created
                            )
                            paid_at = (
                                datetime.fromtimestamp(paid_ts, tz=UTC)
                                if paid_ts
                                else datetime.now(UTC)
                            )
                            payment = Payment(
                                id=uuid.uuid4(),
                                app_id=subscription.app_id,
                                merchant_order_no=merchant_order_no,
                                provider=subscription.provider,
                                amount=invoice_amount,
                                currency=currency_enum,
                                status=PaymentStatus.succeeded,
                                provider_txn_id=payment_intent_id,
                                subscription_id=subscription.id,
                                paid_at=paid_at,
                            )
                            self.session.add(payment)
                            await self.session.flush()

            case "invoice_payment_failed":
                if subscription.status in (
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.trialing.value,
                ):
                    subscription.status = SubscriptionStatus.past_due.value

            case "subscription_paused":
                subscription.status = SubscriptionStatus.paused.value

            case "subscription_resumed":
                subscription.status = SubscriptionStatus.active.value

            case "subscription_trial_will_end":
                pass

            case "invoice_action_required":
                pass

        if event_created:
            subscription.last_event_at = datetime.fromtimestamp(
                event_created, tz=UTC
            )

        callback.status = CallbackStatus.processed
        callback.processed_at = datetime.now(UTC)

        await self._create_subscription_webhook_delivery(
            subscription, event.outcome, event.provider_event_id
        )
        await self.session.commit()

    # ==================== 辅助方法 ====================

    async def _upsert_callback(self, event: CallbackEvent) -> Callback:
        callback_id = uuid.uuid4()
        provider_val = event.provider

        stmt = (
            pg_insert(Callback)
            .values(
                id=callback_id,
                provider=provider_val,
                provider_event_id=event.provider_event_id,
                provider_txn_id=event.provider_txn_id,
                source_type=None,
                source_id=None,
                payload=event.raw_payload,
                status=CallbackStatus.processing,
            )
            .on_conflict_do_nothing(
                constraint="uq_callbacks_provider_provider_event_id"
            )
        )

        result = await self.session.execute(stmt)

        if result.rowcount > 0:
            await self.session.flush()
            fetch = select(Callback).where(Callback.id == callback_id)
            row = await self.session.execute(fetch)
            return row.scalar_one()
        else:
            fetch = select(Callback).where(
                Callback.provider == provider_val,
                Callback.provider_event_id == event.provider_event_id,
            )
            row = await self.session.execute(fetch)
            return row.scalar_one()

    async def _find_payment(self, event: CallbackEvent) -> Payment | None:
        """定位 Payment（优先 app_id + merchant_order_no 精确匹配，回退 provider_txn_id）"""
        if event.merchant_order_no and event.app_id:
            stmt = select(Payment).where(
                Payment.app_id == event.app_id,
                Payment.merchant_order_no == event.merchant_order_no,
            )
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                return payment

        if event.provider_txn_id:
            stmt = select(Payment).where(
                Payment.provider_txn_id == event.provider_txn_id,
            )
            if event.app_id:
                stmt = stmt.where(Payment.app_id == event.app_id)
            stmt = stmt.order_by(Payment.created_at.desc()).limit(1)
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                return payment

            # 回退：在 app-managed 订阅的 _internal.old_renewal_txn_ids 中查找旧 session ID
            # 依赖 SQLAlchemy 2.0+ 的 JSONB 链式下标语法；meta 为 NULL 时路径返回 NULL 不匹配
            # 仅在 app_id 已知时执行，防止跨租户误匹配
            if not event.app_id:
                return None
            _settings = get_settings()
            txn_id_json = func.cast(
                func.jsonb_build_array(event.provider_txn_id), PG_JSONB
            )
            lookback = datetime.now(UTC) - timedelta(
                minutes=_settings.renewal_payment_expire_minutes * 2
            )
            fallback_filters = [
                Subscription.app_id == event.app_id,
                Subscription.payment_method.in_(APP_MANAGED_METHODS),
                Subscription.last_renewal_notified_at >= lookback,
                Subscription.meta["_internal"]["old_renewal_txn_ids"]
                .cast(PG_JSONB)
                .contains(txn_id_json),
            ]
            stmt = (
                select(Subscription)
                .where(*fallback_filters)
                .order_by(Subscription.last_renewal_notified_at.desc())
                .limit(1)
            )
            result = await self.session.execute(stmt)
            sub = result.scalar_one_or_none()
            if sub:
                # 找到与旧 provider_txn_id 对应的原始 Payment（已被标记为 canceled）
                orig_stmt = (
                    select(Payment)
                    .where(
                        Payment.subscription_id == sub.id,
                        Payment.provider_txn_id == event.provider_txn_id,
                    )
                    .limit(1)
                )
                orig_result = await self.session.execute(orig_stmt)
                orig_payment = orig_result.scalar_one_or_none()
                if orig_payment:
                    return orig_payment
                logger.warning(
                    "old_renewal_txn_id_matched_but_payment_not_found",
                    provider_txn_id=event.provider_txn_id,
                    subscription_id=str(sub.id),
                )

        return None

    async def _find_subscription(
        self, event: CallbackEvent
    ) -> Subscription | None:
        app_id = event.app_id

        sub_id_candidates = set()
        if event.subscription_id:
            sub_id_candidates.add(event.subscription_id)
        if (
            event.provider_txn_id
            and event.provider_txn_id != event.subscription_id
        ):
            sub_id_candidates.add(event.provider_txn_id)

        if sub_id_candidates:
            stmt = select(Subscription).where(
                Subscription.provider_subscription_id.in_(sub_id_candidates)
            )
            if app_id:
                stmt = stmt.where(Subscription.app_id == app_id)
            stmt = stmt.order_by(Subscription.created_at.desc()).limit(1)
            result = await self.session.execute(stmt)
            subscription = result.scalar_one_or_none()
            if subscription:
                return subscription

        if event.checkout_session_id:
            stmt = select(Subscription).where(
                Subscription.provider_checkout_session_id
                == event.checkout_session_id
            )
            if app_id:
                stmt = stmt.where(Subscription.app_id == app_id)
            stmt = stmt.order_by(Subscription.created_at.desc()).limit(1)
            result = await self.session.execute(stmt)
            subscription = result.scalar_one_or_none()
            if subscription:
                return subscription

        if event.gateway_subscription_id:
            stmt = select(Subscription).where(
                Subscription.id == event.gateway_subscription_id
            )
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        return None

    def _map_outcome_to_status(self, outcome: str) -> PaymentStatus | None:
        outcome_map = {
            "succeeded": PaymentStatus.succeeded,
            "failed": PaymentStatus.failed,
            "canceled": PaymentStatus.canceled,
            "expired": PaymentStatus.canceled,
            "pending": PaymentStatus.pending,
        }
        return outcome_map.get(outcome)

    # ==================== WebhookDelivery ====================

    async def _create_webhook_delivery(
        self,
        *,
        app_id: uuid.UUID,
        event_id: str,
        event_type: str,
        payload: dict,
        notify_url: str | None = None,
        source_type: str = "payment",
        source_id: uuid.UUID | None = None,
    ):
        if not notify_url:
            stmt = select(App.notify_url).where(App.id == app_id)
            result = await self.session.execute(stmt)
            notify_url = result.scalar_one_or_none()

        if not notify_url:
            logger.warning("缺少回调通知地址", app_id=str(app_id))
            return

        payload = {"event_id": event_id, "event_type": event_type, **payload}

        stmt = select(WebhookDelivery).where(
            WebhookDelivery.app_id == app_id,
            WebhookDelivery.event_id == event_id,
        )
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            if existing.status in (
                DeliveryStatus.succeeded,
                DeliveryStatus.dead,
            ):
                logger.debug(
                    "Webhook已终态，跳过",
                    event_id=event_id,
                    status=existing.status.value,
                )
                return
            existing.notify_url = notify_url
            existing.payload = payload
            existing.status = DeliveryStatus.pending
            existing.attempt_count = 0
            existing.next_attempt_at = datetime.now(UTC)
            existing.last_attempt_at = None
            existing.last_http_status = None
            existing.last_error = None
            existing.delivered_at = None
            logger.info("Webhook投递任务已重入队", event_id=event_id)
            return

        delivery = WebhookDelivery(
            id=uuid.uuid4(),
            app_id=app_id,
            source_type=source_type,
            source_id=source_id,
            event_id=event_id,
            event_type=event_type,
            notify_url=notify_url,
            payload=payload,
            status=DeliveryStatus.pending,
            attempt_count=0,
            next_attempt_at=datetime.now(UTC),
        )
        self.session.add(delivery)
        await self.session.flush()
        logger.info("Webhook投递任务已创建", event_id=event_id)

    async def _create_payment_webhook_delivery(
        self, payment: Payment, event_status: PaymentStatus
    ):
        await self._create_webhook_delivery(
            app_id=payment.app_id,
            source_type="payment",
            source_id=payment.id,
            notify_url=payment.notify_url,
            event_id=f"{payment.id}_{event_status.value}",
            event_type=f"payment.{event_status.value}",
            payload={
                "payment_id": str(payment.id),
                "merchant_order_no": payment.merchant_order_no,
                "status": payment.status.value,
                "amount": payment.amount,
                "currency": payment.currency.value,
                "provider_txn_id": payment.provider_txn_id,
                "paid_at": (
                    payment.paid_at.isoformat() if payment.paid_at else None
                ),
            },
        )

    async def _create_refund_webhook_delivery(
        self, payment: Payment, refund: Refund, event_status: RefundStatus
    ):
        await self._create_webhook_delivery(
            app_id=payment.app_id,
            source_type="refund",
            source_id=refund.id,
            notify_url=refund.notify_url or payment.notify_url,
            event_id=f"{refund.id}_{event_status.value}",
            event_type=f"refund.{event_status.value}",
            payload={
                "refund_id": str(refund.id),
                "payment_id": str(payment.id),
                "merchant_order_no": payment.merchant_order_no,
                "status": refund.status.value,
                "refund_amount": refund.refund_amount,
                "provider_refund_id": refund.provider_refund_id,
                "refunded_at": (
                    refund.refunded_at.isoformat()
                    if refund.refunded_at
                    else None
                ),
                "reason": refund.reason,
                "currency": payment.currency.value,
            },
        )

    async def _create_subscription_webhook_delivery(
        self,
        subscription: Subscription,
        outcome: str,
        provider_event_id: str,
    ):
        stmt = select(Customer.external_user_id).where(
            Customer.id == subscription.customer_id
        )
        external_user_id = (
            await self.session.execute(stmt)
        ).scalar_one_or_none()

        await self._create_webhook_delivery(
            app_id=subscription.app_id,
            source_type="subscription",
            source_id=subscription.id,
            notify_url=subscription.notify_url,
            event_id=f"{subscription.id}_{provider_event_id}",
            event_type=f"subscription.{outcome}",
            payload={
                "subscription_id": str(subscription.id),
                "external_user_id": external_user_id,
                "plan_id": str(subscription.plan_id),
                "status": subscription.status,
                "amount": subscription.amount,
                "currency": subscription.currency.value,
                "current_period_start": (
                    subscription.current_period_start.isoformat()
                    if subscription.current_period_start
                    else None
                ),
                "current_period_end": (
                    subscription.current_period_end.isoformat()
                    if subscription.current_period_end
                    else None
                ),
                "cancel_at_period_end": subscription.cancel_at_period_end,
            },
        )

    async def notify_subscription_event(
        self, subscription: Subscription, outcome: str, event_id: str
    ):
        """公开方法：供 Worker 清理任务等外部调用"""
        await self._create_subscription_webhook_delivery(
            subscription, outcome, event_id
        )
