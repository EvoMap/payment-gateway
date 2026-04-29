"""
CallbackService 订阅回调单元测试

模拟 Stripe Webhook 事件，验证 _process_subscription_callback 对
订阅状态、周期、Plan 关联、Payment 创建等各维度的处理逻辑。
"""

import uuid
from datetime import datetime, timedelta, UTC

import pytest
from sqlalchemy import select

from gateway.core.constants import (
    EventCategory,
    Provider,
    SubscriptionStatus,
)
from gateway.core.models import Payment, Subscription, WebhookDelivery
from gateway.core.schemas import CallbackEvent
from gateway.services.callbacks import CallbackService


# ── helpers ──


def _ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _make_event(
    *,
    outcome: str,
    category: EventCategory = EventCategory.subscription,
    subscription_id: str | None = None,
    checkout_session_id: str | None = None,
    gateway_subscription_id: uuid.UUID | None = None,
    event_data: dict | None = None,
    event_created: int | None = None,
    provider_event_id: str | None = None,
    invoice_id: str | None = None,
) -> CallbackEvent:
    now_ts = event_created or _ts(datetime.now(UTC))
    return CallbackEvent(
        provider=Provider.stripe,
        provider_event_id=provider_event_id or f"evt_{uuid.uuid4().hex[:12]}",
        provider_txn_id=subscription_id,
        merchant_order_no=None,
        outcome=outcome,
        event_category=category,
        subscription_id=subscription_id,
        checkout_session_id=checkout_session_id,
        gateway_subscription_id=gateway_subscription_id,
        invoice_id=invoice_id,
        raw_payload={
            "created": now_ts,
            "data": {"object": event_data or {}},
        },
    )


# =====================================================================
#  1. subscription_activated — 首次订阅完成
# =====================================================================


class TestSubscriptionActivated:
    async def test_activate_from_incomplete(
        self, session, incomplete_subscription, test_customer
    ):
        event = _make_event(
            outcome="subscription_activated",
            checkout_session_id=incomplete_subscription.provider_checkout_session_id,
            gateway_subscription_id=incomplete_subscription.id,
            event_data={"subscription": "sub_activated_001"},
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(incomplete_subscription)
        assert incomplete_subscription.status == SubscriptionStatus.active.value
        assert incomplete_subscription.provider_subscription_id == "sub_activated_001"

    async def test_activate_with_trial(
        self, session, incomplete_subscription, test_customer
    ):
        incomplete_subscription.trial_end = datetime.now(UTC) + timedelta(days=7)
        await session.flush()

        event = _make_event(
            outcome="subscription_activated",
            gateway_subscription_id=incomplete_subscription.id,
            event_data={"subscription": "sub_trialing_001"},
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(incomplete_subscription)
        assert incomplete_subscription.status == SubscriptionStatus.trialing.value


# =====================================================================
#  2. subscription_created — Stripe 创建订阅事件
# =====================================================================


class TestSubscriptionCreated:
    async def test_created_sets_provider_id_and_period(
        self, session, incomplete_subscription, test_customer
    ):
        now = datetime.now(UTC)
        period_start = _ts(now)
        period_end = _ts(now + timedelta(days=30))

        event = _make_event(
            outcome="subscription_created",
            gateway_subscription_id=incomplete_subscription.id,
            event_data={
                "id": "sub_created_002",
                "status": "active",
                "items": {
                    "data": [
                        {
                            "current_period_start": period_start,
                            "current_period_end": period_end,
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(incomplete_subscription)
        assert incomplete_subscription.provider_subscription_id == "sub_created_002"
        assert incomplete_subscription.status == SubscriptionStatus.active.value
        assert incomplete_subscription.current_period_start is not None
        assert incomplete_subscription.current_period_end is not None


# =====================================================================
#  3. subscription_updated — 状态变更 & 价格/Plan 切换
# =====================================================================


class TestSubscriptionUpdated:
    async def test_updated_status_change(
        self, session, active_subscription, test_customer
    ):
        now = datetime.now(UTC)
        event = _make_event(
            outcome="subscription_updated",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "status": "past_due",
                "cancel_at_period_end": False,
                "items": {
                    "data": [
                        {
                            "current_period_start": _ts(now),
                            "current_period_end": _ts(now + timedelta(days=30)),
                            "price": {
                                "id": active_subscription.provider_price_id,
                            },
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.past_due.value

    async def test_updated_price_change_resolves_plan(
        self, session, active_subscription, pro_plan, test_customer
    ):
        """Webhook 推送了新的 price_id → 自动关联到对应 Plan。"""
        now = datetime.now(UTC)
        event = _make_event(
            outcome="subscription_updated",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "status": "active",
                "cancel_at_period_end": False,
                "items": {
                    "data": [
                        {
                            "current_period_start": _ts(now),
                            "current_period_end": _ts(now + timedelta(days=30)),
                            "price": {
                                "id": pro_plan.provider_price_id,
                                "product": pro_plan.provider_product_id,
                            },
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.plan_id == pro_plan.id
        assert active_subscription.provider_price_id == pro_plan.provider_price_id

    async def test_updated_clears_pending_downgrade(
        self, session, active_subscription, pro_plan, test_customer
    ):
        """价格变更与 pending_plan 匹配时，清除降级待生效状态。"""
        active_subscription.pending_plan_id = pro_plan.id
        active_subscription.pending_plan_change_at = datetime.now(UTC)
        active_subscription.provider_schedule_id = "sub_sched_pending"
        await session.flush()

        now = datetime.now(UTC)
        event = _make_event(
            outcome="subscription_updated",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "status": "active",
                "cancel_at_period_end": False,
                "items": {
                    "data": [
                        {
                            "current_period_start": _ts(now),
                            "current_period_end": _ts(now + timedelta(days=30)),
                            "price": {
                                "id": pro_plan.provider_price_id,
                                "product": pro_plan.provider_product_id,
                            },
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.plan_id == pro_plan.id
        assert active_subscription.pending_plan_id is None
        assert active_subscription.pending_plan_change_at is None
        assert active_subscription.provider_schedule_id is None

    async def test_updated_cancel_at_period_end(
        self, session, active_subscription, test_customer
    ):
        now = datetime.now(UTC)
        event = _make_event(
            outcome="subscription_updated",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "status": "active",
                "cancel_at_period_end": True,
                "items": {
                    "data": [
                        {
                            "current_period_start": _ts(now),
                            "current_period_end": _ts(now + timedelta(days=30)),
                            "price": {
                                "id": active_subscription.provider_price_id,
                            },
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.cancel_at_period_end is True


# =====================================================================
#  4. subscription_canceled — 取消订阅
# =====================================================================


class TestSubscriptionCanceled:
    async def test_canceled_sets_terminal_state(
        self, session, active_subscription, test_customer
    ):
        canceled_ts = _ts(datetime.now(UTC))
        event = _make_event(
            outcome="subscription_canceled",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "canceled_at": canceled_ts,
                "ended_at": canceled_ts,
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.canceled.value
        assert active_subscription.canceled_at is not None
        assert active_subscription.ended_at is not None


# =====================================================================
#  5. subscription_expired / subscription_payment_failed
# =====================================================================


class TestSubscriptionExpiredAndFailed:
    async def test_expired_from_incomplete(
        self, session, incomplete_subscription, test_customer
    ):
        event = _make_event(
            outcome="subscription_expired",
            gateway_subscription_id=incomplete_subscription.id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(incomplete_subscription)
        assert (
            incomplete_subscription.status
            == SubscriptionStatus.incomplete_expired.value
        )

    async def test_payment_failed_from_incomplete(
        self, session, incomplete_subscription, test_customer
    ):
        event = _make_event(
            outcome="subscription_payment_failed",
            gateway_subscription_id=incomplete_subscription.id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(incomplete_subscription)
        assert (
            incomplete_subscription.status
            == SubscriptionStatus.incomplete_expired.value
        )


# =====================================================================
#  6. invoice_paid — 续费成功
# =====================================================================


class TestInvoicePaid:
    async def test_invoice_paid_creates_payment_and_updates_period(
        self, session, active_subscription, test_customer, test_app
    ):
        now = datetime.now(UTC)
        new_start = _ts(now + timedelta(days=30))
        new_end = _ts(now + timedelta(days=60))
        invoice_id = f"in_test_{uuid.uuid4().hex[:8]}"

        event = _make_event(
            outcome="invoice_paid",
            category=EventCategory.invoice,
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "id": invoice_id,
                "amount_paid": 999,
                "currency": "usd",
                "payment_intent": "pi_test_inv_001",
                "status_transitions": {"paid_at": _ts(now)},
                "lines": {
                    "data": [
                        {
                            "type": "subscription",
                            "period": {"start": new_start, "end": new_end},
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        # 验证周期更新
        await session.refresh(active_subscription)
        assert active_subscription.current_period_start is not None
        assert active_subscription.current_period_end is not None

        # 验证 Payment 记录被创建
        stmt = select(Payment).where(
            Payment.merchant_order_no == f"sub_inv_{invoice_id}"
        )
        result = await session.execute(stmt)
        payment = result.scalar_one_or_none()
        assert payment is not None
        assert payment.amount == 999
        assert payment.subscription_id == active_subscription.id

    async def test_invoice_paid_recovers_from_past_due(
        self, session, active_subscription, test_customer
    ):
        active_subscription.status = SubscriptionStatus.past_due.value
        await session.flush()

        now = datetime.now(UTC)
        event = _make_event(
            outcome="invoice_paid",
            category=EventCategory.invoice,
            subscription_id=active_subscription.provider_subscription_id,
            event_data={
                "id": f"in_recover_{uuid.uuid4().hex[:8]}",
                "amount_paid": 999,
                "currency": "usd",
                "lines": {
                    "data": [
                        {
                            "type": "subscription",
                            "period": {
                                "start": _ts(now),
                                "end": _ts(now + timedelta(days=30)),
                            },
                        }
                    ]
                },
            },
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.active.value


# =====================================================================
#  7. invoice_payment_failed — 续费失败
# =====================================================================


class TestInvoicePaymentFailed:
    async def test_active_to_past_due(
        self, session, active_subscription, test_customer
    ):
        event = _make_event(
            outcome="invoice_payment_failed",
            category=EventCategory.invoice,
            subscription_id=active_subscription.provider_subscription_id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.past_due.value

    async def test_trialing_to_past_due(
        self, session, active_subscription, test_customer
    ):
        active_subscription.status = SubscriptionStatus.trialing.value
        await session.flush()

        event = _make_event(
            outcome="invoice_payment_failed",
            category=EventCategory.invoice,
            subscription_id=active_subscription.provider_subscription_id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.past_due.value


# =====================================================================
#  8. subscription_paused / subscription_resumed
# =====================================================================


class TestPauseResumed:
    async def test_paused(self, session, active_subscription, test_customer):
        event = _make_event(
            outcome="subscription_paused",
            subscription_id=active_subscription.provider_subscription_id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.paused.value

    async def test_resumed(self, session, active_subscription, test_customer):
        active_subscription.status = SubscriptionStatus.paused.value
        await session.flush()

        event = _make_event(
            outcome="subscription_resumed",
            subscription_id=active_subscription.provider_subscription_id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.active.value


# =====================================================================
#  9. 事件时序保护
# =====================================================================


class TestEventOrdering:
    async def test_old_event_skipped(
        self, session, active_subscription, test_customer
    ):
        """last_event_at 之前的事件应被跳过。"""
        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        active_subscription.last_event_at = datetime.now(UTC)
        await session.flush()

        event = _make_event(
            outcome="subscription_canceled",
            subscription_id=active_subscription.provider_subscription_id,
            event_data={"canceled_at": _ts(old_time)},
            event_created=_ts(old_time),
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.active.value


# =====================================================================
#  10. Webhook Delivery 创建
# =====================================================================


class TestWebhookDelivery:
    async def test_webhook_delivery_created(
        self, session, active_subscription, test_customer, test_app
    ):
        event = _make_event(
            outcome="subscription_paused",
            subscription_id=active_subscription.provider_subscription_id,
        )

        svc = CallbackService(session)
        await svc.process_callback(event)

        stmt = select(WebhookDelivery).where(
            WebhookDelivery.source_type == "subscription",
            WebhookDelivery.source_id == active_subscription.id,
        )
        result = await session.execute(stmt)
        delivery = result.scalars().first()

        assert delivery is not None
        assert delivery.event_type == "subscription.subscription_paused"
        assert delivery.notify_url == active_subscription.notify_url
        assert delivery.payload["subscription_id"] == str(active_subscription.id)
