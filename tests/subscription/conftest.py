"""
订阅测试 fixtures：Mock Adapter、测试数据工厂
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, UTC
from unittest.mock import AsyncMock, patch

import pytest

from gateway.core.constants import Currency, Provider, SubscriptionStatus
from gateway.core.models import App, Customer, Plan, Subscription
from gateway.core.schemas import PaymentTypeEnum
from gateway.providers.base import (
    ProviderPaymentResult,
    SubscriptionActionResult,
    SubscriptionCheckoutResult,
)
from gateway.providers.stripe import StripeAdapter


# ── Mock Adapter ──


@pytest.fixture
def mock_adapter():
    """返回一个 spec=StripeAdapter 的 AsyncMock，所有方法预设合理返回值。"""
    adapter = AsyncMock(spec=StripeAdapter)
    adapter.provider = Provider.stripe

    adapter.create_customer.return_value = "cus_test_new_456"
    adapter.create_subscription_checkout.return_value = SubscriptionCheckoutResult(
        session_id="cs_test_new_789",
        checkout_url="https://checkout.stripe.com/c/pay/cs_test_new_789",
    )
    adapter.create_payment.return_value = ProviderPaymentResult(
        type=PaymentTypeEnum.url,
        payload={
            "checkout_url": "https://checkout.stripe.com/c/pay/cs_test_pay_123",
            "session_id": "cs_test_pay_123",
        },
        provider_txn_id="cs_test_pay_123",
    )
    adapter.cancel_subscription.return_value = SubscriptionActionResult(
        subscription_id="sub_test_100",
        status="active",
        current_period_end=datetime(2026, 6, 1, tzinfo=UTC),
        cancel_at_period_end=True,
    )
    adapter.resume_subscription.return_value = SubscriptionActionResult(
        subscription_id="sub_test_100",
        status="active",
        cancel_at_period_end=False,
    )
    adapter.change_subscription_plan.return_value = SubscriptionActionResult(
        subscription_id="sub_test_100",
        status="active",
    )
    adapter.schedule_subscription_downgrade.return_value = "sub_sched_test_001"
    adapter.release_subscription_schedule.return_value = None
    adapter.pause_subscription.return_value = SubscriptionActionResult(
        subscription_id="sub_test_100",
        status="paused",
    )
    adapter.unpause_subscription.return_value = SubscriptionActionResult(
        subscription_id="sub_test_100",
        status="active",
    )
    adapter.cancel_payment = AsyncMock(return_value={"status": "expired"})
    return adapter


@pytest.fixture
def patch_deps(session, mock_adapter):
    """
    统一 patch SubscriptionService 的两个外部依赖:
    - get_adapter  → 返回 mock_adapter
    - get_session_ctx → 返回当前测试 session（避免独立事务）
    """

    @asynccontextmanager
    async def _mock_session_ctx():
        yield session

    with (
        patch(
            "gateway.services.subscriptions.get_adapter", return_value=mock_adapter
        ),
        patch(
            "gateway.services.subscriptions.get_session_ctx", _mock_session_ctx
        ),
    ):
        yield mock_adapter


# ── 测试数据 ──


@pytest.fixture
async def test_app(session):
    app = App(
        id=uuid.uuid4(),
        name="Test App",
        api_key=f"test_api_key_{uuid.uuid4().hex[:8]}",
        is_active=True,
        notify_url="https://example.com/webhook",
    )
    session.add(app)
    await session.flush()
    return app


@pytest.fixture
async def basic_plan(session, test_app):
    plan = Plan(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        slug="basic-monthly",
        name="Basic Plan",
        amount=999,
        currency=Currency.USD,
        interval="month",
        interval_count=1,
        provider_product_id="prod_basic_test",
        provider_price_id="price_basic_test",
        tier=1,
        is_active=True,
    )
    session.add(plan)
    await session.flush()
    return plan


@pytest.fixture
async def pro_plan(session, test_app):
    plan = Plan(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        slug="pro-monthly",
        name="Pro Plan",
        amount=2999,
        currency=Currency.USD,
        interval="month",
        interval_count=1,
        provider_product_id="prod_pro_test",
        provider_price_id="price_pro_test",
        tier=2,
        is_active=True,
    )
    session.add(plan)
    await session.flush()
    return plan


@pytest.fixture
async def enterprise_plan(session, test_app):
    plan = Plan(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        slug="enterprise-monthly",
        name="Enterprise Plan",
        amount=9999,
        currency=Currency.USD,
        interval="month",
        interval_count=1,
        provider_product_id="prod_ent_test",
        provider_price_id="price_ent_test",
        tier=3,
        is_active=True,
    )
    session.add(plan)
    await session.flush()
    return plan


@pytest.fixture
async def test_customer(session, test_app):
    customer = Customer(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        external_user_id="test_user_001",
        provider_customer_id="cus_test_existing",
        email="user@example.com",
    )
    session.add(customer)
    await session.flush()
    return customer


@pytest.fixture
async def active_subscription(session, test_app, test_customer, basic_plan):
    """一个处于 active 状态的订阅，带完整的周期信息。"""
    now = datetime.now(UTC)
    sub = Subscription(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        customer_id=test_customer.id,
        plan_id=basic_plan.id,
        provider_subscription_id="sub_test_100",
        provider_checkout_session_id="cs_test_old_100",
        provider_price_id=basic_plan.provider_price_id,
        amount=basic_plan.amount,
        currency=basic_plan.currency,
        status=SubscriptionStatus.active.value,
        current_period_start=now,
        current_period_end=now + timedelta(days=30),
        cancel_at_period_end=False,
        notify_url="https://example.com/webhook",
    )
    session.add(sub)
    await session.flush()
    return sub


@pytest.fixture
async def incomplete_subscription(session, test_app, test_customer, basic_plan):
    """一个处于 incomplete 状态的订阅（Checkout 尚未完成）。"""
    sub = Subscription(
        id=uuid.uuid4(),
        app_id=test_app.id,
        provider=Provider.stripe,
        customer_id=test_customer.id,
        plan_id=basic_plan.id,
        provider_subscription_id=None,
        provider_checkout_session_id="cs_test_incomplete_200",
        provider_price_id=basic_plan.provider_price_id,
        amount=basic_plan.amount,
        currency=basic_plan.currency,
        status=SubscriptionStatus.incomplete.value,
        cancel_at_period_end=False,
        notify_url="https://example.com/webhook",
    )
    session.add(sub)
    await session.flush()
    return sub
