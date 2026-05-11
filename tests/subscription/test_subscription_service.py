"""
SubscriptionService 单元测试

覆盖场景:
  1. 首次订阅（create_subscription）
  2. 取消订阅（cancel_subscription）
  3. 恢复订阅（resume_subscription）
  4. 升级 / 降级（change_plan）
  5. 暂停 / 恢复暂停（pause / unpause）
  6. 取消待生效降级（cancel_pending_downgrade）
"""

import uuid
from datetime import datetime, timedelta, UTC
from unittest.mock import patch

import pytest

from gateway.core.constants import PaymentMethod, SubscriptionStatus
from gateway.core.exceptions import (
    BadRequestException,
    ConflictException,
    NotFoundException,
)
from gateway.core.models import Subscription
from gateway.core.schemas import (
    CancelSubscriptionRequest,
    ChangePlanRequest,
    CreateSubscriptionRequest,
)
from gateway.services.subscriptions import SubscriptionService


# =====================================================================
#  1. 创建订阅
# =====================================================================


class TestCreateSubscription:
    async def test_create_success(
        self, session, test_app, basic_plan, patch_deps
    ):
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="brand_new_user",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        sub, checkout_url = await svc.create_subscription(test_app, req)

        assert sub.status == SubscriptionStatus.incomplete.value
        assert sub.plan_id == basic_plan.id
        assert sub.amount == basic_plan.amount
        assert sub.currency == basic_plan.currency
        assert sub.cancel_at_period_end is False
        assert sub.provider_checkout_session_id == "cs_test_new_789"
        assert sub.last_event_at is None
        assert "cs_test_new_789" in checkout_url

        adapter = patch_deps
        adapter.create_customer.assert_called_once()
        adapter.create_subscription_checkout.assert_called_once()

    async def test_create_with_trial(
        self, session, test_app, basic_plan, patch_deps
    ):
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="trial_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            trial_period_days=14,
        )

        sub, _ = await svc.create_subscription(test_app, req)

        assert sub.status == SubscriptionStatus.incomplete.value
        assert sub.trial_end is not None
        # trial_end 应在 ~14 天后
        delta = sub.trial_end - datetime.now(UTC)
        assert 13 <= delta.days <= 14

    async def test_app_managed_trial_starts_without_initial_payment(
        self, session, test_app, basic_plan, patch_deps
    ):
        # wechat_pay only supports CNY/HKD; align fixture with currency validation.
        basic_plan.currency = Currency.CNY
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="wechat_trial_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            trial_period_days=7,
            payment_method=PaymentMethod.wechat_pay,
            payment_options={"client": "web"},
        )

        sub, checkout_url = await svc.create_subscription(test_app, req)

        assert checkout_url == ""
        assert sub.status == SubscriptionStatus.trialing.value
        assert sub.current_period_end == sub.trial_end
        patch_deps.create_payment.assert_not_called()

        payments = (
            await session.execute(
                select(Payment).where(Payment.subscription_id == sub.id)
            )
        ).scalars().all()
        assert payments == []

    async def test_app_managed_alipay_rejects_unsupported_plan_currency(
        self, session, test_app, basic_plan, patch_deps
    ):
        basic_plan.currency = Currency.JPY
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="alipay_jpy_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.alipay,
        )

        with pytest.raises(BadRequestException, match="alipay 仅支持"):
            await svc.create_subscription(test_app, req)

    async def test_app_managed_wechat_rejects_unsupported_plan_currency(
        self, session, test_app, basic_plan, patch_deps
    ):
        # Stripe wechat_pay only supports CNY/HKD; USD plan must be rejected upfront.
        basic_plan.currency = Currency.USD
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="wechat_usd_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.wechat_pay,
        )

        with pytest.raises(BadRequestException, match="wechat_pay 仅支持"):
            await svc.create_subscription(test_app, req)

    async def test_app_managed_wechat_accepts_cny_plan(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # CNY plan should pass currency validation and proceed to create_payment.
        basic_plan.currency = Currency.CNY
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="wechat_cny_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.wechat_pay,
        )

        sub, _checkout_url = await svc.create_subscription(test_app, req)
        assert sub.payment_method == PaymentMethod.wechat_pay.value
        # adapter.create_payment must have been invoked with CNY + wechat_pay
        mock_adapter.create_payment.assert_called_once()
        kwargs = mock_adapter.create_payment.call_args.kwargs
        assert kwargs["currency"] == "CNY"
        assert kwargs["payment_method"] == PaymentMethod.wechat_pay

    async def test_app_managed_passes_email_to_create_payment_metadata(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # Regression: alipay/wechat_pay (app-managed) must forward req.email
        # into adapter.create_payment(metadata=...) so Stripe Checkout can
        # pre-fill the email field on the hosted payment page.
        basic_plan.currency = __import__(
            "gateway.core.constants", fromlist=["Currency"]
        ).Currency.CNY
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="alipay_email_user",
            plan_id=basic_plan.id,
            email="user@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.alipay,
        )

        await svc.create_subscription(test_app, req)

        mock_adapter.create_payment.assert_called_once()
        metadata = mock_adapter.create_payment.call_args.kwargs["metadata"]
        assert metadata["customer_email"] == "user@example.com"
        assert metadata["subscription_id"]
        assert metadata["app_id"]

    async def test_stripe_create_payment_keeps_customer_email_out_of_metadata(self):
        # Regression for PII risk: customer_email is forwarded to Stripe via
        # the dedicated session_data["customer_email"] field, but it must NOT
        # be copied into session_data["metadata"] (Stripe guidance: keep PII
        # out of metadata).
        from unittest.mock import AsyncMock, MagicMock, patch as patch_obj
        import stripe as _stripe
        from gateway.providers.stripe import StripeAdapter
        from gateway.core.constants import PaymentMethod

        adapter = StripeAdapter()
        fake_session = MagicMock(id="cs_test_unit", url="https://stripe.test/cs")
        with patch_obj.object(
            _stripe.checkout.Session,
            "create_async",
            new=AsyncMock(return_value=fake_session),
        ) as create_mock:
            await adapter.create_payment(
                currency="CNY",
                merchant_order_no="unit_001",
                quantity=1,
                unit_amount=9900,
                product_name="Unit",
                notify_url="https://example.com/n",
                payment_method=PaymentMethod.alipay,
                payment_options=None,
                success_url="https://example.com/s",
                cancel_url="https://example.com/c",
                metadata={
                    "subscription_id": "sub_x",
                    "app_id": "app_x",
                    "customer_email": "pii@example.com",
                },
                app_id="app_x",
                expire_minutes=30,
            )
        kwargs = create_mock.call_args.kwargs
        assert kwargs["customer_email"] == "pii@example.com"
        # PII must not bleed into metadata or payment_intent_data.metadata
        assert "customer_email" not in kwargs["metadata"]
        assert "customer_email" not in kwargs["payment_intent_data"]["metadata"]
        # The non-PII business keys are still forwarded
        assert kwargs["metadata"]["subscription_id"] == "sub_x"
        assert kwargs["metadata"]["app_id"] == "app_x"

    async def test_app_managed_omits_customer_email_when_request_lacks_it(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # Without req.email we must NOT inject a None / empty customer_email
        # key — keeping the metadata minimal avoids polluting Stripe metadata.
        basic_plan.currency = __import__(
            "gateway.core.constants", fromlist=["Currency"]
        ).Currency.CNY
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="alipay_no_email_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.alipay,
        )

        await svc.create_subscription(test_app, req)

        metadata = mock_adapter.create_payment.call_args.kwargs["metadata"]
        assert "customer_email" not in metadata

    async def test_existing_customer_email_change_synced_to_provider(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # When the same external_user_id subscribes again with a new email,
        # the gateway must propagate the change to the provider so future
        # Checkout sessions (card path) pre-fill the new address.
        from gateway.core.models import Customer
        from gateway.core.constants import Provider

        existing = Customer(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            external_user_id="email_change_user",
            provider_customer_id="cus_existing_001",
            email="old@example.com",
        )
        session.add(existing)
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="email_change_user",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        await svc.create_subscription(test_app, req)

        mock_adapter.update_customer_email.assert_called_once_with(
            "cus_existing_001", "new@example.com"
        )
        await session.refresh(existing)
        assert existing.email == "new@example.com"

    async def test_existing_customer_unchanged_email_skips_provider_sync(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # Same email → no provider call (avoid unnecessary API traffic).
        from gateway.core.models import Customer
        from gateway.core.constants import Provider

        existing = Customer(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            external_user_id="same_email_user",
            provider_customer_id="cus_existing_002",
            email="same@example.com",
        )
        session.add(existing)
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="same_email_user",
            plan_id=basic_plan.id,
            email="same@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        await svc.create_subscription(test_app, req)

        mock_adapter.update_customer_email.assert_not_called()

    async def test_existing_customer_provider_sync_failure_keeps_old_email(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # Provider call failure must not block the subscribe flow, but the
        # local email must NOT advance — otherwise subsequent subscribes see
        # matching emails and never retry the sync, leaving the provider
        # permanently stale.
        from gateway.core.models import Customer
        from gateway.core.constants import Provider

        existing = Customer(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            external_user_id="sync_fail_user",
            provider_customer_id="cus_existing_003",
            email="old@example.com",
        )
        session.add(existing)
        await session.flush()

        mock_adapter.update_customer_email.side_effect = RuntimeError("stripe down")

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="sync_fail_user",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        sub, _ = await svc.create_subscription(test_app, req)
        assert sub is not None
        await session.refresh(existing)
        assert existing.email == "old@example.com"  # stays retriable

    async def test_existing_customer_not_implemented_advances_local_email(
        self, session, test_app, basic_plan, patch_deps, mock_adapter
    ):
        # When the provider explicitly does not support email sync (raises
        # NotImplementedError), there's no point keeping the local row stale —
        # advance it so the diff doesn't trigger a useless call every time.
        from gateway.core.models import Customer
        from gateway.core.constants import Provider

        existing = Customer(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            external_user_id="not_impl_user",
            provider_customer_id="cus_existing_004",
            email="old@example.com",
        )
        session.add(existing)
        await session.flush()

        mock_adapter.update_customer_email.side_effect = NotImplementedError()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="not_impl_user",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        await svc.create_subscription(test_app, req)
        await session.refresh(existing)
        assert existing.email == "new@example.com"

    async def test_create_inactive_plan_rejected(
        self, session, test_app, basic_plan, patch_deps
    ):
        basic_plan.is_active = False
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="user_inactive",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        with pytest.raises(BadRequestException, match="不存在或已停用"):
            await svc.create_subscription(test_app, req)

    async def test_create_plan_without_price_rejected(
        self, session, test_app, basic_plan, patch_deps
    ):
        basic_plan.provider_price_id = None
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="user_no_price",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        with pytest.raises(BadRequestException, match="渠道同步"):
            await svc.create_subscription(test_app, req)

    async def test_create_app_managed_without_provider_price_allowed(
        self, session, test_app, basic_plan, patch_deps
    ):
        # wechat_pay only supports CNY/HKD; align fixture with currency validation.
        basic_plan.currency = Currency.CNY
        basic_plan.provider_price_id = None
        await session.flush()

        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="wechat_user",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            payment_method=PaymentMethod.wechat_pay,
            payment_options={"client": "web"},
        )

        sub, checkout_url = await svc.create_subscription(test_app, req)

        assert sub.payment_method == PaymentMethod.wechat_pay.value
        assert sub.provider_price_id is None
        assert sub.last_event_at is None
        assert "cs_test_pay_123" in checkout_url
        patch_deps.create_payment.assert_called_once()

    async def test_create_duplicate_incomplete_rejected(
        self, session, test_app, basic_plan, incomplete_subscription, patch_deps
    ):
        """已有 incomplete 订阅时不允许再次创建。"""
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="test_user_001",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        with pytest.raises(ConflictException, match="未完成的订阅"):
            await svc.create_subscription(test_app, req)

    async def test_create_with_force_cleanup_cleans_incomplete(
        self, session, test_app, basic_plan, incomplete_subscription, patch_deps
    ):
        """force_cleanup=True 时先过期未完成订阅，再创建新的 Checkout。"""
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="test_user_001",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            force_cleanup=True,
        )

        sub, checkout_url = await svc.create_subscription(test_app, req)

        assert sub.status == SubscriptionStatus.incomplete.value
        assert sub.plan_id == basic_plan.id
        assert "cs_test_new_789" in checkout_url
        patch_deps.cancel_payment.assert_called()

        await session.refresh(incomplete_subscription)
        assert (
            incomplete_subscription.status
            == SubscriptionStatus.incomplete_expired.value
        )

    async def test_create_with_force_cleanup_cleans_active(
        self, session, test_app, basic_plan, active_subscription, patch_deps
    ):
        """force_cleanup=True 时先立即取消活跃订阅，再创建新的 Checkout。"""
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="test_user_001",
            plan_id=basic_plan.id,
            email="new@example.com",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            force_cleanup=True,
        )

        sub, _ = await svc.create_subscription(test_app, req)

        assert sub.status == SubscriptionStatus.incomplete.value
        patch_deps.cancel_subscription.assert_called_with(
            "sub_test_100", immediate=True
        )

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.canceled.value

    async def test_create_duplicate_active_rejected(
        self, session, test_app, basic_plan, active_subscription, patch_deps
    ):
        """subscription_single_active=True 时，已有活跃订阅不允许再创建。"""
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id="test_user_001",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        with pytest.raises(ConflictException, match="已有活跃订阅"):
            await svc.create_subscription(test_app, req)


# =====================================================================
#  2. 取消订阅
# =====================================================================


class TestCancelSubscription:
    async def test_cancel_at_period_end(
        self, session, test_app, active_subscription, patch_deps
    ):
        svc = SubscriptionService(session)
        req = CancelSubscriptionRequest(immediate=False)

        sub = await svc.cancel_subscription(
            test_app.id, active_subscription.id, req
        )

        assert sub.cancel_at_period_end is True
        assert sub.status == SubscriptionStatus.active.value
        assert sub.canceled_at is None
        patch_deps.cancel_subscription.assert_called_once_with(
            "sub_test_100", immediate=False
        )

    async def test_cancel_immediate(
        self, session, test_app, active_subscription, patch_deps
    ):
        svc = SubscriptionService(session)
        req = CancelSubscriptionRequest(immediate=True)

        sub = await svc.cancel_subscription(
            test_app.id, active_subscription.id, req
        )

        assert sub.status == SubscriptionStatus.canceled.value
        assert sub.canceled_at is not None
        assert sub.ended_at is not None
        patch_deps.cancel_subscription.assert_called_once_with(
            "sub_test_100", immediate=True
        )

    async def test_cancel_releases_pending_downgrade(
        self, session, test_app, active_subscription, pro_plan, patch_deps
    ):
        """取消时如果有待生效的降级 schedule，应先释放。"""
        active_subscription.provider_schedule_id = "sub_sched_old"
        active_subscription.pending_plan_id = pro_plan.id
        active_subscription.pending_plan_change_at = datetime.now(UTC)
        await session.flush()

        svc = SubscriptionService(session)
        req = CancelSubscriptionRequest(immediate=False)

        sub = await svc.cancel_subscription(
            test_app.id, active_subscription.id, req
        )

        assert sub.pending_plan_id is None
        assert sub.pending_plan_change_at is None
        assert sub.provider_schedule_id is None
        patch_deps.release_subscription_schedule.assert_called_once_with(
            "sub_sched_old"
        )

    async def test_cancel_wrong_status_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        active_subscription.status = SubscriptionStatus.canceled.value
        await session.flush()

        svc = SubscriptionService(session)
        req = CancelSubscriptionRequest(immediate=False)

        with pytest.raises(BadRequestException, match="不允许取消"):
            await svc.cancel_subscription(
                test_app.id, active_subscription.id, req
            )

    async def test_cancel_incomplete_no_provider_id_rejected(
        self, session, test_app, incomplete_subscription, patch_deps
    ):
        """incomplete 状态（无 provider_subscription_id）不允许取消。"""
        incomplete_subscription.status = SubscriptionStatus.active.value
        incomplete_subscription.provider_subscription_id = None
        await session.flush()

        svc = SubscriptionService(session)
        req = CancelSubscriptionRequest(immediate=False)

        with pytest.raises(BadRequestException, match="尚未激活"):
            await svc.cancel_subscription(
                test_app.id, incomplete_subscription.id, req
            )


# =====================================================================
#  3. 恢复订阅
# =====================================================================


class TestResumeSubscription:
    async def test_resume_success(
        self, session, test_app, active_subscription, patch_deps
    ):
        active_subscription.cancel_at_period_end = True
        await session.flush()

        svc = SubscriptionService(session)
        sub = await svc.resume_subscription(
            test_app.id, active_subscription.id
        )

        assert sub.cancel_at_period_end is False
        patch_deps.resume_subscription.assert_called_once_with("sub_test_100")

    async def test_resume_not_canceling_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        """未设置 cancel_at_period_end 时不允许恢复。"""
        assert active_subscription.cancel_at_period_end is False

        svc = SubscriptionService(session)

        with pytest.raises(BadRequestException, match="未设置周期末取消"):
            await svc.resume_subscription(
                test_app.id, active_subscription.id
            )

    async def test_resume_wrong_status_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        active_subscription.status = SubscriptionStatus.canceled.value
        active_subscription.cancel_at_period_end = True
        await session.flush()

        svc = SubscriptionService(session)

        with pytest.raises(BadRequestException, match="不允许恢复"):
            await svc.resume_subscription(
                test_app.id, active_subscription.id
            )

    async def test_resume_expired_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        """周期已过期时不允许恢复，需要重新创建。"""
        active_subscription.cancel_at_period_end = True
        active_subscription.current_period_end = datetime(2020, 1, 1, tzinfo=UTC)
        await session.flush()

        svc = SubscriptionService(session)

        with pytest.raises(BadRequestException, match="已过期"):
            await svc.resume_subscription(
                test_app.id, active_subscription.id
            )


# =====================================================================
#  4. 升级 / 降级
# =====================================================================


class TestChangePlan:
    async def test_upgrade_success(
        self,
        session,
        test_app,
        active_subscription,
        basic_plan,
        enterprise_plan,
        test_customer,
        patch_deps,
    ):
        """升级: tier 变高，立即生效。"""
        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=enterprise_plan.id)

        result = await svc.change_plan(
            test_app.id, active_subscription.id, req
        )

        assert result["direction"] == "upgrade"
        assert result["effective"] == "immediate"
        assert result["current_plan"].id == enterprise_plan.id
        assert result["pending_plan"] is None

        assert active_subscription.plan_id == enterprise_plan.id
        assert active_subscription.amount == enterprise_plan.amount
        assert active_subscription.provider_price_id == enterprise_plan.provider_price_id

        patch_deps.change_subscription_plan.assert_called_once()

    async def test_downgrade_success(
        self,
        session,
        test_app,
        active_subscription,
        basic_plan,
        pro_plan,
        enterprise_plan,
        patch_deps,
    ):
        """降级: tier 变低，周期末生效（通过 SubscriptionSchedule）。"""
        # 先升到 enterprise
        active_subscription.plan_id = enterprise_plan.id
        active_subscription.provider_price_id = enterprise_plan.provider_price_id
        active_subscription.amount = enterprise_plan.amount
        await session.flush()

        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=basic_plan.id)

        result = await svc.change_plan(
            test_app.id, active_subscription.id, req
        )

        assert result["direction"] == "downgrade"
        assert result["effective"] == "period_end"
        assert result["current_plan"].id == enterprise_plan.id
        assert result["pending_plan"].id == basic_plan.id
        assert result["pending_plan_change_at"] is not None

        assert active_subscription.pending_plan_id == basic_plan.id
        assert active_subscription.provider_schedule_id == "sub_sched_test_001"

        patch_deps.schedule_subscription_downgrade.assert_called_once()

    async def test_upgrade_clears_stale_schedule(
        self,
        session,
        test_app,
        active_subscription,
        basic_plan,
        pro_plan,
        enterprise_plan,
        test_customer,
        patch_deps,
    ):
        """升级时如果存在残留的 provider_schedule_id，应先释放。"""
        active_subscription.plan_id = pro_plan.id
        active_subscription.provider_price_id = pro_plan.provider_price_id
        active_subscription.amount = pro_plan.amount
        # pending_plan_id 不设置（否则会被前置校验拒绝），只设 schedule_id
        active_subscription.provider_schedule_id = "sub_sched_existing"
        await session.flush()

        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=enterprise_plan.id)

        result = await svc.change_plan(
            test_app.id, active_subscription.id, req
        )

        assert result["direction"] == "upgrade"
        assert active_subscription.provider_schedule_id is None
        patch_deps.release_subscription_schedule.assert_called_once_with(
            "sub_sched_existing"
        )

    async def test_change_same_plan_rejected(
        self, session, test_app, active_subscription, basic_plan, patch_deps
    ):
        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=basic_plan.id)

        with pytest.raises(BadRequestException, match="与当前计划相同"):
            await svc.change_plan(
                test_app.id, active_subscription.id, req
            )

    async def test_change_same_tier_rejected(
        self, session, test_app, patch_deps
    ):
        """同等级（tier 相同）不允许变更。"""
        from sqlalchemy import select

        # 独立创建数据，避免其他 fixture 干扰
        customer = Customer(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            external_user_id="tier_test_user",
            provider_customer_id="cus_tier_test",
        )
        plan_a = Plan(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            slug="tier-a",
            name="Plan A",
            amount=100,
            currency=Currency.USD,
            interval="month",
            interval_count=1,
            provider_product_id="prod_a",
            provider_price_id="price_a",
            tier=5,
            is_active=True,
        )
        plan_b = Plan(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            slug="tier-b",
            name="Plan B",
            amount=200,
            currency=Currency.USD,
            interval="month",
            interval_count=1,
            provider_product_id="prod_b",
            provider_price_id="price_b",
            tier=5,
            is_active=True,
        )
        session.add_all([customer, plan_a, plan_b])
        await session.flush()

        now = datetime.now(UTC)
        sub = Subscription(
            id=uuid.uuid4(),
            app_id=test_app.id,
            provider=Provider.stripe,
            customer_id=customer.id,
            plan_id=plan_a.id,
            provider_subscription_id="sub_tier_test",
            provider_price_id=plan_a.provider_price_id,
            amount=plan_a.amount,
            currency=plan_a.currency,
            status=SubscriptionStatus.active.value,
            current_period_start=now,
            current_period_end=now + timedelta(days=30),
            cancel_at_period_end=False,
        )
        session.add(sub)
        await session.flush()

        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=plan_b.id)

        with pytest.raises(BadRequestException, match="同等级"):
            await svc.change_plan(test_app.id, sub.id, req)

    async def test_change_inactive_target_rejected(
        self, session, test_app, active_subscription, pro_plan, patch_deps
    ):
        pro_plan.is_active = False
        await session.flush()

        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=pro_plan.id)

        with pytest.raises(BadRequestException, match="已停用"):
            await svc.change_plan(
                test_app.id, active_subscription.id, req
            )

    async def test_downgrade_already_pending_rejected(
        self,
        session,
        test_app,
        active_subscription,
        basic_plan,
        pro_plan,
        enterprise_plan,
        patch_deps,
    ):
        """已有待生效变更时不允许再降级。"""
        active_subscription.plan_id = enterprise_plan.id
        active_subscription.pending_plan_id = pro_plan.id
        await session.flush()

        svc = SubscriptionService(session)
        req = ChangePlanRequest(new_plan_id=basic_plan.id)

        with pytest.raises(BadRequestException, match="待生效的计划变更"):
            await svc.change_plan(
                test_app.id, active_subscription.id, req
            )

    async def test_cancel_pending_downgrade(
        self, session, test_app, active_subscription, pro_plan, patch_deps
    ):
        active_subscription.pending_plan_id = pro_plan.id
        active_subscription.pending_plan_change_at = datetime.now(UTC) + timedelta(days=20)
        active_subscription.provider_schedule_id = "sub_sched_cancel_me"
        await session.flush()

        svc = SubscriptionService(session)
        sub = await svc.cancel_pending_downgrade(
            test_app.id, active_subscription.id
        )

        assert sub.pending_plan_id is None
        assert sub.pending_plan_change_at is None
        assert sub.provider_schedule_id is None
        patch_deps.release_subscription_schedule.assert_called_once_with(
            "sub_sched_cancel_me"
        )


# =====================================================================
#  5. 暂停 / 恢复暂停
# =====================================================================


class TestPauseUnpause:
    async def test_pause_subscription(
        self, session, test_app, active_subscription, patch_deps
    ):
        svc = SubscriptionService(session)
        sub = await svc.pause_subscription(
            test_app.id, active_subscription.id
        )

        assert sub.status == SubscriptionStatus.paused.value
        patch_deps.pause_subscription.assert_called_once_with("sub_test_100")

    async def test_pause_canceling_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        active_subscription.cancel_at_period_end = True
        await session.flush()

        svc = SubscriptionService(session)

        with pytest.raises(BadRequestException, match="待取消"):
            await svc.pause_subscription(
                test_app.id, active_subscription.id
            )

    async def test_unpause_subscription(
        self, session, test_app, active_subscription, patch_deps
    ):
        active_subscription.status = SubscriptionStatus.paused.value
        await session.flush()

        svc = SubscriptionService(session)
        sub = await svc.unpause_subscription(
            test_app.id, active_subscription.id
        )

        assert sub.status == SubscriptionStatus.active.value
        patch_deps.unpause_subscription.assert_called_once_with("sub_test_100")

    async def test_unpause_not_paused_rejected(
        self, session, test_app, active_subscription, patch_deps
    ):
        svc = SubscriptionService(session)

        with pytest.raises(BadRequestException, match="未处于暂停"):
            await svc.unpause_subscription(
                test_app.id, active_subscription.id
            )


# =====================================================================
#  6. 查询
# =====================================================================


class TestQueries:
    async def test_get_subscription(
        self, session, test_app, active_subscription
    ):
        svc = SubscriptionService(session)
        sub = await svc.get_subscription(
            test_app.id, active_subscription.id
        )
        assert sub.id == active_subscription.id

    async def test_get_subscription_not_found(self, session, test_app):
        svc = SubscriptionService(session)
        with pytest.raises(NotFoundException):
            await svc.get_subscription(test_app.id, uuid.uuid4())

    async def test_get_user_active_subscription(
        self, session, test_app, test_customer, active_subscription
    ):
        svc = SubscriptionService(session)
        sub = await svc.get_user_active_subscription(
            test_app.id, test_customer.external_user_id
        )
        assert sub is not None
        assert sub.id == active_subscription.id

    async def test_list_subscriptions(
        self, session, test_app, active_subscription
    ):
        svc = SubscriptionService(session)
        subs, total = await svc.list_subscriptions(test_app.id)
        assert total >= 1
        assert any(s.id == active_subscription.id for s in subs)


from sqlalchemy import select  # noqa: E402

from gateway.core.constants import Currency, Provider  # noqa: E402
from gateway.core.models import Customer, Payment, Plan, Subscription  # noqa: E402
