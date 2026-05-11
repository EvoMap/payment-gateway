"""
App-managed renewal lifecycle tests.
"""

from datetime import datetime, timedelta, UTC
from unittest.mock import patch

from sqlalchemy import select

from gateway.core.constants import PaymentStatus, SubscriptionStatus
from gateway.core.models import Payment
from gateway.services.renewal import RenewalService


class TestRenewalLifecycle:
    async def test_scan_skips_already_expired_active_subscription(
        self, session, active_subscription, patch_deps
    ):
        active_subscription.payment_method = "wechat_pay"
        active_subscription.current_period_end = datetime.now(UTC) - timedelta(days=1)
        await session.flush()

        svc = RenewalService(session)
        with patch("gateway.services.renewal.get_adapter", return_value=patch_deps):
            await svc.scan_and_create_renewals()

        await session.refresh(active_subscription)
        assert active_subscription.renewal_payment_id is None
        patch_deps.create_payment.assert_not_called()

    async def test_expired_active_moves_to_past_due_and_creates_renewal(
        self, session, active_subscription, patch_deps
    ):
        period_end = datetime.now(UTC) - timedelta(days=1)
        active_subscription.payment_method = "wechat_pay"
        active_subscription.current_period_end = period_end
        await session.flush()

        svc = RenewalService(session)
        with patch("gateway.services.renewal.get_adapter", return_value=patch_deps):
            await svc.enforce_grace_periods()

        await session.refresh(active_subscription)
        assert active_subscription.status == SubscriptionStatus.past_due.value
        assert active_subscription.grace_period_end == period_end + timedelta(days=3)
        assert active_subscription.renewal_payment_id is not None

        payment = (
            await session.execute(
                select(Payment).where(
                    Payment.id == active_subscription.renewal_payment_id
                )
            )
        ).scalar_one()
        assert payment.status == PaymentStatus.pending
        assert payment.merchant_order_no.startswith(
            f"sub_renew_{active_subscription.id}_"
        )

    async def test_renewal_payment_forwards_customer_email_for_prefill(
        self, session, active_subscription, test_customer, patch_deps
    ):
        # App-managed renewals must pre-fill the saved customer email on the
        # hosted Checkout page (parity with the first-time subscribe path).
        active_subscription.payment_method = "wechat_pay"
        active_subscription.current_period_end = (
            datetime.now(UTC) - timedelta(days=1)
        )
        active_subscription.meta = {
            "_internal": {
                "success_url": "https://example.com/success",
                "cancel_url": "https://example.com/cancel",
                "payment_options": {"client": "web"},
            }
        }
        await session.flush()

        svc = RenewalService(session)
        with patch("gateway.services.renewal.get_adapter", return_value=patch_deps):
            await svc.enforce_grace_periods()

        patch_deps.create_payment.assert_called_once()
        metadata = patch_deps.create_payment.call_args.kwargs["metadata"]
        assert metadata["customer_email"] == test_customer.email
        assert metadata["renewal"] == "true"
        assert metadata["subscription_id"] == str(active_subscription.id)
