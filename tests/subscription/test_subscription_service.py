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

from gateway.core.constants import SubscriptionStatus
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

    async def test_create_duplicate_incomplete_rejected(
        self, session, test_app, basic_plan, incomplete_subscription, patch_deps
    ):
        """已有 incomplete 订阅时不允许再次创建。"""
        svc = SubscriptionService(session)
        req = CreateSubscriptionRequest(
            external_user_id=incomplete_subscription.customer.external_user_id
            if hasattr(incomplete_subscription, "customer")
            else "test_user_001",
            plan_id=basic_plan.id,
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
        )

        with pytest.raises(ConflictException, match="未完成的订阅"):
            await svc.create_subscription(test_app, req)

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


from gateway.core.constants import Currency, Provider  # noqa: E402
from gateway.core.models import Customer, Plan, Subscription  # noqa: E402
