"""
计划管理服务（Plan CRUD + Stripe Product/Price 同步）
"""

import uuid

import structlog
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.models import Plan
from gateway.core.constants import Provider
from gateway.core.settings import get_settings
from gateway.core.exceptions import (
    NotFoundException,
    BadRequestException,
    ConflictException,
)
from gateway.core.schemas import CreatePlanRequest, UpdatePlanRequest
from gateway.providers import get_adapter, is_provider_allowed

logger = structlog.get_logger(__name__)
settings = get_settings()


class PlanService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_plan(self, app_id: uuid.UUID, req: CreatePlanRequest) -> Plan:
        log = logger.bind(app_id=str(app_id), slug=req.slug, provider=req.provider.value)

        if not is_provider_allowed(req.provider):
            raise BadRequestException(
                message=f"支付渠道 {req.provider.value} 未启用", code=4004
            )

        existing_stmt = select(Plan).where(
            Plan.app_id == app_id,
            Plan.slug == req.slug,
            Plan.provider == req.provider,
        )
        existing = (await self.session.execute(existing_stmt)).scalar_one_or_none()
        if existing:
            raise ConflictException(
                message=f"计划 slug '{req.slug}' 在该渠道下已存在", code=4096
            )

        adapter = get_adapter(req.provider)
        from gateway.providers.base import SubscriptionProviderMixin

        if not isinstance(adapter, SubscriptionProviderMixin):
            raise BadRequestException(
                message=f"渠道 {req.provider.value} 不支持订阅功能", code=4007
            )

        product_id, price_id = await adapter.create_product_and_price(
            name=req.name,
            amount=req.amount,
            currency=req.currency.value,
            interval=req.interval.value,
            interval_count=req.interval_count,
        )

        plan = Plan(
            id=uuid.uuid4(),
            app_id=app_id,
            provider=req.provider,
            slug=req.slug,
            name=req.name,
            description=req.description,
            amount=req.amount,
            currency=req.currency,
            interval=req.interval.value,
            interval_count=req.interval_count,
            provider_product_id=product_id,
            provider_price_id=price_id,
            tier=req.tier,
            features=req.features,
            is_active=True,
        )

        self.session.add(plan)
        await self.session.flush()
        log.info("计划创建完成", plan_id=str(plan.id))
        return plan

    async def get_plan(self, app_id: uuid.UUID, plan_id: uuid.UUID) -> Plan:
        stmt = select(Plan).where(Plan.id == plan_id, Plan.app_id == app_id)
        result = await self.session.execute(stmt)
        plan = result.scalar_one_or_none()
        if not plan:
            raise NotFoundException(message="计划不存在", code=4048)
        return plan

    async def list_plans(
        self,
        app_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Plan], int]:
        count_stmt = (
            select(func.count()).select_from(Plan).where(Plan.app_id == app_id)
        )
        total = (await self.session.execute(count_stmt)).scalar_one()

        stmt = (
            select(Plan)
            .where(Plan.app_id == app_id)
            .order_by(Plan.tier.asc(), Plan.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await self.session.execute(stmt)
        plans = list(result.scalars().all())
        return plans, total

    async def update_plan(
        self, app_id: uuid.UUID, plan_id: uuid.UUID, req: UpdatePlanRequest
    ) -> Plan:
        plan = await self.get_plan(app_id, plan_id)
        log = logger.bind(plan_id=str(plan_id))

        if req.name is not None:
            plan.name = req.name
        if req.description is not None:
            plan.description = req.description
        if req.tier is not None:
            plan.tier = req.tier
        if req.features is not None:
            plan.features = req.features
        if req.is_active is not None:
            plan.is_active = req.is_active

        if req.amount is not None and req.amount != plan.amount:
            adapter = get_adapter(plan.provider)
            from gateway.providers.base import SubscriptionProviderMixin

            if not isinstance(adapter, SubscriptionProviderMixin):
                raise BadRequestException(
                    message=f"渠道 {plan.provider.value} 不支持订阅功能", code=4007
                )

            if not plan.provider_product_id:
                raise BadRequestException(
                    message="计划缺少渠道商品ID，无法创建新价格", code=4008
                )

            old_price_id = plan.provider_price_id

            new_price_id = await adapter.create_price(
                product_id=plan.provider_product_id,
                amount=req.amount,
                currency=plan.currency.value,
                interval=plan.interval,
                interval_count=plan.interval_count,
            )

            if old_price_id:
                try:
                    await adapter.archive_price(old_price_id)
                except Exception as e:
                    log.warning("归档旧 Price 失败", error=str(e))

            plan.amount = req.amount
            plan.provider_price_id = new_price_id
            log.info("Price 版本已更新", new_price_id=new_price_id)

        await self.session.flush()
        log.info("计划更新完成")
        return plan

    async def deactivate_plan(self, app_id: uuid.UUID, plan_id: uuid.UUID) -> Plan:
        plan = await self.get_plan(app_id, plan_id)
        plan.is_active = False
        await self.session.flush()
        logger.info("计划已停用", plan_id=str(plan_id))
        return plan
