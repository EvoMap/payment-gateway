"""
订阅管理 API 路由
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from gateway.db import get_session
from gateway.core.auth import get_app_from_api_key
from gateway.core.models import App, Customer, Subscription
from gateway.core.constants import SubscriptionStatus
from gateway.core.schemas import (
    CreateSubscriptionRequest,
    CreateSubscriptionResponse,
    SubscriptionResponse,
    SubscriptionListResponse,
    CancelSubscriptionRequest,
    ChangePlanRequest,
    ChangePlanResponse,
    PreviewChangePlanRequest,
    PreviewChangePlanResponse,
    PlanResponse,
)
from gateway.core.responses import success_response
from gateway.core.exceptions import NotFoundException
from gateway.services.subscriptions import SubscriptionService

router = APIRouter()


async def _build_subscription_response(
    sub: Subscription, session: AsyncSession
) -> dict:
    """构建 SubscriptionResponse 所需数据"""
    from sqlalchemy import select
    from gateway.core.models import Plan, Customer as CustModel

    plan = (
        await session.execute(select(Plan).where(Plan.id == sub.plan_id))
    ).scalar_one_or_none()

    pending_plan = None
    if sub.pending_plan_id:
        pending_plan = (
            await session.execute(
                select(Plan).where(Plan.id == sub.pending_plan_id)
            )
        ).scalar_one_or_none()

    customer = (
        await session.execute(
            select(CustModel).where(CustModel.id == sub.customer_id)
        )
    ).scalar_one_or_none()

    return SubscriptionResponse(
        id=sub.id,
        external_user_id=customer.external_user_id if customer else "",
        plan=PlanResponse.model_validate(plan) if plan else None,
        pending_plan=(
            PlanResponse.model_validate(pending_plan) if pending_plan else None
        ),
        pending_plan_change_at=sub.pending_plan_change_at,
        amount=sub.amount,
        currency=sub.currency,
        status=SubscriptionStatus(sub.status),
        current_period_start=sub.current_period_start,
        current_period_end=sub.current_period_end,
        cancel_at_period_end=sub.cancel_at_period_end,
        canceled_at=sub.canceled_at,
        trial_start=sub.trial_start,
        trial_end=sub.trial_end,
        created_at=sub.created_at,
    ).model_dump(mode="json")


@router.post("/subscriptions", summary="创建订阅")
async def create_subscription(
    req: CreateSubscriptionRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    subscription, checkout_url = await svc.create_subscription(app, req)
    return success_response(
        data=CreateSubscriptionResponse(
            subscription_id=subscription.id,
            checkout_url=checkout_url,
            status=SubscriptionStatus(subscription.status),
        ).model_dump(mode="json"),
        status_code=201,
    )


@router.get("/subscriptions", summary="查询订阅列表")
async def list_subscriptions(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    external_user_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    subs, total = await svc.list_subscriptions(
        app.id,
        page=page,
        page_size=page_size,
        external_user_id=external_user_id,
        status=status,
    )
    items = [await _build_subscription_response(s, session) for s in subs]
    return success_response(
        data={"total": total, "items": items}
    )


@router.get(
    "/subscriptions/user/{external_user_id}",
    summary="查询用户当前活跃订阅",
)
async def get_user_subscription(
    external_user_id: str,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.get_user_active_subscription(app.id, external_user_id)
    if not sub:
        raise NotFoundException(message="该用户无活跃订阅", code=4050)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.get(
    "/subscriptions/{subscription_id}", summary="查询订阅详情"
)
async def get_subscription(
    subscription_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.get_subscription(app.id, subscription_id)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.post(
    "/subscriptions/{subscription_id}/cancel", summary="取消订阅"
)
async def cancel_subscription(
    subscription_id: UUID,
    req: CancelSubscriptionRequest = CancelSubscriptionRequest(),
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.cancel_subscription(app.id, subscription_id, req)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.post(
    "/subscriptions/{subscription_id}/resume", summary="恢复订阅"
)
async def resume_subscription(
    subscription_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.resume_subscription(app.id, subscription_id)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.post(
    "/subscriptions/{subscription_id}/pause", summary="暂停订阅"
)
async def pause_subscription(
    subscription_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.pause_subscription(app.id, subscription_id)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.post(
    "/subscriptions/{subscription_id}/unpause",
    summary="恢复暂停的订阅",
)
async def unpause_subscription(
    subscription_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.unpause_subscription(app.id, subscription_id)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)


@router.post(
    "/subscriptions/{subscription_id}/change-plan",
    summary="升降级（变更计划）",
)
async def change_plan(
    subscription_id: UUID,
    req: ChangePlanRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    result = await svc.change_plan(app.id, subscription_id, req)

    current_plan = result["current_plan"]
    pending_plan = result["pending_plan"]

    return success_response(
        data=ChangePlanResponse(
            subscription_id=subscription_id,
            direction=result["direction"],
            effective=result["effective"],
            current_plan=PlanResponse.model_validate(current_plan),
            pending_plan=(
                PlanResponse.model_validate(pending_plan)
                if pending_plan
                else None
            ),
            pending_plan_change_at=result["pending_plan_change_at"],
            current_period_end=result.get("current_period_end"),
            status=result.get("status", "active"),
        ).model_dump(mode="json")
    )



@router.post(
    "/subscriptions/{subscription_id}/preview-change",
    summary="预览变更计划费用",
)
async def preview_change(
    subscription_id: UUID,
    req: PreviewChangePlanRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    result = await svc.preview_change(app.id, subscription_id, req.new_plan_id)
    return success_response(
        data=PreviewChangePlanResponse(**result).model_dump(mode="json")
    )

@router.post(
    "/subscriptions/{subscription_id}/cancel-pending-change",
    summary="取消待生效的降级",
)
async def cancel_pending_change(
    subscription_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = SubscriptionService(session)
    sub = await svc.cancel_pending_downgrade(app.id, subscription_id)
    data = await _build_subscription_response(sub, session)
    return success_response(data=data)
