"""
计划管理 API 路由
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.db import get_session
from gateway.core.auth import get_app_from_api_key
from gateway.core.models import App
from gateway.core.schemas import (
    CreatePlanRequest,
    UpdatePlanRequest,
    PlanResponse,
    PlanListResponse,
)
from gateway.core.responses import success_response
from gateway.services.plans import PlanService

router = APIRouter()


@router.post("/plans", summary="创建计划")
async def create_plan(
    req: CreatePlanRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = PlanService(session)
    plan = await svc.create_plan(app.id, req)
    return success_response(
        data=PlanResponse.model_validate(plan).model_dump(mode="json"),
        status_code=201,
    )


@router.get("/plans", summary="查询计划列表")
async def list_plans(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = PlanService(session)
    plans, total = await svc.list_plans(app.id, page=page, page_size=page_size)
    return success_response(
        data=PlanListResponse(
            total=total,
            items=[PlanResponse.model_validate(p) for p in plans],
        ).model_dump(mode="json")
    )


@router.get("/plans/{plan_id}", summary="查询计划详情")
async def get_plan(
    plan_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = PlanService(session)
    plan = await svc.get_plan(app.id, plan_id)
    return success_response(
        data=PlanResponse.model_validate(plan).model_dump(mode="json")
    )


@router.put("/plans/{plan_id}", summary="更新计划")
async def update_plan(
    plan_id: UUID,
    req: UpdatePlanRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = PlanService(session)
    plan = await svc.update_plan(app.id, plan_id, req)
    return success_response(
        data=PlanResponse.model_validate(plan).model_dump(mode="json")
    )


@router.delete("/plans/{plan_id}", summary="停用计划")
async def deactivate_plan(
    plan_id: UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    svc = PlanService(session)
    plan = await svc.deactivate_plan(app.id, plan_id)
    return success_response(
        data=PlanResponse.model_validate(plan).model_dump(mode="json")
    )
