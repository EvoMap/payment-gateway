"""
支付相关 API 路由（适配 app_id metadata 注入）
"""

import uuid

import structlog
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.db import get_session
from gateway.core.auth import get_app_from_api_key
from gateway.core.models import App, Payment
from gateway.core.constants import PaymentStatus
from gateway.core.exceptions import (
    BadRequestException,
    NotFoundException,
)
from gateway.core.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    CancelPaymentRequest,
    CancelPaymentResponse,
    PaymentResponse,
)
from gateway.core.responses import success_response
from gateway.services.payments import PaymentService
from gateway.providers import get_adapter

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/payments", summary="创建支付")
async def create_payment(
    req: CreatePaymentRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    log = logger.bind(
        app_id=str(app.id),
        merchant_order_no=req.merchant_order_no,
        provider=req.provider.value,
    )
    log.info("收到创建支付请求")

    payment_svc = PaymentService(session)
    payment, is_new = await payment_svc.create_or_get_payment(app, req)

    if not is_new and payment.status != PaymentStatus.pending:
        return success_response(
            data=CreatePaymentResponse(
                payment_id=payment.id,
                merchant_order_no=payment.merchant_order_no,
                status=payment.status,
                type="url",
                payload={"message": "支付已完成或已取消"},
            ).model_dump(mode="json")
        )

    adapter = get_adapter(req.provider)

    from gateway.core.settings import get_settings

    settings = get_settings()

    result = await adapter.create_payment(
        currency=req.currency.value,
        merchant_order_no=req.merchant_order_no,
        quantity=req.quantity,
        notify_url=payment.notify_url or "",
        expire_minutes=req.expire_minutes or settings.payment_expire_minutes_default,
        unit_amount=req.unit_amount,
        product_name=req.product_name,
        product_desc=req.product_desc,
        success_url=req.success_url,
        cancel_url=req.cancel_url,
        metadata={**(req.metadata or {}), "app_id": str(app.id)},
        app_id=str(app.id),
    )

    if result.provider_txn_id and not payment.provider_txn_id:
        payment.provider_txn_id = result.provider_txn_id

    log.info(
        "支付创建完成",
        payment_id=str(payment.id),
        type=result.type.value,
        provider_txn_id=result.provider_txn_id,
    )

    return success_response(
        data=CreatePaymentResponse(
            payment_id=payment.id,
            merchant_order_no=payment.merchant_order_no,
            status=payment.status,
            type=result.type,
            payload=result.payload,
        ).model_dump(mode="json"),
        status_code=201,
    )


@router.get("/payments/{payment_id}", summary="查询支付详情")
async def get_payment(
    payment_id: uuid.UUID,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    payment_svc = PaymentService(session)
    payment = await payment_svc.get_payment_by_id(app, payment_id)

    return success_response(
        data=PaymentResponse.model_validate(payment).model_dump(mode="json")
    )


@router.get("/payments", summary="查询支付列表")
async def list_payments(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status: PaymentStatus | None = None,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    count_stmt = (
        select(func.count())
        .select_from(Payment)
        .where(Payment.app_id == app.id)
    )
    if status:
        count_stmt = count_stmt.where(Payment.status == status)
    total = (await session.execute(count_stmt)).scalar_one()

    stmt = (
        select(Payment)
        .where(Payment.app_id == app.id)
        .order_by(Payment.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    if status:
        stmt = stmt.where(Payment.status == status)

    result = await session.execute(stmt)
    payments = list(result.scalars().all())

    return success_response(
        data={
            "total": total,
            "items": [
                PaymentResponse.model_validate(p).model_dump(mode="json")
                for p in payments
            ],
        }
    )


@router.post("/payments/cancel", summary="取消支付")
async def cancel_payment(
    req: CancelPaymentRequest,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    log = logger.bind(
        app_id=str(app.id),
        payment_id=str(req.payment_id),
        merchant_order_no=req.merchant_order_no,
    )
    log.info("收到取消支付请求")

    payment_svc = PaymentService(session)
    payment = await payment_svc.get_payment_by_id(app, req.payment_id)

    if payment.merchant_order_no != req.merchant_order_no:
        raise BadRequestException(
            message="订单号不匹配",
            code=4002,
        )

    if payment.status != PaymentStatus.pending:
        raise BadRequestException(
            message=f"只有待支付状态可以取消，当前状态: {payment.status.value}",
            code=4003,
        )

    adapter = get_adapter(payment.provider)

    cancel_result = await adapter.cancel_payment(
        merchant_order_no=payment.merchant_order_no,
        provider_txn_id=payment.provider_txn_id,
    )

    if cancel_result.get("success"):
        payment.status = PaymentStatus.canceled

    return success_response(
        data=CancelPaymentResponse(
            payment_id=payment.id,
            merchant_order_no=payment.merchant_order_no,
            status=payment.status,
            provider_result=cancel_result,
        ).model_dump(mode="json")
    )


@router.get(
    "/payments/order/{merchant_order_no}",
    summary="通过商户订单号查询支付",
)
async def get_payment_by_order_no(
    merchant_order_no: str,
    app: App = Depends(get_app_from_api_key),
    session: AsyncSession = Depends(get_session),
):
    payment_svc = PaymentService(session)
    payment = await payment_svc.get_payment_by_merchant_order_no(
        app, merchant_order_no
    )

    return success_response(
        data=PaymentResponse.model_validate(payment).model_dump(mode="json")
    )
