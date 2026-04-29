"""
管理 API 路由
"""

import uuid
import structlog
from fastapi import APIRouter, Depends, Query, Body

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..services.apps import AppService
from ..services.refunds import RefundService
from gateway.core.models import Payment
from gateway.core.schemas import (
    CallbackEvent,
    CreateRefundRequest,
    RefundResponse,
    RefundListResponse,
)
from gateway.core.responses import success_response
from gateway.services.callbacks import CallbackService
from gateway.core.constants import PaymentStatus, Provider, EventCategory
from gateway.core.schemas import CreateAppRequest, AppResponse, AppListResponse
from gateway.core.exceptions import (
    NotFoundException,
    BadRequestException,
    InternalServerException,
)

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/apps", status_code=201)
async def create_app(
    req: CreateAppRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    创建应用

    创建一个新的应用并自动生成 API Key。

    - **name**: 应用名称（必填，唯一）
    - **notify_url**: 默认支付结果回调通知地址（可选）

    返回创建的应用信息，包括生成的 API Key。
    """
    log = logger.bind(name=req.name)
    log.info("收到创建应用请求")

    app_service = AppService(session)
    app = await app_service.create_app(req)

    log.info("应用创建成功", app_id=str(app.id))
    response_data = AppResponse.model_validate(app)
    return success_response(
        data=response_data.model_dump(mode="json"), msg="应用创建成功", status_code=201
    )


@router.get("/apps")
async def list_apps(
    skip: int = Query(0, ge=0, description="跳过的记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回的最大记录数"),
    session: AsyncSession = Depends(get_session),
):
    """
    列举所有应用

    返回应用列表，支持分页。

    - **skip**: 跳过的记录数（用于分页，默认 0）
    - **limit**: 返回的最大记录数（默认 100，最大 1000）
    """
    logger.info("收到应用列表请求", skip=skip, limit=limit)

    app_service = AppService(session)
    apps, total = await app_service.list_apps(skip=skip, limit=limit)

    logger.info("应用列表查询成功", total=total, returned=len(apps))
    response_data = AppListResponse(
        total=total, items=[AppResponse.model_validate(app) for app in apps]
    )
    return success_response(data=response_data.model_dump(mode="json"), msg="查询成功")


@router.get("/apps/{app_id}")
async def get_app(
    app_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """
    获取应用详情

    根据应用 ID 获取应用的详细信息。
    """
    logger.info("收到应用详情请求", app_id=str(app_id))

    app_service = AppService(session)
    app = await app_service.get_app_by_id(app_id)

    logger.info("应用详情查询成功", app_id=str(app_id))
    response_data = AppResponse.model_validate(app)
    return success_response(data=response_data.model_dump(mode="json"), msg="查询成功")


@router.delete("/apps/{app_id}")
async def delete_app(
    app_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """
    删除应用

    根据应用 ID 删除应用。注意：删除应用会影响该应用下的所有支付记录。
    """
    logger.info("收到删除应用请求", app_id=str(app_id))

    app_service = AppService(session)
    await app_service.delete_app(app_id)

    logger.info("应用删除成功", app_id=str(app_id))
    return success_response(msg="应用删除成功")


@router.patch("/apps/{app_id}/status")
async def update_app_status(
    app_id: uuid.UUID,
    is_active: bool = Query(..., description="是否启用应用"),
    session: AsyncSession = Depends(get_session),
):
    """
    更新应用状态

    启用或禁用应用。禁用的应用无法进行支付操作。

    - **is_active**: true 启用，false 禁用
    """
    logger.info("收到应用状态更新请求", app_id=str(app_id), is_active=is_active)

    app_service = AppService(session)
    app = await app_service.update_app_status(app_id, is_active)

    logger.info("应用状态更新成功", app_id=str(app_id), is_active=is_active)
    response_data = AppResponse.model_validate(app)
    return success_response(
        data=response_data.model_dump(mode="json"), msg="应用状态更新成功"
    )


@router.post("/payments/{payment_id}/test-success", summary="模拟支付成功")
async def test_payment_success(
    payment_id: uuid.UUID,
    provider: Provider = Body(embed=True),
    session: AsyncSession = Depends(get_session),
):
    """
    测试接口：直接模拟支付成功（仅用于开发测试）

    ### 功能
    - 查找对应的支付记录
    - 模拟 Stripe 支付成功（如果是 Stripe 支付）
    - 构造标准化的回调事件
    - 触发内部 webhook 处理流程
    - 更新支付状态并生成 webhook 投递任务

    ### 注意
    - 此接口仅用于测试环境
    - 生产环境应该删除或禁用
    - 仅支持 Stripe 支付渠道

    ### 返回
    - 支付状态更新结果
    - webhook 投递任务创建状态

    ### 示例
    ```bash
    curl -X POST http://localhost:8000/admin/payments/{payment_id}/test-success
    ```
    """

    log = logger.bind(payment_id=str(payment_id))
    log.info("收到模拟支付成功请求")

    # 1. 查找支付记录
    stmt = select(Payment).where(Payment.id == payment_id)
    result = await session.execute(stmt)
    payment = result.scalar_one_or_none()

    if not payment:
        log.warning("未找到支付记录")
        raise NotFoundException(
            message="支付记录不存在", code=4046, details={"payment_id": str(payment_id)}
        )

    # 2. 检查当前状态
    if payment.status == PaymentStatus.succeeded:
        log.info("支付已为成功状态")
        return success_response(
            data={
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "paid_at": payment.paid_at.isoformat() if payment.paid_at else None,
            },
            msg="支付已经成功",
        )

    # 3. 检查支付渠道
    match provider:
        case Provider.stripe:
            # 4. 在 Stripe 侧真实完成支付（用于测试）
            provider_txn_id = payment.provider_txn_id
            if not provider_txn_id:
                log.warning("支付缺少渠道交易号", payment_id=str(payment_id))
                # 如果没有 provider_txn_id，无法在 Stripe 侧操作
                raise BadRequestException(
                    message="支付记录缺少渠道交易ID，无法在Stripe测试",
                    code=4006,
                    details={"payment_id": str(payment_id)},
                )
            from gateway.providers.stripe import get_stripe_adapter

            await get_stripe_adapter().confirm_payment(payment_id, provider_txn_id)

        case _:
            log.warning("不支持的支付渠道", provider=payment.provider.value)
            raise BadRequestException(
                message=f"测试接口仅支持 Stripe，当前为 {payment.provider.value}",
                code=4007,
                details={"provider": payment.provider.value},
            )

    # 6. 构造标准化的 CallbackEvent（基于真实的 Stripe 状态）
    # 参考：https://docs.stripe.com/webhooks
    event_id = f"evt_test_{payment_id}_{int(__import__('time').time())}"
    callback_event = CallbackEvent(
        provider=provider,
        provider_event_id=event_id,
        provider_txn_id=provider_txn_id,
        merchant_order_no=payment.merchant_order_no,
        outcome="succeeded",
        event_category=EventCategory.payment,
        app_id=payment.app_id,
        raw_payload={
            "id": event_id,
            "object": "event",
            "type": "payment_intent.succeeded",
            "provider": "stripe",
            "data": {
                "object": {
                    "id": provider_txn_id,
                    "object": "payment_intent",
                    "amount": payment.amount,
                    "currency": payment.currency.value,
                    "status": "succeeded",
                    "metadata": {
                        "merchant_order_no": payment.merchant_order_no,
                    },
                }
            },
        },
    )

    # 7. 调用 CallbackService 处理回调（触发状态更新和 webhook 投递）
    callback_service = CallbackService(session)
    try:
        await callback_service.process_callback(callback_event)
        log.info("回调处理成功")
    except Exception as e:
        log.error("回调处理失败", error=str(e))
        raise InternalServerException(
            message="回调处理失败", code=5003, details={"error": str(e)}
        )

    # 8. 刷新支付记录以获取最新状态
    await session.refresh(payment)

    log.info(
        "模拟支付成功流程完成",
        new_status=payment.status.value,
        paid_at=payment.paid_at.isoformat() if payment.paid_at else None,
    )

    return success_response(
        data={
            "payment_id": str(payment.id),
            "merchant_order_no": payment.merchant_order_no,
            "status": payment.status.value,
            "amount": payment.amount,
            "currency": payment.currency.value,
            "provider_txn_id": payment.provider_txn_id,
            "paid_at": payment.paid_at.isoformat() if payment.paid_at else None,
            "test_mode": True,
        },
        msg="支付测试成功",
    )


# ===== 退款管理 API =====


@router.post("/refunds", status_code=201, summary="创建退款")
async def create_refund(
    req: CreateRefundRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    创建退款（支持全额退款和部分退款）
    
    ### 功能
    - 创建退款请求
    - 支持全额退款（不传 refund_amount）和部分退款
    - 验证支付状态和退款金额
    - 调用支付渠道退款接口
    - 记录退款结果
    
    ### 业务规则
    1. 支付必须是成功状态（succeeded）
    2. 如果不指定退款金额，则为全额退款
    3. 部分退款时，累计退款金额不能超过支付金额
    4. 支持多次部分退款
    
    ### 参数
    - **payment_id**: 支付交易ID（必填）
    - **refund_amount**: 退款金额（最小货币单位，如分）。不填则为全额退款
    - **reason**: 退款原因（可选）
    
    ### 返回
    - 退款记录详情，包括退款ID、状态、金额等
    
    ### 示例
    ```bash
    # 全额退款
    curl -X POST http://localhost:8000/admin/refunds \\
      -H "Content-Type: application/json" \\
      -d '{
        "payment_id": "550e8400-e29b-41d4-a716-446655440000",
        "reason": "Customer requested refund"
      }'
    
    # 部分退款（退款 500 分 = 5 USD）
    curl -X POST http://localhost:8000/admin/refunds \\
      -H "Content-Type: application/json" \\
      -d '{
        "payment_id": "550e8400-e29b-41d4-a716-446655440000",
        "refund_amount": 500,
        "reason": "Partial refund"
      }'
    ```
    """
    log = logger.bind(payment_id=str(req.payment_id))
    log.info("收到创建退款请求")

    refund_service = RefundService(session)
    refund = await refund_service.create_refund(req)

    log.info("退款创建成功", refund_id=str(refund.id))
    response_data = RefundResponse.model_validate(refund)
    return success_response(
        data=response_data.model_dump(mode="json"), msg="退款创建成功", status_code=201
    )


@router.get("/refunds/{refund_id}", summary="查询退款详情")
async def get_refund(
    refund_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """
    查询退款详情

    根据退款ID获取退款的详细信息。

    ### 参数
    - **refund_id**: 退款ID

    ### 返回
    - 退款记录详情
    """
    logger.info("收到退款详情查询请求", refund_id=str(refund_id))

    refund_service = RefundService(session)
    refund = await refund_service.get_refund(refund_id)

    logger.info("退款详情查询成功", refund_id=str(refund_id))
    response_data = RefundResponse.model_validate(refund)
    return success_response(data=response_data.model_dump(mode="json"), msg="查询成功")


@router.get(
    "/payments/{payment_id}/refunds",
    summary="查询支付的退款记录",
)
async def list_refunds_by_payment(
    payment_id: uuid.UUID,
    skip: int = Query(0, ge=0, description="跳过的记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回的最大记录数"),
    session: AsyncSession = Depends(get_session),
):
    """
    查询支付的所有退款记录

    返回指定支付的所有退款记录，支持分页。

    ### 参数
    - **payment_id**: 支付交易ID
    - **skip**: 跳过的记录数（用于分页，默认 0）
    - **limit**: 返回的最大记录数（默认 100，最大 1000）

    ### 返回
    - 退款记录列表和总数
    """
    logger.info(
        "收到退款列表查询请求", payment_id=str(payment_id), skip=skip, limit=limit
    )

    refund_service = RefundService(session)
    refunds, total = await refund_service.list_refunds_by_payment(
        payment_id=payment_id,
        skip=skip,
        limit=limit,
    )

    logger.info("退款列表查询成功", total=total, returned=len(refunds))
    response_data = RefundListResponse(
        total=total, items=[RefundResponse.model_validate(refund) for refund in refunds]
    )
    return success_response(data=response_data.model_dump(mode="json"), msg="查询成功")


@router.post("/refunds/{refund_id}/sync", summary="同步退款状态")
async def sync_refund_status(
    refund_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """
    同步退款状态（从支付渠道）

    从支付渠道查询最新的退款状态并更新本地记录。

    ### 功能
    - 查询支付渠道的退款状态
    - 更新本地退款记录
    - 如果状态已经是最终状态，则跳过同步

    ### 参数
    - **refund_id**: 退款ID

    ### 返回
    - 更新后的退款记录详情

    ### 示例
    ```bash
    curl -X POST http://localhost:8000/admin/refunds/{refund_id}/sync
    ```
    """
    logger.info("收到退款状态同步请求", refund_id=str(refund_id))

    refund_service = RefundService(session)
    refund = await refund_service.sync_refund_status(refund_id)

    logger.info(
        "退款状态同步完成",
        refund_id=str(refund_id),
        status=refund.status.value,
    )
    response_data = RefundResponse.model_validate(refund)
    return success_response(
        data=response_data.model_dump(mode="json"), msg="状态同步成功"
    )
