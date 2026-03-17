"""
API 请求/响应 Schema（Pydantic v2）
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from gateway.core.constants import (
    Provider,
    Currency,
    PaymentStatus,
    DeliveryStatus,
    RefundStatus,
)


# ===== 通用 =====


class PaymentTypeEnum(str, Enum):
    """统一下单返回类型"""

    redirect = "redirect"
    form = "form"
    qr = "qr"
    client_secret = "client_secret"
    url = "url"


# ===== 创建支付 =====


class CreatePaymentRequest(BaseModel):
    """创建支付请求"""

    merchant_order_no: str = Field(
        ..., min_length=1, max_length=64, description="商户订单号"
    )
    provider: Provider = Field(..., description="支付渠道")
    currency: Currency = Field(..., description="币种")
    quantity: int = Field(..., gt=0, description="商品数量")
    unit_amount: int | None = Field(
        None, gt=0, description="单价（最小货币单位，如分）"
    )
    product_name: str | None = Field(None, max_length=250, description="商品名称")
    product_desc: str | None = Field(None, max_length=500, description="商品描述")
    notify_url: str | None = Field(
        None, max_length=2048, description="本单回调地址（覆盖 App 默认）"
    )
    expire_minutes: int | None = Field(
        None, gt=0, le=1440, description="过期时间（分钟）"
    )
    # 可选参数
    success_url: str | None = Field(
        None, max_length=2048, description="支付成功跳转 URL"
    )
    cancel_url: str | None = Field(
        None, max_length=2048, description="取消支付跳转 URL"
    )
    metadata: dict[str, Any] | None = Field(None, description="额外的元数据")


class CreatePaymentResponse(BaseModel):
    """创建支付响应（混合返回）"""

    payment_id: UUID = Field(..., description="支付 ID")
    merchant_order_no: str = Field(..., description="商户订单号")
    status: PaymentStatus = Field(..., description="支付状态")
    type: PaymentTypeEnum = Field(..., description="返回类型")
    payload: dict[str, Any] = Field(..., description="类型对应的 payload")

    # 示例：
    # type=redirect: payload={\"url\": \"https://...\"}
    # type=form: payload={\"html\": \"<form>...</form>\"}
    # type=qr: payload={\"code_url\": \"weixin://...\"}
    # type=client_secret: payload={\"client_secret\": \"pi_xxx_secret_xxx\"}


# ===== 查询支付 =====


class PaymentResponse(BaseModel):
    """支付详情响应"""

    id: UUID
    app_id: UUID
    merchant_order_no: str
    provider: Provider
    amount: int
    currency: Currency
    status: PaymentStatus
    provider_txn_id: str | None
    notify_url: str | None
    created_at: datetime
    updated_at: datetime
    paid_at: datetime | None

    class Config:
        from_attributes = True  # Pydantic v2: 允许从 ORM 模型创建


# ===== 取消支付 =====


class CancelPaymentRequest(BaseModel):
    """取消支付请求"""

    merchant_order_no: str = Field(
        ..., min_length=1, max_length=64, description="商户订单号"
    )
    payment_id: UUID = Field(..., description="支付 ID")


class CancelPaymentResponse(BaseModel):
    """取消支付响应"""

    payment_id: UUID = Field(..., description="支付 ID")
    merchant_order_no: str = Field(..., description="商户订单号")
    status: PaymentStatus = Field(..., description="支付状态")
    provider_result: dict[str, Any] | None = Field(None, description="支付渠道返回结果")


# ===== 渠道回调（内部处理，无需公开 schema） =====


# 回调由 provider adapter 解析，这里只保留必要的内部事件结构
class CallbackEvent(BaseModel):
    """标准化回调事件（provider adapter 输出）"""

    provider_event_id: str
    provider_txn_id: str | None
    merchant_order_no: str | None
    outcome: str  # "succeeded" | "failed" | "pending" | ...
    raw_payload: dict[str, Any]


# ===== WebhookDelivery 状态查询（可选，供内部调试） =====


class WebhookDeliveryResponse(BaseModel):
    """出站投递任务响应"""

    id: UUID
    event_id: str
    event_type: str
    status: DeliveryStatus
    attempt_count: int
    last_http_status: int | None
    last_error: str | None
    delivered_at: datetime | None
    created_at: datetime

    class Config:
        from_attributes = True


# ===== 应用管理 =====


class CreateAppRequest(BaseModel):
    """创建应用请求"""

    name: str = Field(..., min_length=1, max_length=100, description="应用名称")
    notify_url: str | None = Field(
        None, max_length=2048, description="默认支付结果回调通知地址"
    )


class AppResponse(BaseModel):
    """应用响应"""

    id: UUID
    name: str
    api_key: str
    is_active: bool
    notify_url: str | None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AppListResponse(BaseModel):
    """应用列表响应"""

    total: int = Field(..., description="总数")
    items: list[AppResponse] = Field(..., description="应用列表")


# ===== 退款相关 =====


class CreateRefundRequest(BaseModel):
    """创建退款请求"""

    payment_id: UUID = Field(..., description="支付交易ID")
    refund_amount: int | None = Field(
        None, gt=0, description="退款金额（最小货币单位，如分）。不填则为全额退款"
    )
    reason: str | None = Field(None, max_length=500, description="退款原因")
    notify_url: str | None = Field(
        None, max_length=2048, description="退款结果回调通知地址（覆盖 App 默认）"
    )


class RefundResponse(BaseModel):
    """退款响应"""

    id: UUID
    payment_id: UUID
    refund_amount: int
    reason: str | None
    notify_url: str | None
    status: RefundStatus
    provider: Provider
    provider_refund_id: str | None
    created_at: datetime
    updated_at: datetime
    refunded_at: datetime | None

    class Config:
        from_attributes = True


class RefundListResponse(BaseModel):
    """退款列表响应"""

    total: int = Field(..., description="总数")
    items: list[RefundResponse] = Field(..., description="退款列表")
