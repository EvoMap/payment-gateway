"""
API 请求/响应 Schema（Pydantic v2）
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gateway.core.constants import (
    Provider,
    Currency,
    PaymentMethod,
    ALIPAY_SUPPORTED_CURRENCIES,
    WECHAT_PAY_SUPPORTED_CURRENCIES,
    PaymentStatus,
    DeliveryStatus,
    RefundStatus,
    EventCategory,
    BillingInterval,
    SubscriptionStatus,
    ProrationMode,
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
    success_url: str | None = Field(
        None, max_length=2048, description="支付成功跳转 URL"
    )
    cancel_url: str | None = Field(
        None, max_length=2048, description="取消支付跳转 URL"
    )
    metadata: dict[str, Any] | None = Field(None, description="额外的元数据")
    external_user_id: str | None = Field(
        None,
        max_length=128,
        description="调用方用户标识（可选，填写后启用 pending 并发控制）",
    )
    payment_method: PaymentMethod | None = Field(
        None,
        description="支付方式（card / wechat_pay / alipay）。不填默认 card",
    )
    payment_options: dict[str, Any] | None = Field(
        None,
        description="支付方式附加选项（如 wechat_pay 需要 {\"client\": \"web\"}）",
    )

    @model_validator(mode="after")
    def validate_payment_options(self) -> "CreatePaymentRequest":
        if self.payment_options and self.payment_method is None:
            raise ValueError(
                "传入 payment_options 时必须同时指定 payment_method"
            )
        if self.payment_method == PaymentMethod.wechat_pay:
            opts = self.payment_options or {}
            allowed_keys = {"client"}
            extra = set(opts.keys()) - allowed_keys
            if extra:
                raise ValueError(
                    f"wechat_pay payment_options 仅支持 {allowed_keys}，"
                    f"多余字段: {extra}"
                )
            client = opts.get("client")
            if client is not None and client not in ("web", "ios", "android"):
                raise ValueError(
                    "payment_options.client 值必须为 web / ios / android 之一"
                )
            if self.currency.value not in WECHAT_PAY_SUPPORTED_CURRENCIES:
                supported = sorted(WECHAT_PAY_SUPPORTED_CURRENCIES)
                raise ValueError(
                    f"wechat_pay 仅支持 {supported} 币种，"
                    f"当前: {self.currency.value}"
                )
        elif self.payment_method == PaymentMethod.alipay:
            if self.payment_options:
                raise ValueError("alipay 不需要 payment_options")
            if self.currency.value not in ALIPAY_SUPPORTED_CURRENCIES:
                supported = sorted(ALIPAY_SUPPORTED_CURRENCIES)
                raise ValueError(
                    f"alipay 仅支持 {supported} 币种，"
                    f"当前: {self.currency.value}"
                )
        elif self.payment_method == PaymentMethod.card:
            if self.payment_options:
                raise ValueError("card 不需要 payment_options")
        return self


class CreatePaymentResponse(BaseModel):
    """创建支付响应（混合返回）"""

    payment_id: UUID = Field(..., description="支付 ID")
    merchant_order_no: str = Field(..., description="商户订单号")
    status: PaymentStatus = Field(..., description="支付状态")
    type: PaymentTypeEnum = Field(..., description="返回类型")
    payload: dict[str, Any] = Field(..., description="类型对应的 payload")


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

    model_config = ConfigDict(from_attributes=True)


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


# ===== 渠道回调 =====


class CallbackEvent(BaseModel):
    """标准化回调事件（provider adapter 输出）"""

    provider: Provider
    provider_event_id: str
    provider_txn_id: str | None
    merchant_order_no: str | None
    outcome: str
    event_category: EventCategory | None = None
    app_id: UUID | None = None
    subscription_id: str | None = None
    checkout_session_id: str | None = None
    gateway_subscription_id: UUID | None = None
    invoice_id: str | None = None
    raw_payload: dict[str, Any]


# ===== WebhookDelivery =====


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

    model_config = ConfigDict(from_attributes=True)


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

    model_config = ConfigDict(from_attributes=True)


class AppListResponse(BaseModel):
    """应用列表响应"""

    total: int = Field(..., description="总数")
    items: list[AppResponse] = Field(..., description="应用列表")


# ===== 退款相关 =====


class CreateRefundRequest(BaseModel):
    """创建退款请求"""

    payment_id: UUID = Field(..., description="支付交易ID")
    refund_amount: int | None = Field(
        None, gt=0, description="退款金额（最小货币单位）。不填则为全额退款"
    )
    reason: str | None = Field(None, max_length=500, description="退款原因")
    notify_url: str | None = Field(
        None, max_length=2048, description="退款结果回调通知地址"
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

    model_config = ConfigDict(from_attributes=True)


class RefundListResponse(BaseModel):
    """退款列表响应"""

    total: int = Field(..., description="总数")
    items: list[RefundResponse] = Field(..., description="退款列表")


# ===== 计划相关 =====


class CreatePlanRequest(BaseModel):
    provider: Provider = Field(..., description="支付渠道")
    slug: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=100)
    description: str | None = None
    amount: int = Field(..., ge=0)
    currency: Currency
    interval: BillingInterval
    interval_count: int = Field(default=1, ge=1)
    tier: int = Field(default=0)
    features: dict | None = None


class UpdatePlanRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    amount: int | None = Field(None, gt=0)
    tier: int | None = None
    features: dict | None = None
    is_active: bool | None = None


class PlanResponse(BaseModel):
    id: UUID
    slug: str
    name: str
    description: str | None
    amount: int
    currency: Currency
    interval: BillingInterval
    interval_count: int
    tier: int
    features: dict | None
    is_active: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PlanListResponse(BaseModel):
    total: int
    items: list[PlanResponse]


# ===== 订阅相关 =====


class CreateSubscriptionRequest(BaseModel):
    external_user_id: str = Field(..., min_length=1, max_length=128)
    plan_id: UUID
    email: str | None = None
    success_url: str = Field(..., max_length=2048)
    cancel_url: str = Field(..., max_length=2048)
    notify_url: str | None = Field(None, max_length=2048)
    trial_period_days: int | None = Field(None, ge=1, le=365)
    metadata: dict | None = None
    force_cleanup: bool = False
    payment_method: PaymentMethod = Field(
        default=PaymentMethod.card,
        description="支付方式。默认 card（走 Stripe 订阅），wechat_pay/alipay 走应用层续费",
    )
    payment_options: dict[str, Any] | None = Field(
        None,
        description="支付方式选项（如 wechat_pay 需 {\"client\": \"web\"}）",
    )

    @model_validator(mode="after")
    def validate_reserved_metadata(self) -> "CreateSubscriptionRequest":
        if self.metadata and "_internal" in self.metadata:
            raise ValueError("metadata top-level key '_internal' is reserved for system use and cannot be set by the caller")
        return self

    @model_validator(mode="after")
    def validate_subscription_payment_options(self) -> "CreateSubscriptionRequest":
        if self.payment_options and self.payment_method == PaymentMethod.card:
            raise ValueError("payment_options requires payment_method to be wechat_pay or alipay")
        if self.payment_method == PaymentMethod.wechat_pay:
            opts = self.payment_options or {}
            allowed_keys = {"client"}
            extra = set(opts.keys()) - allowed_keys
            if extra:
                raise ValueError(
                    f"wechat_pay payment_options only supports {allowed_keys}, unexpected keys: {extra}"
                )
            client = opts.get("client")
            if client is not None:
                if not isinstance(client, str) or client not in ("web", "ios", "android"):
                    raise ValueError("payment_options.client must be a string, one of: web, ios, android")
        elif self.payment_method == PaymentMethod.alipay:
            if self.payment_options:
                raise ValueError("alipay does not accept payment_options")
        return self


class CreateSubscriptionResponse(BaseModel):
    subscription_id: UUID
    checkout_url: str
    status: SubscriptionStatus


class SubscriptionResponse(BaseModel):
    id: UUID
    external_user_id: str
    plan: PlanResponse
    pending_plan: PlanResponse | None = None
    pending_plan_change_at: datetime | None = None
    amount: int
    currency: Currency
    status: SubscriptionStatus
    payment_method: PaymentMethod
    renewal_payment_id: UUID | None = None
    grace_period_end: datetime | None = None
    current_period_start: datetime | None
    current_period_end: datetime | None
    cancel_at_period_end: bool
    canceled_at: datetime | None
    trial_start: datetime | None
    trial_end: datetime | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SubscriptionListResponse(BaseModel):
    total: int
    items: list[SubscriptionResponse]


class CancelSubscriptionRequest(BaseModel):
    immediate: bool = Field(default=False, description="是否立即取消")


class ChangePlanRequest(BaseModel):
    new_plan_id: UUID
    proration_mode: ProrationMode | None = None
    credit_amount: int | None = None


class ChangePlanResponse(BaseModel):
    subscription_id: UUID
    direction: str
    effective: str
    current_plan: PlanResponse
    pending_plan: PlanResponse | None
    pending_plan_change_at: datetime | None
    current_period_end: datetime | None = None
    status: str


class PreviewChangePlanRequest(BaseModel):
    new_plan_id: UUID


class PreviewChangePlanLineItem(BaseModel):
    amount: int = Field(..., description="金额（最小货币单位，如分）")
    description: str = Field(default="", description="行项目描述")


class PreviewChangePlanResponse(BaseModel):
    currency: str
    total: int = Field(..., description="总金额（最小货币单位）")
    lines: list[PreviewChangePlanLineItem] = Field(default_factory=list, description="发票行项目明细")
