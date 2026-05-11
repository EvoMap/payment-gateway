"""
Provider Adapter 基类 + 订阅 Mixin 接口
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from enum import Enum

from pydantic import BaseModel

from gateway.core.constants import Provider
from gateway.core.schemas import PaymentTypeEnum, CallbackEvent


class PaymentFlowType(str, Enum):
    """支付流程类型"""

    HOSTED = "hosted"


class ProviderPaymentResult(BaseModel):
    """Provider 下单结果"""

    type: PaymentTypeEnum
    payload: dict[str, Any]
    provider_txn_id: str | None = None


class SubscriptionCheckoutResult(BaseModel):
    """订阅 Checkout 创建结果"""

    session_id: str
    checkout_url: str


class SubscriptionActionResult(BaseModel):
    """订阅操作结果（取消/恢复/升降级）"""

    subscription_id: str
    status: str
    current_period_end: datetime | None = None
    cancel_at_period_end: bool = False


class ProviderAdapter(ABC):
    """支付渠道适配器基类"""

    @property
    @abstractmethod
    def provider(self) -> Provider:
        pass

    @property
    def supported_flows(self) -> list[PaymentFlowType]:
        return [PaymentFlowType.HOSTED]

    @abstractmethod
    async def create_payment(
        self,
        *,
        currency: str,
        merchant_order_no: str,
        quantity: int,
        notify_url: str,
        expire_minutes: int | None = None,
        unit_amount: int | None = None,
        product_name: str | None = None,
        product_desc: str | None = None,
        **kwargs,
    ) -> ProviderPaymentResult:
        pass

    @abstractmethod
    async def create_refund(
        self,
        *,
        txn_id: str,
        merchant_order_no: str,
        refund_amount: int | None = None,
        reason: str | None = None,
    ) -> dict:
        pass

    @abstractmethod
    async def parse_and_verify_callback(
        self,
        headers: dict[str, str],
        body: bytes,
    ) -> CallbackEvent:
        pass

    @abstractmethod
    async def cancel_payment(
        self,
        *,
        merchant_order_no: str,
        provider_txn_id: str | None = None,
    ) -> dict[str, Any]:
        pass

    async def query_payment(self, provider_txn_id: str) -> dict[str, Any]:
        raise NotImplementedError(f"{self.provider} does not implement query_payment")


class SubscriptionProviderMixin(ABC):
    """订阅功能 Provider 接口（Mixin，按需实现）"""

    @abstractmethod
    async def create_customer(
        self,
        *,
        email: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """创建渠道侧 Customer，返回 customer_id"""

    async def update_customer_email(
        self,
        provider_customer_id: str,
        email: str,
    ) -> None:
        """更新渠道侧 Customer email；可选实现，默认未实现。"""
        raise NotImplementedError(
            f"{self.provider} does not implement update_customer_email"
        )

    @abstractmethod
    async def create_product_and_price(
        self,
        *,
        name: str,
        amount: int,
        currency: str,
        interval: str,
        interval_count: int,
    ) -> tuple[str, str]:
        """创建 Product + Price，返回 (product_id, price_id)"""

    @abstractmethod
    async def create_price(
        self,
        *,
        product_id: str,
        amount: int,
        currency: str,
        interval: str,
        interval_count: int,
    ) -> str:
        """在已有 Product 上创建新 Price，返回 price_id"""

    @abstractmethod
    async def archive_price(self, price_id: str) -> None:
        """归档 Price"""

    @abstractmethod
    async def create_subscription_checkout(
        self,
        *,
        customer_id: str,
        price_id: str,
        subscription_id: str,
        app_id: str,
        plan_id: str,
        success_url: str,
        cancel_url: str,
        metadata: dict | None = None,
        trial_period_days: int | None = None,
        expire_minutes: int | None = None,
    ) -> SubscriptionCheckoutResult:
        """创建订阅 Checkout Session"""

    @abstractmethod
    async def cancel_subscription(
        self,
        subscription_id: str,
        *,
        immediate: bool = False,
    ) -> SubscriptionActionResult:
        pass

    @abstractmethod
    async def resume_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        pass

    @abstractmethod
    async def pause_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        pass

    @abstractmethod
    async def unpause_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        pass

    @abstractmethod
    async def change_subscription_plan(
        self,
        subscription_id: str,
        *,
        new_price_id: str,
        proration_mode: str = "auto",
        credit_amount: int | None = None,
        currency: str | None = None,
        customer_id: str | None = None,
    ) -> SubscriptionActionResult:
        pass

    @abstractmethod
    async def schedule_subscription_downgrade(
        self,
        subscription_id: str,
        *,
        new_price_id: str,
        current_period_end: int,
    ) -> str:
        """降级：通过 Subscription Schedule 调度，返回 schedule_id"""

    @abstractmethod
    async def release_subscription_schedule(self, schedule_id: str) -> None:
        """取消待生效的降级调度"""

    @abstractmethod
    async def preview_plan_change(
        self,
        subscription_id: str,
        *,
        new_price_id: str,
    ) -> dict:
        """预览变更计划时 Stripe 将产生的费用明细，返回 dict 包含 line items 和 total"""
