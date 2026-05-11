"""
Stripe Provider Adapter（支付 + 订阅）
"""

import time
import uuid
import traceback
from datetime import datetime, UTC

import httpx
import stripe

from gateway.core.logging import get_logger
from gateway.core.constants import Provider, EventCategory
from gateway.core.settings import get_settings
from gateway.core.exceptions import (
    IgnoredException,
    BadRequestException,
    NotFoundException,
    PaymentProviderException,
)
from gateway.core.schemas import PaymentTypeEnum, CallbackEvent
from .base import (
    ProviderAdapter,
    ProviderPaymentResult,
    PaymentFlowType,
    SubscriptionProviderMixin,
    SubscriptionCheckoutResult,
    SubscriptionActionResult,
)

settings = get_settings()
logger = get_logger()


def _handle_stripe_sub_error(e: stripe.InvalidRequestError) -> None:
    """将 Stripe InvalidRequestError 转为业务异常。"""
    msg = str(e)
    if "No such subscription" in msg:
        raise NotFoundException(message="订阅在 Stripe 中不存在", code=4041)
    if "canceled subscription" in msg.lower():
        raise BadRequestException(message="订阅已取消，无法执行此操作", code=4018)
    raise PaymentProviderException(message=f"Stripe 请求失败: {msg}", code=5021)


def _get_sub_period_end(sub) -> int | None:
    """从 Subscription 的 items 中提取 current_period_end。

    新版 Stripe API 已将 current_period_end 从 Subscription 顶层
    移至 items.data[].current_period_end（订阅项级别）。
    """
    try:
        if sub["items"] and sub["items"]["data"]:
            return sub["items"]["data"][0].current_period_end
    except (AttributeError, IndexError):
        pass
    return None


# wechat_pay 使用 Stripe payment link 模式，不支持 payment_intent_data
_NO_PAYMENT_INTENT_DATA = {"wechat_pay"}


class StripeAdapter(ProviderAdapter, SubscriptionProviderMixin):
    """Stripe 支付 + 订阅适配器"""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.secret_key = settings.stripe_secret_key
        self.webhook_secret = settings.stripe_webhook_secret

        if not self.secret_key:
            raise ValueError(
                "Stripe 配置不完整。请设置：STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET"
            )

        stripe.api_key = self.secret_key
        stripe.max_network_retries = 1
        try:
            stripe.default_http_client = stripe.HTTPXClient(
                timeout=httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)
            )
        except Exception as e:
            logger.warning("stripe_http_client_config_failed", error=str(e))
        StripeAdapter._initialized = True

    @property
    def provider(self) -> Provider:
        return Provider.stripe

    @property
    def supported_flows(self) -> list[PaymentFlowType]:
        return [PaymentFlowType.HOSTED]

    # ==================== 支付方法 ====================

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
        success_url = kwargs.get("success_url")
        cancel_url = kwargs.get("cancel_url")
        metadata = kwargs.get("metadata")
        customer_email = metadata.get("customer_email") if metadata else None
        payment_method = kwargs.get("payment_method")
        payment_options = kwargs.get("payment_options")
        method_type = payment_method.value if payment_method else None

        if payment_method:
            payment_method_types = [method_type]
        else:
            payment_method_types = kwargs.get("payment_method_types")

        # Strip customer_email out of the metadata dict copied into Stripe — it
        # belongs only in the top-level customer_email field (used for prefill).
        # Stripe's own guidance is to keep PII out of metadata.
        passthrough_metadata = {
            k: v for k, v in (metadata or {}).items() if k != "customer_email"
        }
        session_metadata = {
            "merchant_order_no": merchant_order_no,
            "app_id": kwargs.get("app_id", ""),
            **passthrough_metadata,
        }

        session_data = {
            "mode": "payment",
            "customer_email": customer_email,
            "line_items": [
                {
                    "quantity": quantity,
                    "price_data": {
                        "currency": currency.lower(),
                        "unit_amount": unit_amount,
                        "product_data": {
                            "name": (product_name or "商品")[:250],
                            "description": product_desc[:500] if product_desc else None,
                        },
                    },
                }
            ],
            "metadata": session_metadata,
            "success_url": success_url
            or "https://example.com/success?session_id={CHECKOUT_SESSION_ID}",
            "cancel_url": cancel_url or "https://example.com/cancel",
        }

        if method_type is None or method_type not in _NO_PAYMENT_INTENT_DATA:
            session_data["payment_intent_data"] = {"metadata": session_metadata}

        if payment_method_types:
            session_data["payment_method_types"] = payment_method_types
        else:
            session_data["payment_method_types"] = ["card"]

        if payment_method and method_type == "wechat_pay":
            client = (payment_options or {}).get("client", "web")
            session_data["payment_method_options"] = {
                "wechat_pay": {"client": client}
            }

        if expire_minutes:
            expire_seconds = max(1800, min(expire_minutes * 60, 86400))
            session_data["expires_at"] = int(time.time() + expire_seconds)

        try:
            session = await stripe.checkout.Session.create_async(**session_data)
            return ProviderPaymentResult(
                type=PaymentTypeEnum.url,
                payload={
                    "checkout_url": session.url,
                    "session_id": session.id,
                },
                provider_txn_id=session.id,
            )
        except stripe.error.InvalidRequestError as e:
            error_msg = str(e)
            # fallback 仅适用于未显式指定 payment_method 的场景（如 app 配置了不兼容的默认类型）
            if (
                "payment_method" in error_msg.lower()
                and session_data.get("payment_method_types") != ["card"]
                and not payment_method
            ):
                session_data["payment_method_types"] = ["card"]
                session_data.pop("payment_method_options", None)
                if "payment_intent_data" not in session_data:
                    session_data["payment_intent_data"] = {"metadata": session_metadata}
                try:
                    session = await stripe.checkout.Session.create_async(**session_data)
                    return ProviderPaymentResult(
                        type=PaymentTypeEnum.url,
                        payload={
                            "checkout_url": session.url,
                            "session_id": session.id,
                        },
                        provider_txn_id=session.id,
                    )
                except stripe.StripeError:
                    raise
            else:
                raise
        except stripe.StripeError:
            raise

    async def create_refund(
        self,
        *,
        txn_id: str,
        merchant_order_no: str,
        refund_amount: int | None = None,
        reason: str | None = None,
    ) -> dict:
        try:
            payment_intent_id = txn_id
            if txn_id.startswith("cs_"):
                session = await stripe.checkout.Session.retrieve_async(txn_id)
                payment_intent_id = session.payment_intent
                if not payment_intent_id:
                    raise ValueError(
                        f"Checkout Session 尚未生成 PaymentIntent，无法退款: {txn_id}"
                    )

            refund_params = {
                "payment_intent": payment_intent_id,
                "metadata": {"merchant_order_no": merchant_order_no},
            }

            if refund_amount is not None:
                refund_params["amount"] = refund_amount

            if reason:
                stripe_reason = "requested_by_customer"
                if reason in ["duplicate", "fraudulent", "requested_by_customer"]:
                    stripe_reason = reason
                refund_params["reason"] = stripe_reason

            refund = await stripe.Refund.create_async(**refund_params)

            return {
                "refund_id": refund.id,
                "status": refund.status,
                "amount": refund.amount,
                "currency": refund.currency,
                "payment_intent": refund.payment_intent,
                "reason": refund.get("reason"),
                "created": refund.created,
            }

        except stripe.error.StripeError as e:
            raise ValueError(f"Stripe 退款失败: {str(e)}")

    async def cancel_payment(
        self,
        *,
        merchant_order_no: str,
        provider_txn_id: str | None = None,
    ) -> dict:
        try:
            session_id = provider_txn_id

            if not session_id and provider_txn_id:
                sessions = await stripe.checkout.Session.list_async(
                    payment_intent=provider_txn_id,
                    limit=1,
                )
                if sessions.data:
                    session_id = sessions.data[0].id

            if not session_id:
                raise ValueError(
                    "Stripe cancel_payment 需要提供 Checkout Session ID"
                )

            session = await stripe.checkout.Session.expire_async(session_id)

            return {
                "success": bool(session.status == "expired"),
                "session_id": session.id,
                "status": session.status,
            }

        except stripe.error.InvalidRequestError as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Checkout Session 无法取消",
            }
        except stripe.error.StripeError as e:
            raise ValueError(f"Stripe 取消支付失败: {str(e)}")

    async def get_refund(self, refund_id: str) -> dict:
        try:
            refund = await stripe.Refund.retrieve_async(refund_id)
            return {
                "refund_id": refund.id,
                "status": refund.status,
                "amount": refund.amount,
                "currency": refund.currency,
                "payment_intent": refund.payment_intent,
                "reason": refund.get("reason"),
                "created": refund.created,
            }
        except stripe.error.StripeError as e:
            raise ValueError(f"查询 Stripe 退款失败: {str(e)}")

    async def confirm_payment(
        self, payment_id, provider_txn_id: str
    ) -> dict:
        """测试用：确认 Checkout Session 对应的 PaymentIntent（仅用于开发测试）"""
        try:
            session_id = provider_txn_id
            if session_id.startswith("cs_"):
                session = await stripe.checkout.Session.retrieve_async(session_id)
                pi_id = session.payment_intent
                if not pi_id:
                    return {"success": False, "error": "Checkout Session 无 PaymentIntent"}
            else:
                pi_id = session_id

            pi = await stripe.PaymentIntent.retrieve_async(pi_id)
            if pi.status == "succeeded":
                return {"success": True, "status": pi.status}

            if pi.status == "requires_payment_method":
                await stripe.PaymentIntent.modify_async(
                    pi_id,
                    payment_method="pm_card_visa",
                )
                pi = await stripe.PaymentIntent.confirm_async(pi_id)
            elif pi.status == "requires_confirmation":
                pi = await stripe.PaymentIntent.confirm_async(pi_id)

            return {"success": pi.status == "succeeded", "status": pi.status}
        except stripe.StripeError as e:
            raise ValueError(f"Stripe 确认支付失败: {str(e)}")

    # ==================== 订阅方法（SubscriptionProviderMixin 实现） ====================

    async def create_customer(
        self, *, email: str | None = None, metadata: dict | None = None
    ) -> str:
        customer = await stripe.Customer.create_async(
            email=email, metadata=metadata or {}
        )
        return customer.id

    async def update_customer_email(
        self, provider_customer_id: str, email: str
    ) -> None:
        await stripe.Customer.modify_async(provider_customer_id, email=email)

    async def create_product_and_price(
        self,
        *,
        name: str,
        amount: int,
        currency: str,
        interval: str,
        interval_count: int,
    ) -> tuple[str, str]:
        stripe_interval = "month" if interval == "quarter" else interval
        stripe_interval_count = (
            3 * interval_count if interval == "quarter" else interval_count
        )

        product = await stripe.Product.create_async(name=name)
        price = await stripe.Price.create_async(
            product=product.id,
            unit_amount=amount,
            currency=currency.lower(),
            recurring={
                "interval": stripe_interval,
                "interval_count": stripe_interval_count,
            },
        )
        return product.id, price.id

    async def create_price(
        self,
        *,
        product_id: str,
        amount: int,
        currency: str,
        interval: str,
        interval_count: int,
    ) -> str:
        """在已有 Product 上创建新 Price"""
        stripe_interval = "month" if interval == "quarter" else interval
        stripe_interval_count = (
            3 * interval_count if interval == "quarter" else interval_count
        )

        price = await stripe.Price.create_async(
            product=product_id,
            unit_amount=amount,
            currency=currency.lower(),
            recurring={
                "interval": stripe_interval,
                "interval_count": stripe_interval_count,
            },
        )
        return price.id

    async def archive_price(self, price_id: str) -> None:
        await stripe.Price.modify_async(price_id, active=False)

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
        sub_metadata = {
            "gateway_subscription_id": str(subscription_id),
            **(metadata or {}),
        }
        subscription_data: dict = {"metadata": sub_metadata}
        if trial_period_days:
            subscription_data["trial_period_days"] = trial_period_days

        session_metadata = {
            "app_id": str(app_id),
            "plan_id": str(plan_id),
            "gateway_subscription_id": str(subscription_id),
            **(metadata or {}),
        }

        session_data: dict = {
            "mode": "subscription",
            "customer": customer_id,
            "line_items": [{"price": price_id, "quantity": 1}],
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata": session_metadata,
            "subscription_data": subscription_data,
        }
        if expire_minutes:
            expire_seconds = max(1800, min(expire_minutes * 60, 86400))
            session_data["expires_at"] = int(time.time() + expire_seconds)

        session = await stripe.checkout.Session.create_async(**session_data)
        return SubscriptionCheckoutResult(
            session_id=session.id, checkout_url=session.url
        )

    async def cancel_subscription(
        self,
        subscription_id: str,
        *,
        immediate: bool = False,
    ) -> SubscriptionActionResult:
        try:
            if immediate:
                sub = await stripe.Subscription.cancel_async(subscription_id)
            else:
                sub = await stripe.Subscription.modify_async(
                    subscription_id, cancel_at_period_end=True
                )
        except stripe.InvalidRequestError as e:
            _handle_stripe_sub_error(e)
        period_end = _get_sub_period_end(sub)
        return SubscriptionActionResult(
            subscription_id=sub.id,
            status=sub.status,
            current_period_end=(
                datetime.fromtimestamp(period_end, tz=UTC)
                if period_end
                else None
            ),
            cancel_at_period_end=sub.cancel_at_period_end,
        )

    async def resume_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        try:
            sub = await stripe.Subscription.modify_async(
                subscription_id, cancel_at_period_end=False
            )
        except stripe.InvalidRequestError as e:
            _handle_stripe_sub_error(e)
        period_end = _get_sub_period_end(sub)
        return SubscriptionActionResult(
            subscription_id=sub.id,
            status=sub.status,
            current_period_end=(
                datetime.fromtimestamp(period_end, tz=UTC)
                if period_end
                else None
            ),
            cancel_at_period_end=False,
        )

    async def pause_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        try:
            sub = await stripe.Subscription.modify_async(
                subscription_id,
                pause_collection={"behavior": "void"},
            )
        except stripe.InvalidRequestError as e:
            _handle_stripe_sub_error(e)
        period_end = _get_sub_period_end(sub)
        return SubscriptionActionResult(
            subscription_id=sub.id,
            status=sub.status,
            current_period_end=(
                datetime.fromtimestamp(period_end, tz=UTC)
                if period_end
                else None
            ),
            cancel_at_period_end=sub.cancel_at_period_end,
        )

    async def unpause_subscription(
        self, subscription_id: str
    ) -> SubscriptionActionResult:
        try:
            sub = await stripe.Subscription.modify_async(
                subscription_id,
                pause_collection="",
            )
        except stripe.InvalidRequestError as e:
            _handle_stripe_sub_error(e)
        period_end = _get_sub_period_end(sub)
        return SubscriptionActionResult(
            subscription_id=sub.id,
            status=sub.status,
            current_period_end=(
                datetime.fromtimestamp(period_end, tz=UTC)
                if period_end
                else None
            ),
            cancel_at_period_end=sub.cancel_at_period_end,
        )

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
        current_sub = await stripe.Subscription.retrieve_async(subscription_id)
        if not current_sub["items"]["data"]:
            raise ValueError(f"订阅 {subscription_id} 无 items，无法变更")
        current_item_id = current_sub["items"]["data"][0].id

        modify_params = {
            "items": [{"id": current_item_id, "price": new_price_id}],
            "proration_behavior": "always_invoice",
            "billing_cycle_anchor": "now",
        }
        sub = await stripe.Subscription.modify_async(
            subscription_id, **modify_params
        )

        period_end = _get_sub_period_end(sub)
        return SubscriptionActionResult(
            subscription_id=sub.id,
            status=sub.status,
            current_period_end=(
                datetime.fromtimestamp(period_end, tz=UTC)
                if period_end
                else None
            ),
            cancel_at_period_end=sub.cancel_at_period_end,
        )

    async def preview_plan_change(
        self,
        subscription_id: str,
        *,
        new_price_id: str,
    ) -> dict:
        """使用 Stripe Invoice.create_preview 预览变更费用，与实际扣费逻辑一致。"""
        current_sub = await stripe.Subscription.retrieve_async(subscription_id)
        if not current_sub["items"]["data"]:
            raise ValueError(f"订阅 {subscription_id} 无 items，无法预览")
        current_item_id = current_sub["items"]["data"][0].id

        preview = await stripe.Invoice.create_preview_async(
            subscription=subscription_id,
            subscription_details={
                "items": [{"id": current_item_id, "price": new_price_id}],
                "proration_behavior": "always_invoice",
                "billing_cycle_anchor": "now",
            },
        )

        lines = []
        for line in preview.lines.data:
            lines.append({
                "amount": line.amount,
                "description": line.description or "",
            })

        return {
            "currency": preview.currency,
            "total": preview.total,
            "lines": lines,
        }

    async def schedule_subscription_downgrade(
        self,
        subscription_id: str,
        *,
        new_price_id: str,
        current_period_end: int,
    ) -> str:
        schedule = await stripe.SubscriptionSchedule.create_async(
            from_subscription=subscription_id,
        )

        current_phase = schedule.phases[0]
        current_items = [{"price": item["price"]} for item in current_phase["items"]]

        await stripe.SubscriptionSchedule.modify_async(
            schedule.id,
            end_behavior="release",
            phases=[
                {
                    "items": current_items,
                    "start_date": current_phase.start_date,
                    "end_date": current_period_end,
                },
                {
                    "items": [{"price": new_price_id}],
                    "start_date": current_period_end,
                },
            ],
        )

        return schedule.id

    async def release_subscription_schedule(self, schedule_id: str) -> None:
        await stripe.SubscriptionSchedule.release_async(schedule_id)

    # ==================== 回调解析（重构） ====================

    async def parse_and_verify_callback(
        self,
        headers: dict[str, str],
        body: bytes,
    ) -> CallbackEvent:
        if not self.webhook_secret:
            raise ValueError("Stripe webhook_secret not configured")

        sig_header = headers.get("stripe-signature")
        if not sig_header:
            raise ValueError("Missing Stripe-Signature header")

        try:
            event = stripe.Webhook.construct_event(
                payload=body,
                sig_header=sig_header,
                secret=self.webhook_secret,
            )
        except stripe.error.SignatureVerificationError as e:
            raise ValueError(f"Stripe signature verification failed: {e}")

        event_type = event["type"]
        event_data = event["data"]["object"]

        if event_type.startswith("checkout.session"):
            mode = event_data.get("mode")
            if mode == "subscription":
                return self._parse_subscription_checkout_event(
                    event_type, event_data, event
                )
            else:
                return self._parse_payment_checkout_event(
                    event_type, event_data, event
                )

        elif event_type.startswith("customer.subscription"):
            return self._parse_subscription_lifecycle_event(
                event_type, event_data, event
            )

        elif event_type.startswith("invoice."):
            return self._parse_invoice_event(event_type, event_data, event)

        elif event_type in {"refund.updated", "refund.failed"}:
            return self._parse_refund_event(event_type, event_data, event)

        else:
            raise IgnoredException(f"忽略非支持事件类型: {event_type}")

    def _parse_payment_checkout_event(
        self, event_type: str, event_data: dict, event: dict
    ) -> CallbackEvent:
        provider_txn_id = event_data.get("payment_intent")
        metadata = event_data.get("metadata", {})
        merchant_order_no = metadata.get("merchant_order_no")
        app_id_str = metadata.get("app_id")
        app_id = uuid.UUID(app_id_str) if app_id_str else None

        if event_type == "checkout.session.completed":
            payment_status = event_data.get("payment_status")
            if payment_status == "paid":
                outcome = "succeeded"
            elif payment_status == "unpaid":
                outcome = "pending"
            else:
                outcome = "unknown"
        else:
            outcome_map = {
                "checkout.session.async_payment_succeeded": "succeeded",
                "checkout.session.async_payment_failed": "failed",
                "checkout.session.expired": "canceled",
            }
            outcome = outcome_map.get(event_type, "unknown")

        return CallbackEvent(
            provider=self.provider,
            provider_event_id=event["id"],
            provider_txn_id=provider_txn_id,
            merchant_order_no=merchant_order_no,
            outcome=outcome,
            event_category=EventCategory.payment,
            app_id=app_id,
            raw_payload=event,
        )

    def _parse_refund_event(
        self, event_type: str, event_data: dict, event: dict
    ) -> CallbackEvent:
        provider_txn_id = event_data.get("payment_intent") or event_data.get("charge")
        merchant_order_no = event_data.get("metadata", {}).get("merchant_order_no")
        refund_status = event_data.get("status")

        if event_type == "refund.failed":
            outcome = "refund_failed"
        else:
            outcome_map = {
                "succeeded": "refund_succeeded",
                "failed": "refund_failed",
                "pending": "refund_pending",
                "canceled": "refund_canceled",
            }
            outcome = outcome_map.get(refund_status, "refund_unknown")

        return CallbackEvent(
            provider=self.provider,
            provider_event_id=event["id"],
            provider_txn_id=provider_txn_id,
            merchant_order_no=merchant_order_no,
            outcome=outcome,
            event_category=EventCategory.refund,
            raw_payload=event,
        )

    def _parse_subscription_checkout_event(
        self, event_type: str, event_data: dict, event: dict
    ) -> CallbackEvent:
        subscription_id = event_data.get("subscription")
        checkout_session_id = event_data.get("id")

        if event_type == "checkout.session.completed":
            outcome = (
                "subscription_activated"
                if event_data.get("payment_status") == "paid"
                else "subscription_pending"
            )
        elif event_type == "checkout.session.expired":
            outcome = "subscription_expired"
        elif event_type == "checkout.session.async_payment_succeeded":
            outcome = "subscription_activated"
        elif event_type == "checkout.session.async_payment_failed":
            outcome = "subscription_payment_failed"
        else:
            outcome = "subscription_unknown"

        return CallbackEvent(
            provider=self.provider,
            provider_event_id=event["id"],
            provider_txn_id=subscription_id,
            merchant_order_no=None,
            outcome=outcome,
            event_category=EventCategory.subscription,
            subscription_id=subscription_id,
            checkout_session_id=checkout_session_id,
            raw_payload=event,
        )

    def _parse_subscription_lifecycle_event(
        self, event_type: str, event_data: dict, event: dict
    ) -> CallbackEvent:
        subscription_id = event_data.get("id")
        gateway_subscription_id_str = event_data.get("metadata", {}).get(
            "gateway_subscription_id"
        )
        gateway_subscription_id = None
        if gateway_subscription_id_str:
            try:
                gateway_subscription_id = uuid.UUID(gateway_subscription_id_str)
            except ValueError:
                pass

        if event_type == "customer.subscription.created":
            outcome = "subscription_created"
        elif event_type == "customer.subscription.updated":
            previous_attrs = event.get("data", {}).get("previous_attributes", {})
            pause_collection = event_data.get("pause_collection")
            prev_pause = previous_attrs.get("pause_collection")

            if (
                pause_collection
                and prev_pause is None
                and "pause_collection" in previous_attrs
            ):
                outcome = "subscription_paused"
            elif (
                not pause_collection
                and prev_pause is not None
                and "pause_collection" in previous_attrs
            ):
                outcome = "subscription_resumed"
            else:
                outcome = "subscription_updated"
        else:
            outcome_map = {
                "customer.subscription.deleted": "subscription_canceled",
                "customer.subscription.paused": "subscription_paused",
                "customer.subscription.resumed": "subscription_resumed",
                "customer.subscription.trial_will_end": "subscription_trial_will_end",
            }
            outcome = outcome_map.get(event_type, "subscription_unknown")

        return CallbackEvent(
            provider=self.provider,
            provider_event_id=event["id"],
            provider_txn_id=subscription_id,
            merchant_order_no=None,
            outcome=outcome,
            event_category=EventCategory.subscription,
            subscription_id=subscription_id,
            gateway_subscription_id=gateway_subscription_id,
            raw_payload=event,
        )

    def _parse_invoice_event(
        self, event_type: str, event_data: dict, event: dict
    ) -> CallbackEvent:
        subscription_id = event_data.get("subscription")
        if not subscription_id:
            raise IgnoredException(f"忽略非订阅 Invoice 事件: {event_type}")

        gateway_subscription_id_str = (
            event_data.get("subscription_details", {})
            .get("metadata", {})
            .get("gateway_subscription_id")
        )
        gateway_subscription_id = None
        if gateway_subscription_id_str:
            try:
                gateway_subscription_id = uuid.UUID(gateway_subscription_id_str)
            except ValueError:
                pass

        outcome_map = {
            "invoice.paid": "invoice_paid",
            "invoice.payment_failed": "invoice_payment_failed",
            "invoice.payment_action_required": "invoice_action_required",
        }
        outcome = outcome_map.get(event_type)
        if not outcome:
            raise IgnoredException(f"忽略非支持 Invoice 事件类型: {event_type}")

        return CallbackEvent(
            provider=self.provider,
            provider_event_id=event["id"],
            provider_txn_id=subscription_id,
            merchant_order_no=None,
            outcome=outcome,
            event_category=EventCategory.invoice,
            subscription_id=subscription_id,
            gateway_subscription_id=gateway_subscription_id,
            invoice_id=event_data.get("id"),
            raw_payload=event,
        )


_stripe_adapter_instance = None


def get_stripe_adapter() -> StripeAdapter:
    """获取 Stripe 适配器单例"""
    global _stripe_adapter_instance
    if _stripe_adapter_instance is None:
        _stripe_adapter_instance = StripeAdapter()
    return _stripe_adapter_instance
