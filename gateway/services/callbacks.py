"""
Callback 处理服务（入站回调 -> 落库 -> 推进 Payment -> 生成 WebhookDelivery）
"""

import traceback
import uuid
from datetime import datetime, UTC

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from gateway.core.constants import (
    CallbackStatus,
    PaymentStatus,
    DeliveryStatus,
    RefundStatus,
    Provider,
)
from gateway.core.models import App, Callback, Payment, WebhookDelivery, Refund
from gateway.core.schemas import CallbackEvent

logger = structlog.get_logger(__name__)


class CallbackService:
    """回调处理服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def process_callback(self, event: CallbackEvent):
        """
        处理标准化的回调事件

        流程：
        1. 写入 callbacks 表（幂等）
        2. 定位 Payment（通过 merchant_order_no 或 provider_txn_id）
        3. 推进 Payment 状态
        4. 生成 WebhookDelivery
        """
        log = logger.bind(
            provider_event_id=event.provider_event_id,
            provider_txn_id=event.provider_txn_id,
            merchant_order_no=event.merchant_order_no,
            outcome=event.outcome,
        )

        log.info("开始处理回调")

        # 1. 写入 callbacks（幂等）
        callback = await self._upsert_callback(event)

        # 2. 定位 Payment
        payment = await self._find_payment(event)
        if not payment:
            log.warning("未找到回调对应的支付")
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return

        payment_id = payment.id
        # 关联 callback -> payment
        callback.payment_id = payment_id

        # 退款回调
        if event.outcome.startswith("refund_"):
            await self._process_refund_callback(payment, event, callback, log)
        # 支付回调
        else:
            await self._process_payment_callback(payment, event, callback, log)

    async def _process_refund_callback(
        self, payment: Payment, event: CallbackEvent, callback: Callback, log
    ):
        """处理退款回调事件"""
        refund_obj = event.raw_payload.get("data", {}).get("object", {})
        provider_refund_id = refund_obj.get("id")

        if not provider_refund_id:
            log.warning("退款回调缺少退款ID")
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return

        provider_value = event.raw_payload.get("provider")
        provider = None
        if provider_value:
            try:
                provider = Provider(provider_value)
            except ValueError:
                log.warning("未知的退款回调渠道", provider=provider_value)

        stmt = select(Refund).where(Refund.provider_refund_id == provider_refund_id)
        if provider:
            stmt = stmt.where(Refund.provider == provider)

        result = await self.session.execute(stmt)
        refund = result.scalar_one_or_none()

        if not refund:
            log.warning("未找到回调对应的退款", provider_refund_id=provider_refund_id)
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return

        # 关联 callback -> payment（便于追踪）
        callback.payment_id = refund.payment_id

        # 先加锁，再做后续判断，避免并发窗口
        await self.session.refresh(refund, with_for_update=True)

        # 确保 payment 与退款记录一致
        if payment.id != refund.payment_id:
            log.warning(
                "退款回调关联的Payment与退款记录不一致，使用退款记录中的Payment",
                original_payment_id=str(payment.id),
                refund_payment_id=str(refund.payment_id),
            )
            stmt = select(Payment).where(Payment.id == refund.payment_id)
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                await self.session.refresh(payment, with_for_update=True)
            if not payment:
                log.error(
                    "退款记录关联的Payment不存在",
                    refund_id=str(refund.id),
                    refund_payment_id=str(refund.payment_id),
                )
                callback.status = CallbackStatus.failed
                await self.session.commit()
                return

        outcome_map = {
            "refund_succeeded": RefundStatus.succeeded,
            "refund_failed": RefundStatus.failed,
            "refund_pending": RefundStatus.pending,
            "refund_canceled": RefundStatus.canceled,
        }
        new_status = outcome_map.get(event.outcome)

        refund_terminal = {RefundStatus.succeeded, RefundStatus.failed, RefundStatus.canceled}

        if not new_status:
            log.warning(
                "退款回调outcome无法映射到已知状态",
                refund_id=str(refund.id),
                outcome=event.outcome,
            )
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return
        elif refund.status in refund_terminal:
            log.warning(
                "退款已处于终态，忽略状态变更",
                refund_id=str(refund.id),
                current_status=refund.status.value,
                incoming_status=new_status.value,
            )
        elif new_status != refund.status:
            refund.status = new_status
            if new_status == RefundStatus.succeeded and not refund.refunded_at:
                refund.refunded_at = datetime.now(UTC)

            if new_status in refund_terminal:
                await self._create_refund_webhook_delivery(payment, refund, new_status)
        else:
            log.debug(
                "退款回调状态未变更，跳过",
                refund_id=str(refund.id),
                status=refund.status.value,
            )

        if not refund.provider_refund_id:
            refund.provider_refund_id = provider_refund_id

        callback.status = CallbackStatus.processed
        callback.processed_at = datetime.now(UTC)
        await self.session.commit()
        log.info("退款回调处理完成", refund_id=str(refund.id))

    async def _process_payment_callback(
        self, payment: Payment, event: CallbackEvent, callback: Callback, log
    ):

        # 推进 Payment 状态（加锁）
        payment_id = str(payment.id)
        await self.session.refresh(payment, with_for_update=True)

        old_status = payment.status
        new_status = self._map_outcome_to_status(event.outcome)

        payment_terminal = {PaymentStatus.succeeded, PaymentStatus.failed, PaymentStatus.canceled}

        if not new_status:
            log.warning(
                "支付回调outcome无法映射到已知状态",
                payment_id=payment_id,
                outcome=event.outcome,
            )
            callback.status = CallbackStatus.failed
            await self.session.commit()
            return
        elif old_status in payment_terminal:
            log.warning(
                "支付已处于终态，忽略状态变更",
                payment_id=payment_id,
                current_status=old_status.value,
                incoming_status=new_status.value,
            )
        elif new_status != old_status:
            payment.status = new_status

            if event.provider_txn_id and not payment.provider_txn_id:
                payment.provider_txn_id = event.provider_txn_id

            if new_status == PaymentStatus.succeeded and not payment.paid_at:
                payment.paid_at = datetime.now(UTC)

            log.info(
                "支付回调推进支付状态",
                payment_id=payment_id,
                old_status=old_status.value,
                new_status=new_status.value,
            )

            if new_status in payment_terminal:
                await self._create_payment_webhook_delivery(payment, new_status)
        else:
            log.debug(
                "支付回调状态未变更，跳过",
                payment_id=payment_id,
                status=old_status.value,
            )

        # 标记 callback 已处理
        callback.status = CallbackStatus.processed
        callback.processed_at = datetime.now(UTC)

        await self.session.commit()
        log.info("支付回调处理完成", payment_id=payment_id)

    async def _upsert_callback(self, event: CallbackEvent) -> Callback:
        """写入 callback（幂等）"""
        # 尝试插入
        callback = Callback(
            id=uuid.uuid4(),
            provider=event.raw_payload.get("provider"),
            provider_event_id=event.provider_event_id,
            provider_txn_id=event.provider_txn_id,
            payment_id=None,
            payload=event.raw_payload,
            status=CallbackStatus.processing,
        )

        self.session.add(callback)

        try:
            await self.session.flush()
            return callback
        except IntegrityError:
            # 已存在（幂等），回滚后查询
            logger.error(f"写入回调事件失败：{traceback.format_exc()}")
            await self.session.rollback()
            stmt = select(Callback).where(
                Callback.provider == callback.provider,
                Callback.provider_event_id == callback.provider_event_id,
            )
            result = await self.session.execute(stmt)
            existing = result.scalar_one()
            return existing

    async def _find_payment(self, event: CallbackEvent) -> Payment | None:
        """定位 Payment（通过 merchant_order_no 或 provider_txn_id）"""
        # 优先用 merchant_order_no
        if event.merchant_order_no:
            stmt = select(Payment).where(
                Payment.merchant_order_no == event.merchant_order_no
            )
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                return payment

        # 回退用 provider_txn_id
        if event.provider_txn_id:
            stmt = select(Payment).where(
                Payment.provider_txn_id == event.provider_txn_id
            )
            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()
            if payment:
                return payment

        return None

    def _map_outcome_to_status(self, outcome: str) -> PaymentStatus | None:
        """映射 outcome 到 PaymentStatus"""
        outcome_map = {
            "succeeded": PaymentStatus.succeeded,
            "failed": PaymentStatus.failed,
            "canceled": PaymentStatus.canceled,
            # 统一收敛：过期视为取消（未完成支付而关闭）
            "expired": PaymentStatus.canceled,
            "pending": PaymentStatus.pending,
        }
        return outcome_map.get(outcome)

    async def _create_webhook_delivery(
        self,
        payment: Payment,
        *,
        event_id: str,
        event_type: str,
        payload: dict,
    ):
        """生成 WebhookDelivery 任务"""
        notify_url = payment.notify_url
        if not notify_url:
            stmt = select(App.notify_url).where(App.id == payment.app_id)
            result = await self.session.execute(stmt)
            notify_url = result.scalar_one_or_none()

        if not notify_url:
            logger.warning(
                "缺少回调通知地址",
                payment_id=str(payment.id),
                app_id=str(payment.app_id),
            )
            return

        payload = {
            "event_id": event_id,
            "event_type": event_type,
            **payload,
        }

        stmt = select(WebhookDelivery).where(
            WebhookDelivery.app_id == payment.app_id,
            WebhookDelivery.event_id == event_id,
        )
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            if existing.status == DeliveryStatus.succeeded:
                logger.debug(
                    "Webhook已成功投递，跳过重入队",
                    delivery_id=str(existing.id),
                    event_id=event_id,
                )
                return

            existing.notify_url = notify_url
            existing.payload = payload
            existing.status = DeliveryStatus.pending
            existing.attempt_count = 0
            existing.next_attempt_at = datetime.now(UTC)
            existing.last_attempt_at = None
            existing.last_http_status = None
            existing.last_error = None
            existing.delivered_at = None
            logger.info(
                "Webhook投递任务已重入队",
                delivery_id=str(existing.id),
                event_id=event_id,
            )
            return

        delivery = WebhookDelivery(
            id=uuid.uuid4(),
            app_id=payment.app_id,
            payment_id=payment.id,
            event_id=event_id,
            event_type=event_type,
            notify_url=notify_url,
            payload=payload,
            status=DeliveryStatus.pending,
            attempt_count=0,
            next_attempt_at=datetime.now(UTC),  # 立即尝试
        )

        self.session.add(delivery)
        await self.session.flush()
        logger.info(
            "Webhook投递任务已创建",
            delivery_id=str(delivery.id),
            event_id=event_id,
        )

    async def _create_payment_webhook_delivery(
        self, payment: Payment, event_status: PaymentStatus
    ):
        """生成支付回调 WebhookDelivery 任务"""
        await self._create_webhook_delivery(
            payment,
            event_id=f"{payment.id}_{event_status.value}",
            event_type=f"payment.{event_status.value}",
            payload={
                "payment_id": str(payment.id),
                "merchant_order_no": payment.merchant_order_no,
                "status": payment.status.value,
                "amount": payment.amount,
                "currency": payment.currency.value,
                "provider_txn_id": payment.provider_txn_id,
                "paid_at": payment.paid_at.isoformat() if payment.paid_at else None,
            },
        )

    async def _create_refund_webhook_delivery(
        self, payment: Payment, refund: Refund, event_status: RefundStatus
    ):
        """生成退款回调 WebhookDelivery 任务"""
        await self._create_webhook_delivery(
            payment,
            event_id=f"{refund.id}_{event_status.value}",
            event_type=f"refund.{event_status.value}",
            payload={
                "refund_id": str(refund.id),
                "payment_id": str(payment.id),
                "merchant_order_no": payment.merchant_order_no,
                "status": refund.status.value,
                "refund_amount": refund.refund_amount,
                "provider_refund_id": refund.provider_refund_id,
                "refunded_at": (
                    refund.refunded_at.isoformat() if refund.refunded_at else None
                ),
                "reason": refund.reason,
                "currency": payment.currency.value,
            },
        )
