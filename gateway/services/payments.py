"""
支付服务层（创建/查询/状态机推进 + external_user_id 并发控制）
"""

import hashlib
import struct
import uuid
from datetime import datetime, UTC

import structlog
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from gateway.core.models import App, Payment
from gateway.core.constants import PaymentStatus
from gateway.core.exceptions import NotFoundException, ConflictException
from gateway.core.schemas import CreatePaymentRequest

logger = structlog.get_logger(__name__)


class PaymentService:
    """支付服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_or_get_payment(
        self,
        app: App,
        req: CreatePaymentRequest,
        idempotency_key: str | None = None,
    ) -> tuple[Payment, bool]:
        log = logger.bind(
            app_id=str(app.id),
            merchant_order_no=req.merchant_order_no,
            provider=req.provider.value,
            idempotency_key=idempotency_key,
        )

        # external_user_id 并发控制
        if req.external_user_id:
            user_lock_key = struct.unpack(
                ">q",
                hashlib.sha256(
                    f"{app.id}:user:{req.external_user_id}".encode()
                ).digest()[:8],
            )[0]
            await self.session.execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": user_lock_key},
            )

            pending_stmt = select(func.count()).where(
                Payment.app_id == app.id,
                Payment.external_user_id == req.external_user_id,
                Payment.status == PaymentStatus.pending,
            )
            pending_count = (
                await self.session.execute(pending_stmt)
            ).scalar()
            if pending_count >= 1:
                raise ConflictException(
                    message="该用户已有未完成的支付订单", code=4093
                )

        # merchant_order_no advisory lock
        raw = f"{app.id}:{req.merchant_order_no}".encode()
        lock_key = struct.unpack(">q", hashlib.sha256(raw).digest()[:8])[0]
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key}
        )

        stmt = select(Payment).where(
            Payment.app_id == app.id,
            Payment.merchant_order_no == req.merchant_order_no,
        )
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            request_total_amount = (req.unit_amount or 0) * req.quantity

            if (
                existing.amount != request_total_amount
                or existing.currency != req.currency
                or existing.provider != req.provider
            ):
                log.warning(
                    "幂等校验冲突",
                    existing_amount=existing.amount,
                    request_amount=request_total_amount,
                )
                raise ConflictException(
                    message="该商户订单号已存在，但关键参数不一致",
                    code=4091,
                    details={
                        "merchant_order_no": req.merchant_order_no,
                    },
                )

            log.info("幂等命中返回已有支付", payment_id=str(existing.id))
            return existing, False

        total_amount = (req.unit_amount or 0) * req.quantity

        payment = Payment(
            id=uuid.uuid4(),
            app_id=app.id,
            merchant_order_no=req.merchant_order_no,
            provider=req.provider,
            amount=total_amount,
            currency=req.currency,
            status=PaymentStatus.pending,
            notify_url=req.notify_url or app.notify_url,
            provider_txn_id=None,
            external_user_id=req.external_user_id,
        )

        self.session.add(payment)

        try:
            await self.session.flush()
            log.info("支付记录已创建", payment_id=str(payment.id))
            return payment, True
        except IntegrityError as exc:
            await self.session.rollback()
            log.warning("支付创建并发冲突", error=str(exc))
            raise ConflictException(
                message="支付创建并发冲突，请重试",
                code=4092,
                details={"error": str(exc)},
            )

    async def get_payment_by_id(self, app: App, payment_id: uuid.UUID) -> Payment:
        stmt = select(Payment).where(
            Payment.id == payment_id,
            Payment.app_id == app.id,
        )
        result = await self.session.execute(stmt)
        payment = result.scalar_one_or_none()

        if payment is None:
            raise NotFoundException(
                message="支付记录不存在",
                code=4041,
                details={"payment_id": str(payment_id)},
            )

        return payment

    async def get_payment_by_merchant_order_no(
        self, app: App, merchant_order_no: str
    ) -> Payment:
        stmt = select(Payment).where(
            Payment.app_id == app.id,
            Payment.merchant_order_no == merchant_order_no,
        )
        result = await self.session.execute(stmt)
        payment = result.scalar_one_or_none()

        if payment is None:
            raise NotFoundException(
                message="支付记录不存在",
                code=4042,
                details={"merchant_order_no": merchant_order_no},
            )

        return payment

    async def update_payment_status(
        self,
        payment: Payment,
        new_status: PaymentStatus,
        provider_txn_id: str | None = None,
    ):
        payment.status = new_status

        if provider_txn_id and not payment.provider_txn_id:
            payment.provider_txn_id = provider_txn_id

        if new_status == PaymentStatus.succeeded and not payment.paid_at:
            payment.paid_at = datetime.now(UTC)
