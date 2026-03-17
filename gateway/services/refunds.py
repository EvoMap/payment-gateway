"""
退款服务层
"""

import uuid
from datetime import datetime, UTC

import structlog
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.models import Payment, Refund
from gateway.core.exceptions import (
    NotFoundException,
    BadRequestException,
    ServiceUnavailableException,
    InternalServerException,
)
from gateway.core.constants import PaymentStatus, RefundStatus, Provider
from gateway.core.schemas import CreateRefundRequest

logger = structlog.get_logger(__name__)


class RefundService:
    """退款服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_refund(
        self,
        req: CreateRefundRequest,
    ) -> Refund:
        """
        创建退款

        业务规则：
        1. 支付必须是成功状态（succeeded）
        2. 如果不指定退款金额，则为全额退款
        3. 部分退款时，累计退款金额不能超过支付金额
        4. 调用支付渠道的退款接口
        """
        log = logger.bind(
            payment_id=str(req.payment_id),
            refund_amount=req.refund_amount,
        )
        log.info("收到创建退款请求")

        # 1. 查询支付记录
        stmt = select(Payment).where(Payment.id == req.payment_id)
        result = await self.session.execute(stmt)
        payment = result.scalar_one_or_none()

        if not payment:
            log.warning("未找到支付记录")
            raise NotFoundException(
                message="支付记录不存在",
                code=4043,
                details={"payment_id": str(req.payment_id)},
            )

        # 2. 检查支付状态（必须是成功状态）
        if payment.status != PaymentStatus.succeeded:
            log.warning("支付未成功，无法退款", status=payment.status.value)
            raise BadRequestException(
                message=f"支付状态必须为成功状态，当前状态为：{payment.status.value}",
                code=4001,
                details={
                    "payment_id": str(req.payment_id),
                    "status": payment.status.value,
                },
            )

        # 3. 计算退款金额
        refund_amount = req.refund_amount
        if refund_amount is None:
            # 全额退款
            refund_amount = payment.amount
            log.info("全额退款", amount=refund_amount)
        else:
            # 部分退款，检查金额是否有效
            if refund_amount > payment.amount:
                log.warning(
                    "退款金额超过支付金额",
                    refund_amount=refund_amount,
                    payment_amount=payment.amount,
                )
                raise BadRequestException(
                    message=f"退款金额 {refund_amount} 超过支付金额 {payment.amount}",
                    code=4002,
                    details={
                        "refund_amount": refund_amount,
                        "payment_amount": payment.amount,
                    },
                )

        # 4. 检查累计退款金额
        stmt = select(func.sum(Refund.refund_amount)).where(
            Refund.payment_id == payment.id,
            Refund.status.in_([RefundStatus.pending, RefundStatus.succeeded]),
        )
        result = await self.session.execute(stmt)
        total_refunded = result.scalar() or 0

        if total_refunded + refund_amount > payment.amount:
            log.warning(
                "累计退款超过支付金额",
                total_refunded=total_refunded,
                new_refund=refund_amount,
                payment_amount=payment.amount,
            )
            raise BadRequestException(
                message=f"累计退款金额 {total_refunded + refund_amount} 将超过支付金额 {payment.amount}",
                code=4003,
                details={
                    "total_refunded": total_refunded,
                    "new_refund": refund_amount,
                    "total": total_refunded + refund_amount,
                    "payment_amount": payment.amount,
                },
            )

        # 5. 调用支付渠道退款接口
        provider_refund_id = None
        refund_status = RefundStatus.pending

        try:
            if payment.provider == Provider.stripe:
                from gateway.providers.stripe import get_stripe_adapter

                stripe_adapter = get_stripe_adapter()
                refund_result = await stripe_adapter.create_refund(
                    merchant_order_no=payment.merchant_order_no,
                    txn_id=payment.provider_txn_id,
                    refund_amount=refund_amount,
                    reason=req.reason,
                )

                provider_refund_id = refund_result["refund_id"]

                # Stripe 退款状态映射
                stripe_status = refund_result["status"]
                if stripe_status == "succeeded":
                    refund_status = RefundStatus.succeeded
                elif stripe_status == "failed":
                    refund_status = RefundStatus.failed
                else:
                    refund_status = RefundStatus.pending

                log.info(
                    "Stripe退款创建成功",
                    provider_refund_id=provider_refund_id,
                    status=stripe_status,
                )

            elif payment.provider == Provider.alipay:
                # TODO: 实现支付宝退款
                log.warning("支付宝退款未实现")
                raise ServiceUnavailableException(
                    message="支付宝退款功能尚未实现",
                    code=5031,
                    details={"provider": "alipay"},
                )

            elif payment.provider == Provider.wechatpay:
                # TODO: 实现微信支付退款
                log.warning("微信退款未实现")
                raise ServiceUnavailableException(
                    message="微信支付退款功能尚未实现",
                    code=5032,
                    details={"provider": "wechatpay"},
                )

            else:
                log.error("不支持的支付渠道", provider=payment.provider.value)
                raise BadRequestException(
                    message=f"不支持的支付渠道: {payment.provider.value}",
                    code=4004,
                    details={"provider": payment.provider.value},
                )

        except (BadRequestException, ServiceUnavailableException):
            raise
        except Exception as e:
            log.error("退款创建失败", error=str(e))
            raise InternalServerException(
                message="退款创建失败", code=5001, details={"error": str(e)}
            )

        # 6. 创建退款记录
        refund = Refund(
            payment_id=payment.id,
            refund_amount=refund_amount,
            reason=req.reason,
            notify_url=req.notify_url,
            status=refund_status,
            provider=payment.provider,
            provider_refund_id=provider_refund_id,
            refunded_at=(
                datetime.now(UTC) if refund_status == RefundStatus.succeeded else None
            ),
        )

        self.session.add(refund)
        await self.session.commit()
        await self.session.refresh(refund)

        log.info(
            "退款记录创建完成", refund_id=str(refund.id), status=refund.status.value
        )

        return refund

    async def get_refund(self, refund_id: uuid.UUID) -> Refund:
        """查询退款详情"""
        log = logger.bind(refund_id=str(refund_id))
        log.info("收到退款详情查询请求")

        stmt = select(Refund).where(Refund.id == refund_id)
        result = await self.session.execute(stmt)
        refund = result.scalar_one_or_none()

        if not refund:
            log.warning("未找到退款记录")
            raise NotFoundException(
                message="退款记录不存在",
                code=4044,
                details={"refund_id": str(refund_id)},
            )

        log.info("退款记录已找到", status=refund.status.value)
        return refund

    async def list_refunds_by_payment(
        self,
        payment_id: uuid.UUID,
        skip: int = 0,
        limit: int = 100,
    ) -> tuple[list[Refund], int]:
        """查询支付的所有退款记录"""
        log = logger.bind(payment_id=str(payment_id))
        log.info("收到退款列表查询请求", skip=skip, limit=limit)

        # 查询总数
        count_stmt = (
            select(func.count())
            .select_from(Refund)
            .where(Refund.payment_id == payment_id)
        )
        total_result = await self.session.execute(count_stmt)
        total = total_result.scalar() or 0

        # 查询列表
        stmt = (
            select(Refund)
            .where(Refund.payment_id == payment_id)
            .order_by(Refund.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        refunds = list(result.scalars().all())

        log.info("退款列表查询完成", total=total, returned=len(refunds))
        return refunds, total

    async def sync_refund_status(self, refund_id: uuid.UUID) -> Refund:
        """
        同步退款状态（从支付渠道）

        用于查询渠道侧的退款状态并更新本地记录
        """
        log = logger.bind(refund_id=str(refund_id))
        log.info("收到退款状态同步请求")

        # 1. 查询退款记录
        stmt = select(Refund).where(Refund.id == refund_id)
        result = await self.session.execute(stmt)
        refund = result.scalar_one_or_none()

        if not refund:
            log.warning("refund_not_found")
            raise NotFoundException(
                message="退款记录不存在",
                code=4045,
                details={"refund_id": str(refund_id)},
            )

        # 2. 如果已经是最终状态，无需同步
        if refund.status in [
            RefundStatus.succeeded,
            RefundStatus.failed,
            RefundStatus.canceled,
        ]:
            log.info("退款已是终态", status=refund.status.value)
            return refund

        # 3. 从支付渠道查询状态
        if not refund.provider_refund_id:
            log.warning("缺少渠道退款ID")
            raise BadRequestException(
                message="退款记录没有渠道退款ID",
                code=4005,
                details={"refund_id": str(refund_id)},
            )

        try:
            if refund.provider == Provider.stripe:
                from gateway.providers.stripe import get_stripe_adapter

                stripe_adapter = get_stripe_adapter()

                refund_result = await stripe_adapter.get_refund(
                    refund.provider_refund_id
                )

                # 更新状态
                stripe_status = refund_result["status"]
                old_status = refund.status

                if stripe_status == "succeeded":
                    refund.status = RefundStatus.succeeded
                    refund.refunded_at = datetime.now(UTC)
                elif stripe_status == "failed":
                    refund.status = RefundStatus.failed
                elif stripe_status == "canceled":
                    refund.status = RefundStatus.canceled

                if refund.status != old_status:
                    await self.session.commit()
                    await self.session.refresh(refund)
                    log.info(
                        "退款状态已更新",
                        old_status=old_status.value,
                        new_status=refund.status.value,
                    )
                else:
                    log.info("退款状态无变化", status=refund.status.value)

            else:
                log.warning("渠道不支持退款状态同步", provider=refund.provider.value)
                raise ServiceUnavailableException(
                    message=f"渠道 {refund.provider.value} 不支持状态同步",
                    code=5033,
                    details={"provider": refund.provider.value},
                )

        except (BadRequestException, ServiceUnavailableException, NotFoundException):
            raise
        except Exception as e:
            log.error("退款状态同步失败", error=str(e))
            raise InternalServerException(
                message="退款状态同步失败", code=5002, details={"error": str(e)}
            )

        return refund
