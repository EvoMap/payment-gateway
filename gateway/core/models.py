from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from gateway.core.constants import (
    CallbackStatus,
    Currency,
    DeliveryStatus,
    PaymentStatus,
    Provider,
    RefundStatus,
)


class Base(DeclarativeBase):
    pass


class App(Base):
    __tablename__ = "apps"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="应用ID（主键）",
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="应用名称")
    api_key: Mapped[str] = mapped_column(
        String(128), nullable=False, unique=True, comment="应用 API Key（唯一）"
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, comment="是否启用"
    )
    notify_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, comment="默认支付结果回调通知地址"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="创建时间",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="更新时间",
    )

    # 关联：该应用下的支付订单列表
    payments: Mapped[list["Payment"]] = relationship(back_populates="app")
    # 关联：该应用下的出站回调投递任务
    webhook_deliveries: Mapped[list["WebhookDelivery"]] = relationship(
        back_populates="app"
    )


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint(
            "app_id", "merchant_order_no", name="uq_payments_app_merchant_order_no"
        ),
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint(
            "(status = 'succeeded' AND paid_at IS NOT NULL) OR (status <> 'succeeded' AND paid_at IS NULL)",
            name="ck_payments_paid_at_matches_status",
        ),
        Index("ix_payments_app_created_at", "app_id", "created_at"),
        Index("ix_payments_status_created_at", "status", "created_at"),
        Index("ix_payments_provider_provider_txn_id", "provider", "provider_txn_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="支付交易ID（主键）",
    )

    app_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("apps.id", ondelete="RESTRICT"),
        nullable=False,
        comment="所属应用ID（外键 apps.id）",
    )
    merchant_order_no: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="商户订单号（应用内唯一）"
    )

    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider"),
        nullable=False,
        comment="支付渠道/提供方",
    )

    amount: Mapped[int] = mapped_column(
        nullable=False, comment="支付金额（最小货币单位，如分）"
    )
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency"),
        nullable=False,
        comment="币种",
    )

    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status"),
        nullable=False,
        default=PaymentStatus.pending,
        comment="支付状态",
    )

    provider_txn_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧交易号/流水号"
    )
    notify_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, comment="本单回调通知地址（可覆盖应用默认）"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="创建时间",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="更新时间",
    )
    paid_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="支付完成时间"
    )

    # 关联：所属应用
    app: Mapped["App"] = relationship(back_populates="payments")
    # 关联：该支付交易收到的渠道回调事件列表（删除支付交易时由DB通过 ondelete=SET NULL 处理）
    callbacks: Mapped[list["Callback"]] = relationship(
        back_populates="payment", passive_deletes=True
    )
    # 关联：该支付交易对应的出站投递任务
    webhook_deliveries: Mapped[list["WebhookDelivery"]] = relationship(
        back_populates="payment", passive_deletes=True
    )
    # 关联：该支付交易的退款记录
    refunds: Mapped[list["Refund"]] = relationship(back_populates="payment")


class Callback(Base):
    __tablename__ = "callbacks"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_event_id",
            name="uq_callbacks_provider_provider_event_id",
        ),
        Index("ix_callbacks_provider_provider_txn_id", "provider", "provider_txn_id"),
        Index("ix_callbacks_payment_id_received_at", "payment_id", "received_at"),
        Index("ix_callbacks_status_received_at", "status", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="回调记录ID（主键）",
    )

    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider"),
        nullable=False,
        comment="回调来源渠道/提供方",
    )
    provider_event_id: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="渠道侧事件ID（同渠道唯一，用于幂等）"
    )

    provider_txn_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧交易号/流水号（便于关联）"
    )

    payment_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("payments.id", ondelete="SET NULL"),
        nullable=True,
        comment="关联的支付订单ID（外键 payments.id，可为空）",
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, comment="回调原始报文（JSON）"
    )

    status: Mapped[CallbackStatus] = mapped_column(
        Enum(CallbackStatus, name="callback_status"),
        nullable=False,
        default=CallbackStatus.received,
        comment="回调处理状态",
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="接收时间",
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="处理完成时间"
    )

    # 关联：对应的支付订单（可能为空）
    payment: Mapped["Payment | None"] = relationship(
        back_populates="callbacks", passive_deletes=True
    )


class WebhookDelivery(Base):
    """
    出站回调投递任务（通知业务方），支持失败重试/退避/死信。

    说明：
    - callbacks：只负责“渠道入站事件收件箱”
    - webhook_deliveries：负责“对业务方的出站投递 + 自动重试”
    """

    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "app_id", "event_id", name="uq_webhook_deliveries_app_event_id"
        ),
        Index(
            "ix_webhook_deliveries_status_next_attempt_at", "status", "next_attempt_at"
        ),
        Index("ix_webhook_deliveries_app_created_at", "app_id", "created_at"),
        Index("ix_webhook_deliveries_payment_created_at", "payment_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="出站投递任务ID（主键）",
    )

    app_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("apps.id", ondelete="RESTRICT"),
        nullable=False,
        comment="所属应用ID（外键 apps.id）",
    )

    payment_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("payments.id", ondelete="SET NULL"),
        nullable=True,
        comment="关联的支付交易ID（外键 payments.id，可为空）",
    )

    event_id: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="投递事件ID（业务方幂等键，应用内唯一）"
    )
    event_type: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="事件类型（如 payment.succeeded）"
    )

    notify_url: Mapped[str] = mapped_column(
        String(2048), nullable=False, comment="投递目标地址（业务方回调URL）"
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, comment="投递内容（JSON）"
    )

    status: Mapped[DeliveryStatus] = mapped_column(
        Enum(DeliveryStatus, name="delivery_status"),
        nullable=False,
        default=DeliveryStatus.pending,
        comment="投递状态",
    )

    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="投递尝试次数"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="下次尝试时间（退避调度）"
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="最近一次尝试时间"
    )
    last_http_status: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="最近一次HTTP状态码"
    )
    last_error: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="最近一次失败原因/异常摘要"
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="投递成功时间"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="创建时间",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="更新时间",
    )

    # 关联
    app: Mapped["App"] = relationship(back_populates="webhook_deliveries")
    payment: Mapped["Payment | None"] = relationship(
        back_populates="webhook_deliveries", passive_deletes=True
    )


class Refund(Base):
    """
    退款记录表

    说明：
    - 记录所有退款请求及其状态
    - 支持全额退款和部分退款
    - 一笔支付可以有多次退款（累计退款金额不能超过支付金额）
    """

    __tablename__ = "refunds"
    __table_args__ = (
        CheckConstraint("refund_amount > 0", name="ck_refunds_amount_positive"),
        CheckConstraint(
            "(status = 'succeeded' AND refunded_at IS NOT NULL) OR (status <> 'succeeded' AND refunded_at IS NULL)",
            name="ck_refunds_refunded_at_matches_status",
        ),
        Index("ix_refunds_payment_id_created_at", "payment_id", "created_at"),
        Index("ix_refunds_status_created_at", "status", "created_at"),
        Index(
            "ix_refunds_provider_provider_refund_id", "provider", "provider_refund_id"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="退款ID（主键）",
    )

    payment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("payments.id", ondelete="RESTRICT"),
        nullable=False,
        comment="关联的支付交易ID（外键 payments.id）",
    )

    refund_amount: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="退款金额（最小货币单位，如分）",
    )

    reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="退款原因",
    )

    status: Mapped[RefundStatus] = mapped_column(
        Enum(RefundStatus, name="refund_status"),
        nullable=False,
        default=RefundStatus.pending,
        comment="退款状态",
    )

    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider"),
        nullable=False,
        comment="退款渠道/提供方",
    )

    provider_refund_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="渠道侧退款ID",
    )

    notify_url: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
        comment="退款结果回调通知地址（覆盖 App 默认）",
    )

    extra_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="退款额外数据（JSON）",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="创建时间",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="更新时间",
    )

    refunded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="退款完成时间",
    )

    # 关联：所属支付交易
    payment: Mapped["Payment"] = relationship(back_populates="refunds")


# 更新 Payment 模型添加 refunds 关联
