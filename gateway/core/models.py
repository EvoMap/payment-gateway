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
    text,
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

    payments: Mapped[list["Payment"]] = relationship(back_populates="app")
    webhook_deliveries: Mapped[list["WebhookDelivery"]] = relationship(
        back_populates="app"
    )
    customers: Mapped[list["Customer"]] = relationship(back_populates="app")
    plans: Mapped[list["Plan"]] = relationship(back_populates="app")
    subscriptions: Mapped[list["Subscription"]] = relationship(back_populates="app")


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint(
            "app_id",
            "external_user_id",
            "provider",
            name="uq_customers_app_external_user_provider",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="客户ID（主键）",
    )
    app_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("apps.id", ondelete="RESTRICT"),
        nullable=False,
        comment="所属应用ID",
    )
    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider", create_constraint=False),
        nullable=False,
        comment="渠道标识",
    )
    external_user_id: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="调用方用户标识"
    )
    provider_customer_id: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="渠道侧客户ID（如 Stripe cus_xxx）"
    )
    email: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="用户邮箱"
    )
    meta: Mapped[dict | None] = mapped_column(
        "metadata", JSONB, nullable=True, comment="额外元数据"
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

    app: Mapped["App"] = relationship(back_populates="customers")
    subscriptions: Mapped[list["Subscription"]] = relationship(
        back_populates="customer"
    )


class Plan(Base):
    __tablename__ = "plans"
    __table_args__ = (
        UniqueConstraint(
            "app_id", "slug", "provider", name="uq_plans_app_slug_provider"
        ),
        UniqueConstraint(
            "app_id", "provider_product_id", name="uq_plans_app_provider_product_id"
        ),
        CheckConstraint(
            "interval IN ('week', 'month', 'quarter', 'year')",
            name="ck_plans_interval_valid",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="计划ID（主键）",
    )
    app_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("apps.id", ondelete="RESTRICT"),
        nullable=False,
        comment="所属应用ID",
    )
    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider", create_constraint=False),
        nullable=False,
        comment="渠道标识",
    )
    slug: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="计划标识（如 premium）"
    )
    name: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="显示名称"
    )
    description: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="计划描述"
    )
    amount: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="金额（最小货币单位）"
    )
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency", create_constraint=False),
        nullable=False,
        comment="币种",
    )
    interval: Mapped[str] = mapped_column(
        String(32), nullable=False, comment="计费周期（week/month/quarter/year）"
    )
    interval_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, comment="间隔数"
    )
    provider_product_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧商品ID（如 Stripe prod_xxx）"
    )
    provider_price_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="当前活跃渠道侧价格ID（如 Stripe price_xxx）"
    )
    tier: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="等级（用于升降级判断）"
    )
    features: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, comment="可选元数据"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, comment="是否激活"
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

    app: Mapped["App"] = relationship(back_populates="plans")
    subscriptions: Mapped[list["Subscription"]] = relationship(
        back_populates="plan", foreign_keys="[Subscription.plan_id]"
    )


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('incomplete', 'incomplete_expired', 'active', 'past_due', "
            "'canceled', 'unpaid', 'paused', 'trialing')",
            name="ck_subscriptions_status_valid",
        ),
        Index("ix_subscriptions_customer_status", "customer_id", "status"),
        Index("ix_subscriptions_provider_sub_id", "provider_subscription_id"),
        Index("ix_subscriptions_app_created_at", "app_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="订阅ID（主键）",
    )
    app_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("apps.id", ondelete="RESTRICT"),
        nullable=False,
        comment="所属应用ID",
    )
    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider", create_constraint=False),
        nullable=False,
        comment="渠道标识",
    )
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("customers.id", ondelete="RESTRICT"),
        nullable=False,
        comment="客户ID",
    )
    plan_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("plans.id", ondelete="RESTRICT"),
        nullable=False,
        comment="当前计划ID",
    )
    provider_subscription_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧订阅ID（如 Stripe sub_xxx）"
    )
    provider_checkout_session_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧初始 Checkout Session ID"
    )
    provider_price_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="该订阅实际使用的渠道侧价格ID"
    )

    amount: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="订阅金额快照（创建时从 Plan 复制）"
    )
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency", create_constraint=False),
        nullable=False,
        comment="币种快照",
    )

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, comment="订阅状态"
    )
    current_period_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="当前周期开始时间"
    )
    current_period_end: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="当前周期结束时间"
    )
    cancel_at_period_end: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, comment="是否在周期末取消"
    )
    canceled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="取消时间"
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="结束时间"
    )

    trial_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="试用期开始时间"
    )
    trial_end: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="试用期结束时间"
    )

    last_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="最后一次处理的事件时间戳"
    )

    pending_plan_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("plans.id", ondelete="SET NULL"),
        nullable=True,
        comment="待生效的目标计划ID（降级场景，周期末切换）",
    )
    pending_plan_change_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="计划变更生效时间（= current_period_end）",
    )
    provider_schedule_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="渠道侧 Schedule ID（如 Stripe sub_sched_xxx）",
    )

    notify_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, comment="订阅事件回调地址"
    )
    meta: Mapped[dict | None] = mapped_column(
        "metadata", JSONB, nullable=True, comment="额外元数据"
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

    app: Mapped["App"] = relationship(back_populates="subscriptions")
    customer: Mapped["Customer"] = relationship(back_populates="subscriptions")
    plan: Mapped["Plan"] = relationship(
        back_populates="subscriptions", foreign_keys=[plan_id]
    )
    pending_plan: Mapped["Plan | None"] = relationship(foreign_keys=[pending_plan_id])


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
        Index(
            "ix_payments_app_external_user_status",
            "app_id",
            "external_user_id",
            "status",
        ),
        Index("ix_payments_subscription_id", "subscription_id", "created_at"),
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
        Enum(Provider, name="provider", create_constraint=False),
        nullable=False,
        comment="支付渠道/提供方",
    )

    amount: Mapped[int] = mapped_column(
        nullable=False, comment="支付金额（最小货币单位，如分）"
    )
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency", create_constraint=False),
        nullable=False,
        comment="币种",
    )

    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status", create_constraint=False),
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

    external_user_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="调用方用户标识（可选，用于并发控制）"
    )
    subscription_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("subscriptions.id", ondelete="SET NULL"),
        nullable=True,
        comment="关联的订阅ID（订阅产生的支付记录）",
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

    app: Mapped["App"] = relationship(back_populates="payments")
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
        Index("ix_callbacks_source", "source_type", "source_id", "received_at"),
        Index("ix_callbacks_status_received_at", "status", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="回调记录ID（主键）",
    )

    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider", create_constraint=False),
        nullable=False,
        comment="回调来源渠道/提供方",
    )
    provider_event_id: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="渠道侧事件ID（同渠道唯一，用于幂等）"
    )

    provider_txn_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="渠道侧交易号/流水号（便于关联）"
    )

    source_type: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="来源类型：payment / refund / subscription",
    )
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        comment="来源实体 ID",
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, comment="回调原始报文（JSON）"
    )

    status: Mapped[CallbackStatus] = mapped_column(
        Enum(CallbackStatus, name="callback_status", create_constraint=False),
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


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "app_id", "event_id", name="uq_webhook_deliveries_app_event_id"
        ),
        Index(
            "ix_webhook_deliveries_worker_poll",
            "status",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("status IN ('pending', 'failed', 'processing')"),
        ),
        Index("ix_webhook_deliveries_app_created_at", "app_id", "created_at"),
        Index(
            "ix_webhook_deliveries_source",
            "source_type",
            "source_id",
            "created_at",
        ),
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

    source_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="payment",
        comment="事件来源类型：payment / refund / subscription",
    )
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        comment="来源实体 ID（Payment.id / Refund.id / Subscription.id）",
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
        Enum(DeliveryStatus, name="delivery_status", create_constraint=False),
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

    app: Mapped["App"] = relationship(back_populates="webhook_deliveries")


class Refund(Base):
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
        Enum(RefundStatus, name="refund_status", create_constraint=False),
        nullable=False,
        default=RefundStatus.pending,
        comment="退款状态",
    )

    provider: Mapped[Provider] = mapped_column(
        Enum(Provider, name="provider", create_constraint=False),
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

    payment: Mapped["Payment"] = relationship(back_populates="refunds")
