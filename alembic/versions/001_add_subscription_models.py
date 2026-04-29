"""add subscription models (customers, plans, subscriptions) and refactor payment/callback/webhook_delivery

Revision ID: 001_subscription
Revises: None
Create Date: 2026-04-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001_subscription"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # === 新增 customers 表 ===
    op.create_table(
        "customers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "app_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("apps.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider", sa.Enum("stripe", name="provider", create_constraint=False), nullable=False),
        sa.Column("external_user_id", sa.String(128), nullable=False),
        sa.Column("provider_customer_id", sa.String(128), nullable=False),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "app_id",
            "external_user_id",
            "provider",
            name="uq_customers_app_external_user_provider",
        ),
    )

    # === 新增 plans 表 ===
    op.create_table(
        "plans",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "app_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("apps.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider", sa.Enum("stripe", name="provider", create_constraint=False), nullable=False),
        sa.Column("slug", sa.String(64), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("amount", sa.Integer, nullable=False),
        sa.Column(
            "currency",
            sa.Enum(
                "USD", "CNY", "HKD", "KRW", "THB", "EUR", "GBP", "JPY", "INR",
                name="currency",
                create_constraint=False,
            ),
            nullable=False,
        ),
        sa.Column("interval", sa.String(32), nullable=False),
        sa.Column("interval_count", sa.Integer, nullable=False, server_default="1"),
        sa.Column("provider_product_id", sa.String(128), nullable=True),
        sa.Column("provider_price_id", sa.String(128), nullable=True),
        sa.Column("tier", sa.Integer, nullable=False, server_default="0"),
        sa.Column("features", postgresql.JSONB, nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("app_id", "slug", "provider", name="uq_plans_app_slug_provider"),
        sa.UniqueConstraint("app_id", "provider_product_id", name="uq_plans_app_provider_product_id"),
        sa.CheckConstraint(
            "interval IN ('week', 'month', 'quarter', 'year')",
            name="ck_plans_interval_valid",
        ),
    )

    # === 新增 subscriptions 表 ===
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "app_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("apps.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider", sa.Enum("stripe", name="provider", create_constraint=False), nullable=False),
        sa.Column(
            "customer_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("customers.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "plan_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("plans.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider_subscription_id", sa.String(128), nullable=True),
        sa.Column("provider_checkout_session_id", sa.String(128), nullable=True),
        sa.Column("provider_price_id", sa.String(128), nullable=True),
        sa.Column("amount", sa.Integer, nullable=False),
        sa.Column(
            "currency",
            sa.Enum(
                "USD", "CNY", "HKD", "KRW", "THB", "EUR", "GBP", "JPY", "INR",
                name="currency",
                create_constraint=False,
            ),
            nullable=False,
        ),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_at_period_end", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trial_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trial_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "pending_plan_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("plans.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("pending_plan_change_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_schedule_id", sa.String(128), nullable=True),
        sa.Column("notify_url", sa.String(2048), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('incomplete', 'incomplete_expired', 'active', 'past_due', "
            "'canceled', 'unpaid', 'paused', 'trialing')",
            name="ck_subscriptions_status_valid",
        ),
    )
    op.create_index(
        "ix_subscriptions_customer_status", "subscriptions", ["customer_id", "status"]
    )
    op.create_index(
        "ix_subscriptions_provider_sub_id", "subscriptions", ["provider_subscription_id"]
    )
    op.create_index(
        "ix_subscriptions_app_created_at", "subscriptions", ["app_id", "created_at"]
    )

    # === 修改 payments 表：新增 external_user_id + subscription_id ===
    op.add_column(
        "payments",
        sa.Column("external_user_id", sa.String(128), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column(
            "subscription_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("subscriptions.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_payments_app_external_user_status",
        "payments",
        ["app_id", "external_user_id", "status"],
    )
    op.create_index(
        "ix_payments_subscription_id",
        "payments",
        ["subscription_id", "created_at"],
    )

    # === 修改 callbacks 表：删除 payment_id，新增 source_type + source_id ===
    # 数据回填
    op.execute(
        "ALTER TABLE callbacks ADD COLUMN IF NOT EXISTS source_type VARCHAR(32)"
    )
    op.execute(
        "ALTER TABLE callbacks ADD COLUMN IF NOT EXISTS source_id UUID"
    )
    op.execute(
        "UPDATE callbacks SET source_type = 'payment', source_id = payment_id WHERE payment_id IS NOT NULL"
    )
    # 删除旧索引和列
    op.drop_index("ix_callbacks_payment_id_received_at", table_name="callbacks", if_exists=True)
    op.drop_column("callbacks", "payment_id")
    op.create_index(
        "ix_callbacks_source", "callbacks", ["source_type", "source_id", "received_at"]
    )

    # === 修改 webhook_deliveries 表：删除 payment_id，新增 source_type + source_id ===
    op.execute(
        "ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS source_type VARCHAR(32) DEFAULT 'payment' NOT NULL"
    )
    op.execute(
        "ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS source_id UUID"
    )
    op.execute(
        "UPDATE webhook_deliveries SET source_type = 'payment', source_id = payment_id WHERE payment_id IS NOT NULL"
    )
    op.drop_index("ix_webhook_deliveries_payment_created_at", table_name="webhook_deliveries", if_exists=True)
    op.drop_column("webhook_deliveries", "payment_id")
    op.create_index(
        "ix_webhook_deliveries_source",
        "webhook_deliveries",
        ["source_type", "source_id", "created_at"],
    )


def downgrade() -> None:
    # === webhook_deliveries: 恢复 payment_id ===
    op.drop_index("ix_webhook_deliveries_source", table_name="webhook_deliveries")
    op.add_column(
        "webhook_deliveries",
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.execute(
        "UPDATE webhook_deliveries SET payment_id = source_id WHERE source_type = 'payment'"
    )
    op.create_index(
        "ix_webhook_deliveries_payment_created_at",
        "webhook_deliveries",
        ["payment_id", "created_at"],
    )
    op.drop_column("webhook_deliveries", "source_type")
    op.drop_column("webhook_deliveries", "source_id")

    # === callbacks: 恢复 payment_id ===
    op.drop_index("ix_callbacks_source", table_name="callbacks")
    op.add_column(
        "callbacks",
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.execute(
        "UPDATE callbacks SET payment_id = source_id WHERE source_type = 'payment'"
    )
    op.create_index(
        "ix_callbacks_payment_id_received_at",
        "callbacks",
        ["payment_id", "received_at"],
    )
    op.drop_column("callbacks", "source_type")
    op.drop_column("callbacks", "source_id")

    # === payments: 删除新增列 ===
    op.drop_index("ix_payments_subscription_id", table_name="payments")
    op.drop_index("ix_payments_app_external_user_status", table_name="payments")
    op.drop_column("payments", "subscription_id")
    op.drop_column("payments", "external_user_id")

    # === 删除新增表 ===
    op.drop_index("ix_subscriptions_app_created_at", table_name="subscriptions")
    op.drop_index("ix_subscriptions_provider_sub_id", table_name="subscriptions")
    op.drop_index("ix_subscriptions_customer_status", table_name="subscriptions")
    op.drop_table("subscriptions")
    op.drop_table("plans")
    op.drop_table("customers")
