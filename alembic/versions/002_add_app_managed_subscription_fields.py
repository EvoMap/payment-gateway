"""add app-managed subscription fields (payment_method, renewal_payment_id, grace_period_end, etc.)

Revision ID: 002_app_managed_renewal
Revises: 001_subscription
Create Date: 2026-05-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "002_app_managed_renewal"
down_revision: Union[str, None] = "001_subscription"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "subscriptions",
        sa.Column(
            "payment_method",
            sa.String(32),
            nullable=False,
            server_default="card",
            comment="支付方式（card/wechat_pay/alipay），决定计费管理模式",
        ),
    )
    op.add_column(
        "subscriptions",
        sa.Column(
            "renewal_payment_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="当前待支付的续费订单ID",
        ),
    )
    op.create_foreign_key(
        "fk_subscriptions_renewal_payment_id",
        "subscriptions",
        "payments",
        ["renewal_payment_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "subscriptions",
        sa.Column(
            "grace_period_end",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="宽限期截止时间",
        ),
    )
    op.add_column(
        "subscriptions",
        sa.Column(
            "renewal_attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="当前周期已发送续费通知次数",
        ),
    )
    op.add_column(
        "subscriptions",
        sa.Column(
            "last_renewal_notified_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="上次发送续费通知时间",
        ),
    )
    op.add_column(
        "subscriptions",
        sa.Column(
            "last_renewed_period_end",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="上次续费推进时的 period_end（幂等性防重复推进）",
        ),
    )

    # 更新 plans 表 CHECK 约束以支持 'day' interval
    op.drop_constraint("ck_plans_interval_valid", "plans", type_="check")
    op.create_check_constraint(
        "ck_plans_interval_valid",
        "plans",
        "interval IN ('day', 'week', 'month', 'quarter', 'year')",
    )

    op.create_index(
        "ix_subscriptions_renewal_scan",
        "subscriptions",
        ["current_period_end"],
        postgresql_where=sa.text(
            "payment_method IN ('wechat_pay', 'alipay') "
            "AND status = 'active' "
            "AND cancel_at_period_end = FALSE "
            "AND renewal_payment_id IS NULL"
        ),
    )

    op.create_index(
        "ix_subscriptions_grace_period",
        "subscriptions",
        ["grace_period_end"],
        postgresql_where=sa.text(
            "payment_method IN ('wechat_pay', 'alipay') "
            "AND status = 'past_due' "
            "AND grace_period_end IS NOT NULL"
        ),
    )

    # GIN 索引支持 JSONB @> 查询（回退查找 old_renewal_txn_ids）
    op.execute(
        "CREATE INDEX ix_subscriptions_meta_gin ON subscriptions "
        "USING gin(metadata jsonb_path_ops) "
        "WHERE payment_method IN ('wechat_pay', 'alipay')"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_subscriptions_meta_gin")
    op.drop_index("ix_subscriptions_grace_period", table_name="subscriptions")
    op.drop_index("ix_subscriptions_renewal_scan", table_name="subscriptions")
    op.drop_column("subscriptions", "last_renewed_period_end")
    op.drop_column("subscriptions", "last_renewal_notified_at")
    op.drop_column("subscriptions", "renewal_attempts")
    op.drop_column("subscriptions", "grace_period_end")
    op.drop_constraint("fk_subscriptions_renewal_payment_id", "subscriptions", type_="foreignkey")
    op.drop_column("subscriptions", "renewal_payment_id")
    op.drop_column("subscriptions", "payment_method")
    # 还原 plans CHECK 约束
    op.drop_constraint("ck_plans_interval_valid", "plans", type_="check")
    op.execute("UPDATE plans SET interval = 'week' WHERE interval = 'day'")
    op.create_check_constraint(
        "ck_plans_interval_valid",
        "plans",
        "interval IN ('week', 'month', 'quarter', 'year')",
    )
