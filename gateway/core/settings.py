"""
应用配置管理（pydantic-settings）
"""

from __future__ import annotations
from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from gateway.core.constants import Provider

_VALID_PROVIDERS = {p.value for p in Provider}


class Settings(BaseSettings):
    """应用配置（从环境变量加载）"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # 数据库配置
    database_url: str = ""
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "gateway"
    db_password: str = "dev_password"
    db_name: str = "gateway"
    db_echo: bool = False
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_recycle: int = 1800
    need_reset_database: bool = False

    # 允许的支付渠道（接受逗号分隔字符串或 JSON 数组）
    allowed_providers: Annotated[list[str], NoDecode] = Field(default=["stripe"])

    @field_validator("allowed_providers", mode="before")
    @classmethod
    def parse_allowed_providers(cls, v):
        if isinstance(v, str):
            v = [p.strip() for p in v.split(",") if p.strip()]
        unknown = set(v) - _VALID_PROVIDERS
        if unknown:
            raise ValueError(
                f"不支持的支付渠道: {unknown}，可选值: {_VALID_PROVIDERS}"
            )
        return v

    # Stripe 配置
    stripe_secret_key: str
    stripe_webhook_secret: str

    # 应用配置
    debug: bool = False
    log_level: str = "INFO"
    payment_expire_minutes_default: int = Field(default=30, ge=1, le=24 * 60)

    # Worker 配置
    worker_poll_interval: int = 5
    worker_batch_size: int = 10
    worker_max_retries: int = 10
    worker_concurrency: int = 5

    # 订阅配置
    subscription_checkout_expire_minutes: int = Field(default=60, ge=30, le=1440)
    subscription_single_active: bool = Field(default=True)
    subscription_incomplete_cleanup_minutes: int = Field(default=120, ge=60, le=2880)
    subscription_cleanup_interval: int = Field(default=300, ge=60)

    # 应用层续费配置（WeChat/Alipay 订阅）
    renewal_scan_interval: int = Field(default=60, ge=10, description="续费扫描间隔（秒）")
    renewal_grace_interval: int = Field(default=300, ge=60, description="宽限期检查间隔（秒）")
    renewal_reminder_interval: int = Field(default=300, ge=60, description="续费提醒检查间隔（秒）")
    renewal_advance_days: int = Field(default=3, ge=1, le=14, description="提前几天创建续费订单")
    renewal_grace_period_days: int = Field(default=3, ge=1, le=14, description="到期后宽限天数")
    renewal_max_notifications: int = Field(default=3, ge=1, le=10, description="每周期最多续费通知次数")
    renewal_notification_interval_hours: int = Field(default=24, ge=1, description="续费通知间隔（小时）")
    renewal_payment_expire_minutes: int = Field(default=1440, ge=30, le=4320, description="续费支付链接过期时间（分钟）")

    # Webhook HMAC 签名密钥 (用于对外投递 webhook 进行签名)
    webhook_signing_secret: str = ""


@lru_cache
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()
