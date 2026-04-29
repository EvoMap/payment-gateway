"""
Provider 适配器工厂（根据 App + Provider 加载配置并实例化）
"""

from gateway.core.constants import Provider
from gateway.core.settings import get_settings
from gateway.core.exceptions import ProviderNotAllowedException
from .base import ProviderAdapter
from .stripe import StripeAdapter, get_stripe_adapter

__all__ = [
    "ProviderAdapter",
    "StripeAdapter",
    "get_stripe_adapter",
    "get_adapter",
    "is_provider_allowed",
]


def is_provider_allowed(provider: Provider | str) -> bool:
    """判断支付渠道是否在 allowed_providers 白名单中"""
    value = provider.value if isinstance(provider, Provider) else provider
    return value in get_settings().allowed_providers


def get_adapter(provider: Provider) -> ProviderAdapter:
    """
    根据 Provider 获取对应的适配器单例

    只有在调用此函数时才会初始化对应的适配器，
    避免启动时就要求所有支付提供商的配置。
    如果 provider 不在 allowed_providers 白名单中，拒绝调用。
    """
    if not is_provider_allowed(provider):
        raise ProviderNotAllowedException(provider=provider.value)

    if provider == Provider.stripe:
        return get_stripe_adapter()
    else:
        raise ValueError(f"Unsupported provider: {provider}")
