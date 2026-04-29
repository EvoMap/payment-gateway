"""
统一异常定义
"""

from typing import Any


class IgnoredException(Exception):
    """忽略异常"""


class BaseAPIException(Exception):
    """基础API异常类"""

    def __init__(
        self,
        message: str,
        code: int = 1000,
        status_code: int = 500,
        details: Any = None,
    ):
        self.message = message
        self.code = code  # 业务错误码
        self.status_code = status_code  # HTTP状态码
        self.details = details
        super().__init__(self.message)


class BadRequestException(BaseAPIException):
    """请求参数错误 - 400"""

    def __init__(
        self, message: str = "请求参数错误", code: int = 4000, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=400, details=details)


class UnauthorizedException(BaseAPIException):
    """未授权 - 401"""

    def __init__(
        self,
        message: str = "未授权，请提供有效的认证信息",
        code: int = 4010,
        details: Any = None,
    ):
        super().__init__(message=message, code=code, status_code=401, details=details)


class ForbiddenException(BaseAPIException):
    """禁止访问 - 403"""

    def __init__(
        self, message: str = "禁止访问", code: int = 4030, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=403, details=details)


class NotFoundException(BaseAPIException):
    """资源不存在 - 404"""

    def __init__(
        self, message: str = "资源不存在", code: int = 4040, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=404, details=details)


class ConflictException(BaseAPIException):
    """资源冲突 - 409"""

    def __init__(
        self, message: str = "资源冲突", code: int = 4090, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=409, details=details)


class ValidationException(BaseAPIException):
    """数据验证失败 - 422"""

    def __init__(
        self, message: str = "数据验证失败", code: int = 4220, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=422, details=details)


class InternalServerException(BaseAPIException):
    """内部服务器错误 - 500"""

    def __init__(
        self, message: str = "服务器内部错误", code: int = 5000, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=500, details=details)


class ServiceUnavailableException(BaseAPIException):
    """服务不可用 - 503"""

    def __init__(
        self, message: str = "服务暂时不可用", code: int = 5030, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=503, details=details)


class PaymentProviderException(BaseAPIException):
    """支付渠道异常 - 502"""

    def __init__(
        self, message: str = "支付渠道异常", code: int = 5020, details: Any = None
    ):
        super().__init__(message=message, code=code, status_code=502, details=details)


class ProviderNotAllowedException(BadRequestException):
    """支付渠道未开放 - 400"""

    def __init__(self, provider: str):
        super().__init__(
            message=f"支付渠道 {provider} 未开放，请检查 ALLOWED_PROVIDERS 配置",
            code=4001,
            details={"provider": provider},
        )
