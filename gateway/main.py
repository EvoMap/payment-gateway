"""
FastAPI 主应用入口
"""

import asyncio
import traceback
import structlog
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from pathlib import Path

from pydantic import ValidationError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from gateway.db import close_db, init_db
from gateway.core.settings import get_settings
from gateway.core.bootstrap import reset_tables
from gateway.core.logging import configure_logging
from gateway.core.exceptions import BaseAPIException
from gateway.core.responses import (
    error_response,
    bad_request_response,
    validation_error_response,
    internal_server_response,
    service_unavailable_response,
    success_response,
)

settings = get_settings()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    configure_logging()
    await reset_tables()
    await init_db()
    logger.info(
        "已启用的支付渠道",
        allowed_providers=settings.allowed_providers,
    )
    yield
    await close_db()


app = FastAPI(
    title="Payment Gateway",
    description="统一支付网关服务（Stripe）— 支持一次性支付 + 订阅",
    version="2.0.0",
    lifespan=lifespan,
    swagger_ui_parameters={
        "persistAuthorization": True,
    },
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    from fastapi.openapi.utils import get_openapi

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    openapi_schema["components"]["securitySchemes"] = {
        "X-API-Key": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "应用的 API Key（从管理 API 创建应用时获取）",
        }
    }

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/test", include_in_schema=False)
async def api_test_page():
    html_path = _STATIC_DIR / "api_test.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ===== 全局异常处理器 =====


@app.exception_handler(BaseAPIException)
async def base_api_exception_handler(request: Request, exc: BaseAPIException):
    logger.error(
        "接口异常",
        path=request.url.path,
        method=request.method,
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        details=exc.details,
    )
    return error_response(
        msg=exc.message, code=exc.code, data=exc.details, status_code=exc.status_code
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(
        "请求参数校验失败",
        path=request.url.path,
        method=request.method,
        errors=exc.errors(),
    )
    error_details = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"])
        error_details.append(
            {"field": field, "message": error["msg"], "type": error["type"]}
        )
    return bad_request_response(msg="请求参数验证失败", code=4000, data=error_details)


@app.exception_handler(ValidationError)
async def pydantic_validation_exception_handler(request: Request, exc: ValidationError):
    logger.warning(
        "数据模型校验失败",
        path=request.url.path,
        method=request.method,
        errors=exc.errors(),
    )
    error_details = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"])
        error_details.append(
            {"field": field, "message": error["msg"], "type": error["type"]}
        )
    return validation_error_response(msg="数据验证失败", code=4220, data=error_details)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(
        "未处理的异常",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        traceback=traceback.format_exc(),
    )
    error_details = None
    if settings.debug:
        error_details = {
            "error": str(exc),
            "type": type(exc).__name__,
            "traceback": traceback.format_exc(),
        }
    return internal_server_response(msg="服务器内部错误", code=5000, data=error_details)


# ===== 基础路由 =====


@app.get("/")
async def root():
    return success_response(data={"service": "payment-gateway", "version": "2.0.0"})


@app.get("/health")
async def health_check():
    from gateway.db import engine as db_engine

    if db_engine is None:
        return service_unavailable_response(
            msg="服务降级", data={"status": "degraded", "db": "not_initialized"}
        )

    try:
        async with db_engine.connect() as conn:
            await asyncio.wait_for(
                conn.execute(text("SELECT 1")),
                timeout=3.0,
            )
    except asyncio.TimeoutError:
        return service_unavailable_response(
            msg="服务降级", data={"status": "degraded", "db": "timeout"}
        )
    except Exception:
        return service_unavailable_response(
            msg="服务降级", data={"status": "degraded", "db": "error"}
        )

    return success_response(data={"status": "ok"})


# ===== 路由注册 =====
from .routers import payments, callbacks, admin, plans, subscriptions

app.include_router(payments.router, prefix="/v1", tags=["payments"])
app.include_router(callbacks.router, prefix="/v1/callbacks", tags=["callbacks"])
app.include_router(admin.router, prefix="/v1/admin", tags=["admin"])
app.include_router(plans.router, prefix="/v1", tags=["plans"])
app.include_router(subscriptions.router, prefix="/v1", tags=["subscriptions"])
