"""
数据库连接与会话管理（Async SQLAlchemy）
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from gateway.core.settings import get_settings
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


settings = get_settings()

# 全局异步引擎（应用启动时创建，关闭时释放）
engine: AsyncEngine | None = None
async_session_factory: async_sessionmaker[AsyncSession] | None = None


def _adapt_url_for_asyncpg(url: str) -> str:
    """将通用 PostgreSQL 连接字符串适配为 asyncpg 兼容格式。

    - 替换 scheme 为 postgresql+asyncpg
    - 将 sslmode 参数转换为 asyncpg 的 ssl 参数
    """
    if url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]
    elif url.startswith("postgres://"):
        url = "postgresql+asyncpg://" + url[len("postgres://"):]

    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    if "sslmode" in params:
        sslmode = params.pop("sslmode")[0]
        ssl_mapping = {
            "disable": "false",
            "allow": "false",
            "prefer": "prefer",
            "require": "require",
            "verify-ca": "verify-ca",
            "verify-full": "verify-full",
        }
        ssl_value = ssl_mapping.get(sslmode, "prefer")
        if ssl_value != "false":
            params["ssl"] = [ssl_value]

    new_query = urlencode({k: v[0] for k, v in params.items()})
    adapted = parsed._replace(query=new_query)
    return urlunparse(adapted)


def get_database_url() -> str:
    """
    获取异步数据库连接 URL。
    优先使用 DATABASE_URL 环境变量（适用于远程数据库），
    未设置时回退到独立字段拼接。
    """
    if settings.database_url:
        return _adapt_url_for_asyncpg(settings.database_url)

    return (
        f"postgresql+asyncpg://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


async def init_db() -> None:
    """初始化数据库引擎与会话工厂（应用启动时调用）"""
    global engine, async_session_factory

    engine = create_async_engine(
        get_database_url(),
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=settings.db_pool_recycle,
    )

    async_session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


async def close_db() -> None:
    """关闭数据库引擎（应用关闭时调用）"""
    global engine
    if engine:
        await engine.dispose()
        engine = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI 依赖注入：获取异步数据库会话

    Usage:
        @app.get("/")
        async def handler(session: AsyncSession = Depends(get_session)):
            ...
    """
    if async_session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_session_ctx() -> AsyncGenerator[AsyncSession, None]:
    """
    上下文管理器：在非 FastAPI 场景（如 worker）中获取异步会话

    Usage:
        async with get_session_ctx() as session:
            ...
    """
    if async_session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
