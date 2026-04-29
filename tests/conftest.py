"""
测试基础设施：数据库连接、Session fixture（事务回滚隔离）
"""

import asyncio
import os

# ── 在导入任何 gateway 模块之前设置环境变量 ──
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test_fake_key_for_testing")
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "whsec_fake_secret_for_testing")

from gateway.core.settings import get_settings  # noqa: E402

get_settings.cache_clear()

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

from gateway.core.models import Base  # noqa: E402

_DB_USER = os.environ.get("DB_USER", "postgres")
_DB_PASSWORD = os.environ.get("DB_PASSWORD", "dev_password")
_DB_HOST = os.environ.get("DB_HOST", "localhost")
_DB_PORT = os.environ.get("DB_PORT", "5432")
_DB_NAME = os.environ.get("TEST_DB_NAME", "gateway_test")

TEST_DB_URL = (
    f"postgresql+asyncpg://{_DB_USER}:{_DB_PASSWORD}"
    f"@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
)


@pytest.fixture(scope="session")
def _db_tables():
    """Session-scoped: 建表一次（使用独立事件循环，避免跨 loop 问题）。"""

    async def _setup():
        eng = create_async_engine(TEST_DB_URL)
        async with eng.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        await eng.dispose()

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_setup())
    loop.close()
    yield


@pytest.fixture
async def session(_db_tables):
    """
    Per-test async session：
    - 每个测试创建独立连接（NullPool 避免跨 loop 问题）
    - commit 替换为 flush（数据可见但事务不提交）
    - 测试结束 rollback，数据全部回滚
    """
    eng = create_async_engine(TEST_DB_URL, poolclass=NullPool)
    conn = await eng.connect()
    trans = await conn.begin()

    async_session = AsyncSession(conn, expire_on_commit=False)
    async_session.commit = async_session.flush  # type: ignore[assignment]

    yield async_session

    await async_session.close()
    if trans.is_active:
        await trans.rollback()
    await conn.close()
    await eng.dispose()
