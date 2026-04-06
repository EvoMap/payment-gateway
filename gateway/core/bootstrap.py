from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateIndex, CreateColumn

from gateway.db import get_database_url
from gateway.core.settings import get_settings


# 索引迁移：旧索引名 -> 需要删除后由 create_all 重建
_INDEX_MIGRATIONS = [
    {
        "old": "ix_webhook_deliveries_status_next_attempt_at",
        "table": "webhook_deliveries",
    },
]


async def _migrate_schema(engine) -> None:
    """轻量级 schema 迁移（无 Alembic 环境）

    1. 为已存在的表补建模型中新增的列
    2. 删除已变更的旧索引
    3. 补建缺失的新索引
    """
    from gateway.core.models import Base

    async with engine.begin() as conn:
        def _do_migrate(connection):
            inspector = inspect(connection)
            existing_tables = set(inspector.get_table_names())

            # --- 补建缺失的列 ---
            for table_name, table_obj in Base.metadata.tables.items():
                if table_name not in existing_tables:
                    continue
                existing_cols = {
                    c["name"] for c in inspector.get_columns(table_name)
                }
                for col in table_obj.columns:
                    if col.name not in existing_cols:
                        col_ddl = CreateColumn(col).compile(
                            dialect=connection.dialect
                        )
                        connection.execute(text(
                            f'ALTER TABLE "{table_name}" ADD COLUMN {col_ddl}'
                        ))
                        print(f"🆕 已补建列 {table_name}.{col.name}")

            # --- 删除旧索引 ---
            for migration in _INDEX_MIGRATIONS:
                table = migration["table"]
                old_name = migration["old"]
                if table not in existing_tables:
                    continue
                existing = {idx["name"] for idx in inspector.get_indexes(table)}
                if old_name in existing:
                    connection.execute(text(f'DROP INDEX IF EXISTS "{old_name}"'))
                    print(f"🔄 已删除旧索引 {old_name}")

            # --- 补建缺失的索引 ---
            for table_name, table_obj in Base.metadata.tables.items():
                if table_name not in existing_tables:
                    continue
                existing = {idx["name"] for idx in inspector.get_indexes(table_name)}
                for idx in table_obj.indexes:
                    if idx.name not in existing:
                        connection.execute(CreateIndex(idx))
                        print(f"🆕 已补建索引 {idx.name} (表 {table_name})")

        await conn.run_sync(_do_migrate)


async def reset_tables() -> None:
    """根据配置创建或重置数据库表"""

    settings = get_settings()

    # 确保导入所有模型
    from gateway.core.models import Base

    # 创建异步引擎
    engine = create_async_engine(get_database_url(), echo=False)

    if settings.need_reset_database:
        # 强制重置数据库：删除所有表后重新创建
        print("🗄️  正在重置数据库表...")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        print("✅ 数据库表重置完成！")
    else:
        # 检查表是否存在，不存在才创建
        print("🔍 检查数据库表...")

        async with engine.begin() as conn:
            def check_tables_exist(connection):
                inspector = inspect(connection)
                existing_tables = inspector.get_table_names()
                required_tables = Base.metadata.tables.keys()
                return set(required_tables).issubset(set(existing_tables))

            tables_exist = await conn.run_sync(check_tables_exist)

        # 外层 conn 已释放，后续操作各自独立拿连接
        if tables_exist:
            print("✅ 数据库表已存在，检查 schema 迁移...")
            await _migrate_schema(engine)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            print("🗄️  数据库表不存在，正在创建...")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            print("✅ 数据库表创建完成！")

    await engine.dispose()
