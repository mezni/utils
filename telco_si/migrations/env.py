"""Multi-schema aware Alembic migration environment.

Differences from a default Alembic scaffold:

- Uses an **async** engine (asyncpg) via ``async_engine_from_config`` because
  ``DATABASE_URL`` uses the ``+asyncpg`` driver (FR-006).
- ``include_schemas=True`` so autogenerate introspects every target schema.
- Auto-creates the six domain schemas before a fresh upgrade so a brand-new
  instance needs zero manual steps (FR-007, SC-002).
- ``include_object`` restricts introspection to the six domain schemas.
- The ``alembic_version`` table lives in the ``public`` schema
  (``version_table_schema``), matching ``data-model.md``.

The ``DATABASE_URL`` is read from the environment (FR-005) and falls back to
the value configured in ``alembic.ini``.
"""

import asyncio
import os

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config

# Intentionally NO `fileConfig(config.config_file_name)`: alembic.ini declares
# a root logger at WARN which would clobber the application's logging setup
# (INFO) mid-startup and silently swallow the pinned READY readiness line.
# Alembic loggers propagate to the app-configured root handler instead.

TARGET_SCHEMAS = ["catalog", "inventory", "crm", "usage", "billing", "dunning"]

target_metadata = None


def include_object(obj, name, type_, reflected, compare_to):
    """Restrict autogenerate to the six domain schemas."""
    schema = getattr(obj, "schema", None)
    if schema is not None:
        return schema in TARGET_SCHEMAS
    return type_ != "table"


def get_url() -> str:
    url = os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set and no sqlalchemy.url configured in alembic.ini"
        )
    return url


def run_sync_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_object=include_object,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_async() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        async with connectable.begin() as schemas:
            for schema in TARGET_SCHEMAS:
                await schemas.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        await connection.run_sync(lambda conn: run_sync_migrations(conn))

    await connectable.dispose()


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_async())
