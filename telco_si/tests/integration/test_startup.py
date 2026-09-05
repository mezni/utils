"""Integration tests for the multi-schema migration framework (US2).

These tests need a live PostgreSQL instance reachable through ``DATABASE_URL``
(e.g. the compose ``db`` service). They assert FR-007 (six schemas exist after
startup) and that the applied migration history is at the head revision.
"""

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import text

from app import database
from app.config import get_settings
from app.main import _alembic_configuration

DOMAIN_SCHEMAS = {"catalog", "inventory", "crm", "usage", "billing", "dunning"}


@pytest.mark.asyncio
async def test_six_domain_schemas_exist():
    async with database.engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('information_schema')"
            )
        )
        schemas = {row[0] for row in result}
    assert DOMAIN_SCHEMAS.issubset(schemas)


@pytest.mark.asyncio
async def test_migration_history_is_at_head():
    settings = get_settings()
    cfg = _alembic_configuration(settings.database_url)
    script = ScriptDirectory.from_config(cfg)
    head = script.get_current_head()
    assert head is not None

    async with database.engine.connect() as conn:
        result = await conn.execute(
            text("SELECT version_num FROM public.alembic_version")
        )
        applied = result.scalar_one()

    assert applied == head


@pytest.mark.asyncio
async def test_migrations_are_idempotent_noop():
    """Re-running ``upgrade head`` at the head must be a stable no-op (FR-009)."""
    import asyncio

    from alembic import command

    settings = get_settings()
    cfg = _alembic_configuration(settings.database_url)

    async def _version():
        async with database.engine.connect() as conn:
            result = await conn.execute(
                text("SELECT version_num FROM public.alembic_version")
            )
            return result.scalar_one()

    before = await _version()
    await asyncio.to_thread(lambda: command.upgrade(cfg, "head"))
    after = await _version()

    assert after == before == "0001"
