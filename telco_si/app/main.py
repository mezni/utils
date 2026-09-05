"""FastAPI application entrypoint.

Startup lifecycle (FR-011/FR-013/FR-014, per `contracts/startup-migrations.md`):
1. Poll database connectivity within ``DB_RETRY_WINDOW`` (no fatal crash on a
   transient not-ready state).
2. Run ``alembic upgrade head``; genuine migration errors fail fast.
3. Emit the pinned readiness line ``READY: app listening on {API_HOST}:{API_PORT}``
   and start serving.

Any failure at an earlier step logs a clear error and aborts startup with a
non-zero exit status.
"""

import asyncio
import hashlib
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from . import database
from .config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
)
logger = logging.getLogger("app.startup")

settings = get_settings()

SCHEMAS = ("catalog", "inventory", "crm", "usage", "billing", "dunning")

# Alembic stores no content checksum, so the startup runner keeps a ledger of
# the file hash each applied revision was written with. A mismatch means the
# applied revision was edited after the fact and startup must fail clearly
# (`contracts/startup-migrations.md`, spec.md Edge Cases) instead of running a
# silent drift.
REVISION_CHECKSUM_TABLE = "public.alembic_revision_checksum"


def _discover_root() -> Path:
    """Locate the project root holding ``alembic.ini`` and ``migrations/``.

    The installed wheel puts ``app`` under site-packages, so ``__file__`` cannot
    be trusted in the container; the compose WORKDIR (/app) and the local repo
    both contain ``alembic.ini`` at the current working directory.
    """
    cwd = Path.cwd()
    if (cwd / "alembic.ini").exists():
        return cwd
    source_root = Path(__file__).resolve().parent.parent
    if (source_root / "alembic.ini").exists():
        return source_root
    return cwd


PROJECT_ROOT = _discover_root()


def _alembic_configuration(database_url: str) -> AlembicConfig:
    cfg = AlembicConfig(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


async def _wait_for_database() -> None:
    """Poll connectivity for up to ``DB_RETRY_WINDOW`` seconds (FR-014)."""
    deadline = time.monotonic() + settings.db_retry_window
    while not await database.check_connection():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "Database unreachable after "
                f"{settings.db_retry_window}s; aborting startup."
            )
        logger.info(
            "Database not ready yet, retrying in 1s (%.0fs remaining)...",
            remaining,
        )
        await asyncio.sleep(1)
    logger.info("Database connection established.")


def _revision_checksum(script_path: Path) -> str:
    """sha256 of a migration file's raw bytes."""
    return hashlib.sha256(script_path.read_bytes()).hexdigest()


async def _alembic_version_table_exists() -> bool:
    async with database.engine.connect() as conn:
        result = await conn.execute(
            text("SELECT to_regclass('public.alembic_version') IS NOT NULL")
        )
        return bool(result.scalar_one())


async def _applied_revisions() -> list[str]:
    """Revision identifiers recorded in ``public.alembic_version``."""
    if not await _alembic_version_table_exists():
        return []
    async with database.engine.connect() as conn:
        result = await conn.execute(
            text("SELECT version_num FROM public.alembic_version")
        )
        return [row[0] for row in result]


async def _ensure_checksum_table() -> None:
    async with database.engine.begin() as conn:
        await conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {REVISION_CHECKSUM_TABLE} "
                "(version_num TEXT PRIMARY KEY, checksum TEXT NOT NULL)"
            )
        )


async def _stored_revision_checksums() -> dict[str, str]:
    async with database.engine.connect() as conn:
        result = await conn.execute(
            text(f"SELECT version_num, checksum FROM {REVISION_CHECKSUM_TABLE}")
        )
        return {row[0]: row[1] for row in result}


async def _verify_applied_migration_integrity() -> ScriptDirectory:
    """Fail startup if an already-applied migration file was modified.

    Revisions whose ledger entry predates this feature have no recorded
    checksum and are trusted, then backfilled on the next successful run.
    """
    await _ensure_checksum_table()
    script = ScriptDirectory.from_config(_alembic_configuration(settings.database_url))
    stored = await _stored_revision_checksums()
    for revision in await _applied_revisions():
        current = script.get_revision(revision)
        if current is None:
            raise RuntimeError(
                f"Applied revision {revision!r} is missing from migrations/; "
                "refusing to start."
            )
        file_sha = _revision_checksum(Path(current.path))
        if revision in stored and stored[revision] != file_sha:
            raise RuntimeError(
                f"Modified applied revision {revision!r}: file checksum "
                f"{file_sha[:12]} does not match recorded {stored[revision][:12]}. "
                "Edit applied migrations only by adding a new revision; "
                "refusing to start."
            )
    return script


async def _record_applied_revision_checksums(script: ScriptDirectory) -> None:
    for revision in await _applied_revisions():
        current = script.get_revision(revision)
        checksum = _revision_checksum(Path(current.path))
        async with database.engine.begin() as conn:
            await conn.execute(
                text(
                    f"INSERT INTO {REVISION_CHECKSUM_TABLE} (version_num, checksum) "
                    "VALUES (:revision, :checksum) "
                    "ON CONFLICT (version_num) DO UPDATE "
                    "SET checksum = EXCLUDED.checksum"
                ),
                {"revision": revision, "checksum": checksum},
            )


async def _apply_migrations() -> None:
    """Run ``alembic upgrade head`` (idempotent at the head revision).

    Verifies the integrity of already-applied revisions first (spec.md Edge
    Cases), then records their checksums after a successful run.
    """
    cfg = _alembic_configuration(settings.database_url)
    script = await _verify_applied_migration_integrity()
    try:
        await asyncio.to_thread(lambda: command.upgrade(cfg, "head"))
    except Exception as exc:  # noqa: BLE001 - surface any migration failure
        raise RuntimeError(f"Migrations failed: {exc}") from exc
    await _record_applied_revision_checksums(script)
    logger.info("Migrations applied/up to date.")


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await _wait_for_database()
        await _apply_migrations()
        logger.info(
            "READY: app listening on %s:%s",
            settings.api_host,
            settings.api_port,
        )
    except Exception as exc:
        logger.error("%s", exc)
        raise
    yield
    await database.engine.dispose()


app = FastAPI(
    title="Telco SI — BSS/OSS Reference API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> JSONResponse:
    """Report application readiness and database connectivity (FR-012)."""
    if not await database.check_connection():
        return JSONResponse(
            status_code=503,
            content={"status": "error", "database": "down"},
        )
    return JSONResponse(content={"status": "ok", "database": "up"})
