"""Database migration orchestration for Flowfile.

Handles three startup scenarios:
1. Fresh install — no databases exist → create schema via Alembic
2. Legacy migration — old ``flowfile.db`` exists → create schema, copy data
3. Normal startup — ``flowfile_catalog.db`` exists → run pending Alembic migrations
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

import shared.storage_config as _storage_config

logger = logging.getLogger(__name__)

BATCH_SIZE = 500



def _get_base_dir() -> Path:
    """Resolve the base directory for alembic files.

    Handles PyInstaller frozen builds where files are extracted to
    ``sys._MEIPASS``.
    """
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "flowfile_core"  # type: ignore[attr-defined]
    return Path(__file__).resolve().parent.parent


def _get_alembic_config() -> Config:
    """Build an Alembic Config pointing to our alembic directory and database."""
    base = _get_base_dir()
    cfg = Config(str(base / "alembic.ini"))
    cfg.set_main_option("script_location", str(base / "alembic"))
    cfg.set_main_option("sqlalchemy.url", _storage_config.get_database_url())
    return cfg


def _catalog_db_exists() -> bool:
    """Check whether the new ``flowfile_catalog.db`` file already exists."""
    url = _storage_config.get_database_url()
    if "sqlite" in url:
        db_path = Path(url.replace("sqlite:///", ""))
        return db_path.exists()
    return False


def run_alembic_upgrade() -> None:
    """Run all pending Alembic migrations up to *head*."""
    cfg = _get_alembic_config()
    command.upgrade(cfg, "head")
    logger.info("Alembic migrations applied successfully")


def _compute_table_order(inspector, tables: set[str]) -> list[str]:
    """Topological sort of *tables* based on foreign-key dependencies.

    Uses Kahn's algorithm.  Self-referential tables and any cycles are
    appended at the end.
    """
    deps: dict[str, set[str]] = {t: set() for t in tables}
    for table_name in tables:
        for fk in inspector.get_foreign_keys(table_name):
            referred = fk["referred_table"]
            if referred in tables and referred != table_name:
                deps[table_name].add(referred)

    ordered: list[str] = []
    no_deps = [t for t in sorted(tables) if not deps[t]]
    while no_deps:
        t = no_deps.pop(0)
        ordered.append(t)
        for other in sorted(deps):
            if other in ordered or other in no_deps:
                continue
            deps[other].discard(t)
            if not deps[other]:
                no_deps.append(other)

    remaining = [t for t in sorted(tables) if t not in ordered]
    ordered.extend(remaining)
    return ordered


def _migrate_table(
    table_name: str,
    old_engine: Engine,
    new_engine: Engine,
    old_inspector,
    new_inspector,
) -> None:
    """Copy rows from *old* table into *new* table with dynamic column mapping.

    * Columns in both schemas are copied.
    * Old-only columns are silently skipped.
    * New-only columns use their schema defaults.
    """
    old_columns = {c["name"] for c in old_inspector.get_columns(table_name)}
    new_columns = {c["name"] for c in new_inspector.get_columns(table_name)}

    common_columns = sorted(old_columns & new_columns)
    if not common_columns:
        logger.warning("Table '%s': no common columns, skipping", table_name)
        return

    skipped_old = old_columns - new_columns
    if skipped_old:
        logger.info("Table '%s': skipping old columns not in new schema: %s", table_name, skipped_old)

    new_only = new_columns - old_columns
    if new_only:
        logger.info("Table '%s': new columns (will use defaults): %s", table_name, new_only)

    col_list = ", ".join(common_columns)

    with old_engine.connect() as old_conn:
        rows = old_conn.execute(text(f"SELECT {col_list} FROM {table_name}")).fetchall()  # noqa: S608

    if not rows:
        logger.info("Table '%s': no rows to migrate", table_name)
        return

    placeholders = ", ".join(f":{c}" for c in common_columns)
    insert_sql = text(f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})")  # noqa: S608

    with new_engine.connect() as new_conn:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = [dict(zip(common_columns, row, strict=False)) for row in rows[i : i + BATCH_SIZE]]
            new_conn.execute(insert_sql, batch)
        new_conn.commit()

    logger.info("Table '%s': migrated %d rows", table_name, len(rows))


def migrate_data_from_legacy_db() -> None:
    """Copy all data from the old ``flowfile.db`` into ``flowfile_catalog.db``.

    Uses dynamic column mapping so schema differences are handled
    gracefully.  The old database is **never modified**.
    """
    legacy_path = _storage_config.get_legacy_database_path()
    if legacy_path is None:
        logger.info("No legacy database found, skipping data migration")
        return

    new_url = _storage_config.get_database_url()
    old_url = f"sqlite:///{legacy_path}"

    old_engine = create_engine(old_url, connect_args={"check_same_thread": False})
    new_engine = create_engine(new_url, connect_args={"check_same_thread": False})

    try:
        old_inspector = inspect(old_engine)
        new_inspector = inspect(new_engine)

        old_tables = set(old_inspector.get_table_names())
        new_tables = set(new_inspector.get_table_names())

        tables_to_migrate = old_tables & new_tables
        tables_to_migrate.discard("alembic_version")

        migration_order = _compute_table_order(new_inspector, tables_to_migrate)

        logger.info("Migrating data from %s to new catalog database", legacy_path)
        logger.info("Tables to migrate: %s", migration_order)

        # Disable FK checks for the duration of the data copy
        with new_engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.commit()

        for table_name in migration_order:
            try:
                _migrate_table(table_name, old_engine, new_engine, old_inspector, new_inspector)
            except Exception:
                logger.exception(
                    "WARNING: Failed to migrate table '%s'. "
                    "Data for this table was NOT copied. "
                    "You may need to re-enter this data manually.",
                    table_name,
                )

        # Re-enable FK checks
        with new_engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys = ON"))
            conn.commit()

        logger.info("Data migration completed")
    finally:
        old_engine.dispose()
        new_engine.dispose()



def run_startup_migration() -> None:
    """Determine the startup scenario and act accordingly.

    Called once during application startup, before the FastAPI app serves
    requests.

    Scenarios:
    1. ``flowfile_catalog.db`` exists → run pending Alembic migrations
    2. ``flowfile.db`` exists (legacy) → create schema, copy data
    3. Neither exists (fresh install) → create schema from scratch
    """
    catalog_exists = _catalog_db_exists()
    legacy_path = _storage_config.get_legacy_database_path()

    if catalog_exists:
        logger.info("Existing catalog database detected, checking for pending migrations")
        run_alembic_upgrade()

    elif legacy_path is not None:
        logger.info(
            "Legacy database found at %s. Creating new catalog database and migrating data.",
            legacy_path,
        )
        run_alembic_upgrade()
        migrate_data_from_legacy_db()

    else:
        logger.info("Fresh installation detected, creating database schema")
        run_alembic_upgrade()
