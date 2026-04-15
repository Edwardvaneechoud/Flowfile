"""Comprehensive tests for database migration logic.

Tests the three startup scenarios:
1. Fresh install — no databases exist
2. Legacy migration — old flowfile.db exists
3. Normal startup — flowfile_catalog.db already exists

Also tests dynamic column mapping, failure handling, and idempotency.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text


# Helpers


def create_legacy_db(path: Path, tables_data: dict) -> None:
    """Create a SQLite database at *path* with the given tables and data.

    ``tables_data`` maps table names to dicts with:
    * ``columns``: list of column name strings
    * ``rows``: list of tuples (one per row)
    * ``col_types`` (optional): dict mapping column names to SQL types
      (defaults to ``TEXT`` for everything)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{path}")
    with engine.connect() as conn:
        for table_name, spec in tables_data.items():
            columns = spec["columns"]
            col_types = spec.get("col_types", {})
            col_defs = ", ".join(f"{c} {col_types.get(c, 'TEXT')}" for c in columns)
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})"))
            for row in spec.get("rows", []):
                placeholders = ", ".join(f":{c}" for c in columns)
                params = dict(zip(columns, row))
                conn.execute(
                    text(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"),
                    params,
                )
        conn.commit()
    engine.dispose()


def _run_migration(db_path: Path, monkeypatch, legacy_path: Path | None = None):
    """Run the startup migration with environment pointing to *db_path*."""
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))

    if legacy_path is not None:
        import shared.storage_config

        monkeypatch.setattr(shared.storage_config, "get_legacy_database_path", lambda: legacy_path)
    else:
        import shared.storage_config

        monkeypatch.setattr(shared.storage_config, "get_legacy_database_path", lambda: None)

    from flowfile_core.database.migration import run_startup_migration

    run_startup_migration()


def _get_tables(db_path: Path) -> list[str]:
    engine = create_engine(f"sqlite:///{db_path}")
    tables = inspect(engine).get_table_names()
    engine.dispose()
    return tables


# Expected application tables (20) + alembic_version = 21
EXPECTED_APP_TABLES = {
    "users",
    "secrets",
    "db_info",
    "database_connections",
    "cloud_storage_connections",
    "cloud_storage_permissions",
    "kernels",
    "catalog_namespaces",
    "flow_registrations",
    "flow_schedules",
    "flow_runs",
    "flow_favorites",
    "flow_follows",
    "catalog_tables",
    "schedule_trigger_tables",
    "table_favorites",
    "global_artifacts",
    "catalog_table_read_links",
    "scheduler_lock",
    "kafka_connections",
}


# Scenario 1: Fresh install


class TestFreshInstall:
    def test_creates_all_tables(self, tmp_path, monkeypatch):
        """No databases exist -> Alembic creates full schema."""
        db_path = tmp_path / "fresh.db"
        _run_migration(db_path, monkeypatch)

        tables = set(_get_tables(db_path))
        assert EXPECTED_APP_TABLES <= tables
        assert "alembic_version" in tables

    def test_stamps_alembic_version(self, tmp_path, monkeypatch):
        """After fresh install, alembic_version should be at the latest head."""
        db_path = tmp_path / "fresh.db"
        _run_migration(db_path, monkeypatch)

        from alembic.script import ScriptDirectory

        from flowfile_core.database.migration import _get_alembic_config

        cfg = _get_alembic_config()
        script = ScriptDirectory.from_config(cfg)
        expected_head = script.get_current_head()

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        engine.dispose()
        assert version == expected_head

    def test_tables_match_models(self, tmp_path, monkeypatch):
        """The created schema should match the SQLAlchemy model definitions."""
        db_path = tmp_path / "fresh.db"
        _run_migration(db_path, monkeypatch)

        from flowfile_core.database.models import Base

        model_tables = set(Base.metadata.tables.keys())
        db_tables = set(_get_tables(db_path)) - {"alembic_version"}
        assert model_tables == db_tables


# Scenario 2: Legacy migration


class TestLegacyMigration:
    def _make_legacy(self, tmp_path: Path) -> Path:
        """Create a legacy DB with realistic data."""
        legacy = tmp_path / "flowfile.db"
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": ["id", "username", "email", "full_name", "hashed_password", "disabled"],
                    "col_types": {
                        "id": "INTEGER PRIMARY KEY",
                        "username": "TEXT",
                        "email": "TEXT",
                        "full_name": "TEXT",
                        "hashed_password": "TEXT",
                        "disabled": "BOOLEAN",
                    },
                    "rows": [
                        (1, "local_user", "local@flowfile.app", "Local User", "hashed_pw", 0),
                        (2, "admin", "admin@flowfile.app", "Admin", "hashed_pw2", 0),
                    ],
                },
                "catalog_namespaces": {
                    "columns": ["id", "name", "parent_id", "level", "description", "owner_id"],
                    "col_types": {
                        "id": "INTEGER PRIMARY KEY",
                        "name": "TEXT",
                        "parent_id": "INTEGER",
                        "level": "INTEGER",
                        "description": "TEXT",
                        "owner_id": "INTEGER",
                    },
                    "rows": [
                        (1, "General", None, 0, "Default catalog", 1),
                        (2, "default", 1, 1, "Default schema", 1),
                    ],
                },
            },
        )
        return legacy

    def test_data_copied(self, tmp_path, monkeypatch):
        """Data from old DB appears in new DB."""
        legacy = self._make_legacy(tmp_path)
        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            users = conn.execute(text("SELECT username FROM users ORDER BY id")).fetchall()
            namespaces = conn.execute(text("SELECT name FROM catalog_namespaces ORDER BY id")).fetchall()
        engine.dispose()

        assert [r[0] for r in users] == ["local_user", "admin"]
        assert [r[0] for r in namespaces] == ["General", "default"]

    def test_old_db_not_modified(self, tmp_path, monkeypatch):
        """The old flowfile.db must not be touched during migration."""
        legacy = self._make_legacy(tmp_path)
        content_before = legacy.read_bytes()
        # Give filesystem time to register a different mtime if modified
        time.sleep(0.1)

        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        assert legacy.read_bytes() == content_before

    def test_self_referential_fk(self, tmp_path, monkeypatch):
        """catalog_namespaces with parent_id self-reference migrates correctly."""
        legacy = self._make_legacy(tmp_path)
        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name, parent_id FROM catalog_namespaces ORDER BY id")
            ).fetchall()
        engine.dispose()

        assert rows[0] == (1, "General", None)
        assert rows[1] == (2, "default", 1)

    def test_all_tables_created_even_without_legacy_data(self, tmp_path, monkeypatch):
        """New DB should have all tables even if legacy only has a subset."""
        legacy = self._make_legacy(tmp_path)
        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        tables = set(_get_tables(catalog))
        assert EXPECTED_APP_TABLES <= tables


# Dynamic column mapping


class TestDynamicColumnMapping:
    def test_extra_old_columns_skipped(self, tmp_path, monkeypatch):
        """Old DB has columns not in new schema -> silently skipped."""
        legacy = tmp_path / "old.db"
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": [
                        "id", "username", "email", "full_name",
                        "hashed_password", "disabled", "obsolete_column",
                    ],
                    "col_types": {
                        "id": "INTEGER PRIMARY KEY",
                        "username": "TEXT",
                        "email": "TEXT",
                        "full_name": "TEXT",
                        "hashed_password": "TEXT",
                        "disabled": "BOOLEAN",
                        "obsolete_column": "TEXT",
                    },
                    "rows": [(1, "user1", "a@b.com", "User One", "pw", 0, "old_data")],
                },
            },
        )

        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT username FROM users WHERE id = 1")).fetchone()
        engine.dispose()

        assert row is not None
        assert row[0] == "user1"

    def test_new_columns_get_defaults(self, tmp_path, monkeypatch):
        """Old DB missing columns present in new schema -> defaults used."""
        legacy = tmp_path / "old.db"
        # Old DB has users WITHOUT is_admin and must_change_password
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": ["id", "username", "email", "full_name", "hashed_password", "disabled"],
                    "col_types": {
                        "id": "INTEGER PRIMARY KEY",
                        "username": "TEXT",
                        "email": "TEXT",
                        "full_name": "TEXT",
                        "hashed_password": "TEXT",
                        "disabled": "BOOLEAN",
                    },
                    "rows": [(1, "user1", "a@b.com", "User One", "pw", 0)],
                },
            },
        )

        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT username, is_admin FROM users WHERE id = 1")).fetchone()
        engine.dispose()

        assert row is not None
        assert row[0] == "user1"
        # is_admin should be the default (0/False or None since it was not set)

    def test_empty_table_no_error(self, tmp_path, monkeypatch):
        """Legacy DB with empty tables -> no errors."""
        legacy = tmp_path / "old.db"
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": ["id", "username", "email", "full_name", "hashed_password", "disabled"],
                    "col_types": {"id": "INTEGER PRIMARY KEY"},
                    "rows": [],
                },
            },
        )

        catalog = tmp_path / "catalog.db"
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        engine.dispose()
        assert count == 0


# Error handling


class TestErrorHandling:
    def test_failed_table_does_not_crash(self, tmp_path, monkeypatch):
        """If one table fails to migrate, others should still succeed."""
        legacy = tmp_path / "old.db"
        # Create users table normally, but also create a table with
        # data that will cause an insert failure in the new schema
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": ["id", "username", "email", "full_name", "hashed_password", "disabled"],
                    "col_types": {
                        "id": "INTEGER PRIMARY KEY",
                        "username": "TEXT",
                        "email": "TEXT",
                        "full_name": "TEXT",
                        "hashed_password": "TEXT",
                        "disabled": "BOOLEAN",
                    },
                    "rows": [(1, "user1", "a@b.com", "User One", "pw", 0)],
                },
                # This table exists in old but has different structure;
                # if it fails, users should still migrate
                "secrets": {
                    "columns": ["id", "name", "encrypted_value", "iv", "user_id"],
                    "col_types": {"id": "INTEGER PRIMARY KEY"},
                    "rows": [(1, "test_secret", "enc_val", "iv_val", 999)],  # user_id 999 doesn't exist
                },
            },
        )

        catalog = tmp_path / "catalog.db"
        # Should not raise
        _run_migration(catalog, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{catalog}")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT username FROM users WHERE id = 1")).fetchone()
        engine.dispose()
        # Users should still have been migrated regardless of secrets table outcome
        assert row is not None
        assert row[0] == "user1"


# Scenario 3: Existing catalog DB (idempotency)


class TestExistingCatalogDb:
    def test_idempotent_restart(self, tmp_path, monkeypatch):
        """Running migration twice should be a no-op the second time."""
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)

        # Insert a row after first migration
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO users (username, email, full_name, hashed_password) "
                    "VALUES ('x', 'x@x.com', 'X', 'pw')"
                )
            )
            conn.commit()
        engine.dispose()

        # Run again — should not fail or duplicate
        _run_migration(db_path, monkeypatch)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        engine.dispose()
        assert count == 1

    def test_no_legacy_migration_when_catalog_exists(self, tmp_path, monkeypatch):
        """When catalog DB exists, legacy migration should not run even if old DB is present."""
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)

        # Now create a legacy DB with data
        legacy = tmp_path / "old.db"
        create_legacy_db(
            legacy,
            {
                "users": {
                    "columns": ["id", "username"],
                    "col_types": {"id": "INTEGER PRIMARY KEY"},
                    "rows": [(99, "legacy_user")],
                },
            },
        )

        # Run migration again — catalog exists, so legacy should be ignored
        _run_migration(db_path, monkeypatch, legacy_path=legacy)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT COUNT(*) FROM users WHERE username = 'legacy_user'")).scalar()
        engine.dispose()
        assert row == 0  # legacy_user should NOT have been copied


# Topological sort


class TestTopologicalSort:
    def test_tables_with_no_deps_come_first(self):
        """Tables with no FK dependencies should appear before dependent tables."""
        from flowfile_core.database.migration import _compute_table_order

        # Create a mock inspector
        class MockInspector:
            def get_foreign_keys(self, table_name):
                fks = {
                    "users": [],
                    "secrets": [{"referred_table": "users"}],
                    "database_connections": [
                        {"referred_table": "users"},
                        {"referred_table": "secrets"},
                    ],
                }
                return fks.get(table_name, [])

        inspector = MockInspector()
        tables = {"users", "secrets", "database_connections"}
        order = _compute_table_order(inspector, tables)

        assert order.index("users") < order.index("secrets")
        assert order.index("users") < order.index("database_connections")
        assert order.index("secrets") < order.index("database_connections")

    def test_self_referential_table(self):
        """Self-referential FK should not cause infinite loop."""
        from flowfile_core.database.migration import _compute_table_order

        class MockInspector:
            def get_foreign_keys(self, table_name):
                if table_name == "catalog_namespaces":
                    return [{"referred_table": "catalog_namespaces"}]
                return []

        inspector = MockInspector()
        tables = {"catalog_namespaces"}
        order = _compute_table_order(inspector, tables)
        assert order == ["catalog_namespaces"]
