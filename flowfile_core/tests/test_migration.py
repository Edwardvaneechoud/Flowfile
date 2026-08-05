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


# Unknown-revision rollback


class TestUnknownRevisionRollback:
    def test_unknown_revision_rolled_back_to_head(self, tmp_path, monkeypatch, capsys):
        """DB stamped at a revision unknown to local scripts should be re-stamped to head."""
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(text("UPDATE alembic_version SET version_num = '999fakeXYZ'"))
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)
        captured = capsys.readouterr()

        from alembic.script import ScriptDirectory

        from flowfile_core.database.migration import _get_alembic_config

        head = ScriptDirectory.from_config(_get_alembic_config()).get_current_head()

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            ver = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        engine.dispose()

        assert ver == head
        assert "999fakeXYZ" in captured.err

    def test_no_alembic_version_table_is_noop(self, tmp_path, monkeypatch):
        """DB file exists but lacks alembic_version → upgrade runs from base, no crash."""
        db_path = tmp_path / "catalog.db"
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE marker (id INTEGER)"))
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)

        assert "alembic_version" in _get_tables(db_path)


class TestSnapshotDatabase:
    def _make_db(self, path: Path) -> None:
        create_legacy_db(
            path,
            {
                "things": {
                    "columns": ["id", "name"],
                    "col_types": {"id": "INTEGER PRIMARY KEY"},
                    "rows": [(1, "alpha"), (2, "beta")],
                },
            },
        )

    def test_snapshot_is_faithful_copy(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        target = snapshot_database(db_path, "003", "028")

        assert target is not None
        assert target.parent == tmp_path / "db_backups"
        assert "003-to-028" in target.name
        engine = create_engine(f"sqlite:///{target}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id, name FROM things ORDER BY id")).fetchall()
        engine.dispose()
        assert rows == [(1, "alpha"), (2, "beta")]

    def test_none_revision_labelled(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        target = snapshot_database(db_path, None, "028")

        assert target is not None
        assert "none-to-028" in target.name

    def test_disabled_when_keep_zero(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "0")
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        assert snapshot_database(db_path, "003", "028") is None
        assert not (tmp_path / "db_backups").exists()

    def test_invalid_keep_falls_back_to_default(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "not-a-number")
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        assert snapshot_database(db_path, "003", "028") is not None

    def _freeze_time(self, monkeypatch) -> None:
        import flowfile_core.database.backup as backup_mod

        fixed = backup_mod.datetime.now(backup_mod.timezone.utc)

        class _Frozen:
            @staticmethod
            def now(tz=None):
                return fixed

        monkeypatch.setattr(backup_mod, "datetime", _Frozen)

    def test_same_second_snapshots_get_unique_names(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        self._freeze_time(monkeypatch)
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        first = snapshot_database(db_path, "003", "028")
        second = snapshot_database(db_path, "003", "028")

        assert first is not None and second is not None
        assert first != second
        assert second.name.endswith("-1.db")
        assert first.exists() and second.exists()

    def test_prune_mtime_tie_protects_newest(self, tmp_path):
        import os as _os

        from flowfile_core.database.backup import _prune

        backup_dir = tmp_path / "db_backups"
        backup_dir.mkdir()
        older = backup_dir / "catalog.001-to-028.20260101T000000Z.db"
        newest = backup_dir / "catalog.001-to-028.20260101T000000Z-1.db"
        older.write_text("old")
        newest.write_text("new")
        # exact mtime tie: '-1' sorts below the unsuffixed name
        _os.utime(older, (1000, 1000))
        _os.utime(newest, (1000, 1000))

        _prune(backup_dir, "catalog", ".db", keep=1, protect=newest)

        assert newest.exists()
        assert not older.exists()

    def test_glob_metachars_in_db_name_still_pruned(self, tmp_path, monkeypatch):
        self._freeze_time(monkeypatch)
        monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "1")
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "my[db].db"
        self._make_db(db_path)

        snapshot_database(db_path, "001", "028")
        newest = snapshot_database(db_path, "002", "028")

        remaining = list((tmp_path / "db_backups").iterdir())
        assert remaining == [newest]

    def test_busy_database_times_out_without_snapshot(self, tmp_path, monkeypatch):
        import sqlite3

        import flowfile_core.database.backup as backup_mod

        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        monkeypatch.setattr(backup_mod, "_BACKUP_TIMEOUT_SECONDS", 0.5)

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)
        locker = sqlite3.connect(db_path)
        locker.execute("BEGIN EXCLUSIVE")
        try:
            assert backup_mod.snapshot_database(db_path, "003", "028") is None
        finally:
            locker.rollback()
            locker.close()

        backup_dir = tmp_path / "db_backups"
        assert not backup_dir.exists() or list(backup_dir.iterdir()) == []

    def test_prune_keeps_newest(self, tmp_path, monkeypatch):
        import os as _os

        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)

        older = [snapshot_database(db_path, "001", "028") for _ in range(3)]
        for i, snap in enumerate(older):
            _os.utime(snap, (1000 + i, 1000 + i))

        monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "2")
        newest = snapshot_database(db_path, "002", "028")

        remaining = sorted(p.name for p in (tmp_path / "db_backups").iterdir())
        assert len(remaining) == 2
        assert newest.name in remaining
        assert older[2].name in remaining  # highest mtime of the older three survives

    def test_never_raises_when_backup_dir_unwritable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from flowfile_core.database.backup import snapshot_database

        db_path = tmp_path / "catalog.db"
        self._make_db(db_path)
        (tmp_path / "db_backups").write_text("not a directory")

        assert snapshot_database(db_path, "003", "028") is None


# Pre-migration snapshot hook


class TestPreMigrationSnapshot:
    def _backups(self, tmp_path: Path) -> list[Path]:
        backup_dir = tmp_path / "db_backups"
        return sorted(backup_dir.iterdir()) if backup_dir.exists() else []

    def test_no_snapshot_on_fresh_install(self, tmp_path, monkeypatch):
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        assert self._backups(tmp_path) == []

    def test_no_snapshot_when_already_at_head(self, tmp_path, monkeypatch):
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        _run_migration(db_path, monkeypatch)
        assert self._backups(tmp_path) == []

    def test_snapshot_taken_when_migration_pending(self, tmp_path, monkeypatch):
        """A DB at an older revision gets snapshotted before the upgrade mutates it."""
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "020")

        _run_migration(db_path, monkeypatch)

        backups = self._backups(tmp_path)
        assert len(backups) == 1
        assert "020-to-" in backups[0].name

        engine = create_engine(f"sqlite:///{backups[0]}")
        with engine.connect() as conn:
            snap_rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        engine.dispose()
        assert snap_rev == "020"  # snapshot preserves the pre-upgrade state

    def test_snapshot_taken_before_unknown_revision_restamp(self, tmp_path, monkeypatch):
        """The snapshot preserves the unknown stamp before _ensure_known_revision rewrites it."""
        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(text("UPDATE alembic_version SET version_num = '999fakeXYZ'"))
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)

        backups = self._backups(tmp_path)
        assert len(backups) == 1
        assert "999fakeXYZ-to-" in backups[0].name

        engine = create_engine(f"sqlite:///{backups[0]}")
        with engine.connect() as conn:
            snap_rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        engine.dispose()
        assert snap_rev == "999fakeXYZ"

    def test_migration_failure_logs_snapshot_path(self, tmp_path, monkeypatch, capsys):
        """A failing upgrade must leave the snapshot in place and log its path."""
        from types import SimpleNamespace

        monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
        from alembic import command

        import flowfile_core.database.migration as migration_mod
        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "020")

        def _boom(cfg, rev):
            raise RuntimeError("upgrade exploded")

        monkeypatch.setattr(migration_mod, "command", SimpleNamespace(upgrade=_boom, stamp=command.stamp))

        with pytest.raises(RuntimeError, match="upgrade exploded"):
            _run_migration(db_path, monkeypatch)
        captured = capsys.readouterr()

        backups = self._backups(tmp_path)
        assert len(backups) == 1
        assert str(backups[0]) in captured.err

    def test_snapshot_disabled_by_env(self, tmp_path, monkeypatch):
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "020")

        monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "0")
        _run_migration(db_path, monkeypatch)

        assert self._backups(tmp_path) == []
        # migration itself still ran to head
        from alembic.script import ScriptDirectory

        head = ScriptDirectory.from_config(_get_alembic_config()).get_current_head()
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            ver = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        engine.dispose()
        assert ver == head


# Migration 029: catalog_tables.scd2_config


class TestScd2ConfigMigration:
    @staticmethod
    def _columns(db_path: Path, table: str) -> set[str]:
        engine = create_engine(f"sqlite:///{db_path}")
        names = {c["name"] for c in inspect(engine).get_columns(table)}
        engine.dispose()
        return names

    def test_fresh_install_has_the_column(self, tmp_path, monkeypatch):
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        assert "scd2_config" in self._columns(db_path, "catalog_tables")

    def test_upgrade_from_028_adds_the_column(self, tmp_path, monkeypatch):
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "028")
        assert "scd2_config" not in self._columns(db_path, "catalog_tables")

        _run_migration(db_path, monkeypatch)
        assert "scd2_config" in self._columns(db_path, "catalog_tables")

    def test_existing_rows_read_back_null(self, tmp_path, monkeypatch):
        """Forward-only: pre-029 tables become non-SCD2 tables, never a half-populated flag."""
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "028")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO catalog_tables (name, owner_id, file_path, storage_format, table_type) "
                    "VALUES ('legacy_tbl', 1, '/tmp/legacy', 'delta', 'physical')"
                )
            )
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            value = conn.execute(text("SELECT scd2_config FROM catalog_tables WHERE name = 'legacy_tbl'")).scalar()
        engine.dispose()
        assert value is None

    def test_downgrade_then_upgrade_round_trips(self, tmp_path, monkeypatch):
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        cfg = _get_alembic_config()

        command.downgrade(cfg, "028")
        assert "scd2_config" not in self._columns(db_path, "catalog_tables")

        command.upgrade(cfg, "029")
        assert "scd2_config" in self._columns(db_path, "catalog_tables")

    def test_upgrade_is_guarded_when_the_column_already_exists(self, tmp_path, monkeypatch):
        """Dev DBs that added the column out of band must not fail the startup upgrade."""
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "028")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE catalog_tables ADD COLUMN scd2_config TEXT"))
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)
        assert "scd2_config" in self._columns(db_path, "catalog_tables")


# Migration 031: database_connections key-pair auth columns


class TestKeyPairColumnsMigration:
    _NEW_COLUMNS = ("auth_method", "private_key_id", "private_key_passphrase_id")

    @staticmethod
    def _columns(db_path: Path, table: str) -> set[str]:
        engine = create_engine(f"sqlite:///{db_path}")
        names = {c["name"] for c in inspect(engine).get_columns(table)}
        engine.dispose()
        return names

    def test_fresh_install_has_the_columns(self, tmp_path, monkeypatch):
        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        columns = self._columns(db_path, "database_connections")
        assert set(self._NEW_COLUMNS) <= columns

    def test_upgrade_from_030_adds_the_columns(self, tmp_path, monkeypatch):
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "030")
        assert not set(self._NEW_COLUMNS) & self._columns(db_path, "database_connections")

        _run_migration(db_path, monkeypatch)
        assert set(self._NEW_COLUMNS) <= self._columns(db_path, "database_connections")

    def test_existing_rows_read_back_null(self, tmp_path, monkeypatch):
        """Pre-031 connections become password connections (NULL auth_method), never half-migrated."""
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "030")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO database_connections (connection_name, database_type, username, user_id) "
                    "VALUES ('legacy_conn', 'postgresql', 'u', 1)"
                )
            )
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT auth_method, private_key_id, private_key_passphrase_id "
                    "FROM database_connections WHERE connection_name = 'legacy_conn'"
                )
            ).one()
        engine.dispose()
        assert tuple(row) == (None, None, None)

    def test_downgrade_then_upgrade_round_trips(self, tmp_path, monkeypatch):
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        _run_migration(db_path, monkeypatch)
        cfg = _get_alembic_config()

        command.downgrade(cfg, "030")
        assert not set(self._NEW_COLUMNS) & self._columns(db_path, "database_connections")

        command.upgrade(cfg, "031")
        assert set(self._NEW_COLUMNS) <= self._columns(db_path, "database_connections")

    def test_upgrade_is_guarded_when_a_column_already_exists(self, tmp_path, monkeypatch):
        """Dev DBs that added a column out of band must not fail the startup upgrade."""
        from alembic import command

        from flowfile_core.database.migration import _get_alembic_config

        db_path = tmp_path / "catalog.db"
        monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
        command.upgrade(_get_alembic_config(), "030")

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE database_connections ADD COLUMN auth_method VARCHAR"))
            conn.commit()
        engine.dispose()

        _run_migration(db_path, monkeypatch)
        assert set(self._NEW_COLUMNS) <= self._columns(db_path, "database_connections")
