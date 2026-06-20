"""Legacy-migration backfill of new-only UUID columns.

A legacy ``flowfile.db`` predates the ``viz_uuid`` / ``dashboard_uuid`` columns
(migration 023). The new schema makes them ``NOT NULL`` (migration 025), so the
copy path must generate a value for every row instead of dropping it. These tests
assert the rows survive the copy with a non-null, unique generated UUID.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text

from flowfile_core.database.migration import run_startup_migration


def _create_legacy_db(path: Path, tables_data: dict) -> None:
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
                params = dict(zip(columns, row, strict=False))
                conn.execute(
                    text(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"),
                    params,
                )
        conn.commit()
    engine.dispose()


def _run_migration(db_path: Path, monkeypatch, legacy_path: Path) -> None:
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
    import shared.storage_config

    monkeypatch.setattr(shared.storage_config, "get_legacy_database_path", lambda: legacy_path)
    run_startup_migration()


def _make_legacy_with_viz_and_dashboard(tmp_path: Path) -> Path:
    legacy = tmp_path / "flowfile.db"
    _create_legacy_db(
        legacy,
        {
            # Legacy schema: NO viz_uuid column.
            "catalog_visualizations": {
                "columns": ["id", "name", "spec_json", "source_type"],
                "col_types": {"id": "INTEGER PRIMARY KEY"},
                "rows": [
                    (1, "Sales chart", "{}", "table"),
                    (2, "Revenue chart", "{}", "table"),
                ],
            },
            # Legacy schema: NO dashboard_uuid column.
            "catalog_dashboards": {
                "columns": ["id", "name", "layout_json", "layout_version"],
                "col_types": {"id": "INTEGER PRIMARY KEY"},
                "rows": [
                    (1, "Exec dashboard", "{}", 1),
                ],
            },
        },
    )
    return legacy


def test_visualizations_copied_with_generated_viz_uuid(tmp_path, monkeypatch):
    legacy = _make_legacy_with_viz_and_dashboard(tmp_path)
    catalog = tmp_path / "catalog.db"
    _run_migration(catalog, monkeypatch, legacy_path=legacy)

    engine = create_engine(f"sqlite:///{catalog}")
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT name, viz_uuid FROM catalog_visualizations ORDER BY id")
        ).fetchall()
    engine.dispose()

    assert [r[0] for r in rows] == ["Sales chart", "Revenue chart"]
    uuids = [r[1] for r in rows]
    assert all(u for u in uuids), "viz_uuid must be backfilled, not null"
    assert len(set(uuids)) == len(uuids), "generated viz_uuid values must be unique"


def test_dashboards_copied_with_generated_dashboard_uuid(tmp_path, monkeypatch):
    legacy = _make_legacy_with_viz_and_dashboard(tmp_path)
    catalog = tmp_path / "catalog.db"
    _run_migration(catalog, monkeypatch, legacy_path=legacy)

    engine = create_engine(f"sqlite:///{catalog}")
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT name, dashboard_uuid FROM catalog_dashboards ORDER BY id")
        ).fetchall()
    engine.dispose()

    assert [r[0] for r in rows] == ["Exec dashboard"]
    assert rows[0][1], "dashboard_uuid must be backfilled, not null"
