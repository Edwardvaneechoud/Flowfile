"""Visualization & dashboard projection: determinism, portable round-trip, prune, hook isolation.

Charts and dashboards round-trip by stable uuid (viz_uuid / dashboard_uuid). A viz carries its
GraphicWalker spec + a portable source ({catalog, schema} namespace + source table by name, or
inline SQL); a dashboard's tiles reference their viz by viz_uuid so the canvas re-links to local
ids on another machine. The base64 PNG thumbnail is never committed. These tests write rows
directly and drive the project_sync hooks, exactly as the catalog service does.
"""

import json
from pathlib import Path
from uuid import uuid4

import yaml

from flowfile_core.catalog import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogDashboard, CatalogTable, CatalogVisualization
from flowfile_core.project import project_sync, projection
from flowfile_core.schemas.catalog_schema import DashboardLayout, DashboardTile

OWNER = 1


def _make_table(name: str, namespace_id: int | None = None) -> int:
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        table = CatalogTable(
            name=name,
            owner_id=OWNER,
            namespace_id=namespace_id,
            table_type="physical",
            storage_format="delta",
            file_path=None,
            schema_json=json.dumps([]),
            column_count=0,
        )
        repo.create_table(table)
        return table.id


def _make_viz(
    name: str,
    *,
    source_type: str = "sql",
    sql_query: str | None = "SELECT 1",
    table_id: int | None = None,
    namespace_id: int | None = None,
    spec: list[dict] | None = None,
) -> tuple[int, str]:
    with get_db_context() as db:
        viz = CatalogVisualization(
            name=name,
            created_by=OWNER,
            source_type=source_type,
            sql_query=sql_query if source_type == "sql" else None,
            catalog_table_id=table_id if source_type == "table" else None,
            namespace_id=namespace_id,
            spec_json=json.dumps(spec if spec is not None else [{"encodings": {}}]),
            spec_gw_version="0.4.5",
            thumbnail_data_url="data:image/png;base64,AAAA",
        )
        db.add(viz)
        db.commit()
        db.refresh(viz)
        return viz.id, viz.viz_uuid


def _make_dashboard(name: str, layout: DashboardLayout) -> tuple[int, str]:
    with get_db_context() as db:
        dashboard = CatalogDashboard(
            name=name,
            created_by=OWNER,
            layout_json=layout.model_dump_json(),
            layout_version=layout.grid.version,
        )
        db.add(dashboard)
        db.commit()
        db.refresh(dashboard)
        return dashboard.id, dashboard.dashboard_uuid


def _viz_yaml(root: Path) -> dict:
    data = yaml.safe_load((root / "visualizations.yaml").read_text(encoding="utf-8"))
    return {v["viz_uuid"]: v for v in data["visualizations"]}


def _dash_yaml(root: Path) -> dict:
    data = yaml.safe_load((root / "dashboards.yaml").read_text(encoding="utf-8"))
    return {d["dashboard_uuid"]: d for d in data["dashboards"]}


def _clear_viz_dashboards() -> None:
    with get_db_context() as db:
        db.query(CatalogDashboard).filter_by(created_by=OWNER).delete()
        db.query(CatalogVisualization).filter_by(created_by=OWNER).delete()
        db.commit()


def _delete_table(name: str) -> None:
    with get_db_context() as db:
        db.query(CatalogTable).filter_by(name=name, owner_id=OWNER).delete()
        db.commit()


def test_sql_visualization_round_trips_byte_identical(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, vuuid = _make_viz("sql-chart", source_type="sql", sql_query="SELECT region, sales FROM s")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Viz SQL", OWNER)
        entry = _viz_yaml(root)[vuuid]
        assert entry["source_type"] == "sql" and entry["sql_query"] == "SELECT region, sales FROM s"
        assert entry["spec"] and entry["spec_gw_version"] == "0.4.5"
        # The PNG thumbnail is never committed; a SQL viz has no source_table.
        assert "thumbnail_data_url" not in entry and "source_table" not in entry

        viz_bytes = (root / "visualizations.yaml").read_bytes()
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "visualizations.yaml").read_bytes() == viz_bytes, "re-projection must be byte-identical"

        # project -> import -> project must round-trip; upsert by viz_uuid must not duplicate.
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        import_project(root, OWNER)
        with get_db_context() as db:
            rows = db.query(CatalogVisualization).filter_by(created_by=OWNER).all()
            assert len(rows) == 1 and rows[0].viz_uuid == vuuid
            assert rows[0].sql_query == "SELECT region, sales FROM s" and rows[0].source_type == "sql"
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "visualizations.yaml").read_bytes() == viz_bytes, "project->import->project must round-trip"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_table_visualization_relinks_source_on_import(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    tname = f"src_{uuid4().hex[:6]}"
    table_id = _make_table(tname)
    _, vuuid = _make_viz(f"viz_{uuid4().hex[:6]}", source_type="table", table_id=table_id)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Viz Table", OWNER)
        entry = _viz_yaml(root)[vuuid]
        assert entry["source_type"] == "table"
        assert entry["source_table"]["name"] == tname and "sql_query" not in entry

        # Drop viz + table; rebuild from files. tables.yaml recreates the table (new id), and the
        # viz re-links to it by its portable {catalog, schema, name}.
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        _delete_table(tname)
        import_project(root, OWNER)
        with get_db_context() as db:
            viz = db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first()
            table = SQLAlchemyCatalogRepository(db).get_table_by_name(tname, None)
            assert viz is not None and table is not None
            assert viz.catalog_table_id == table.id
    finally:
        _clear_viz_dashboards()
        _delete_table(tname)
        project_sync.close_project(OWNER)


def test_dashboard_round_trips_and_relinks_tiles(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("dash-chart", source_type="sql", sql_query="SELECT 1")
    layout = DashboardLayout(tiles=[DashboardTile(id="tile-1", type="viz", viz_id=viz_id, x=0, y=0, w=6, h=4)])
    _, duuid = _make_dashboard("board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Dash", OWNER)
        tile = _dash_yaml(root)[duuid]["layout"]["tiles"][0]
        assert tile["viz_uuid"] == vuuid and "viz_id" not in tile  # portable tile reference

        dash_bytes = (root / "dashboards.yaml").read_bytes()

        # Rebuild: viz + dashboard recreated with new local ids; the tile re-links to the new viz id.
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            new_viz = db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first()
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert new_viz is not None and dashboard is not None
            stored_tile = json.loads(dashboard.layout_json)["tiles"][0]
            assert stored_tile["viz_id"] == new_viz.id and "viz_uuid" not in stored_tile
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "dashboards.yaml").read_bytes() == dash_bytes, "dashboard must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_prune_removes_absent_viz_and_dashboard(tmp_path):
    from flowfile_core.project.normalize import write_yaml

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, vuuid = _make_viz("prune-chart", source_type="sql", sql_query="SELECT 1")
    _, duuid = _make_dashboard("prune-board", DashboardLayout(tiles=[]))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Prune VD", OWNER)
        assert vuuid in _viz_yaml(root) and duuid in _dash_yaml(root)

        write_yaml(root / "visualizations.yaml", {"visualizations": []})
        write_yaml(root / "dashboards.yaml", {"dashboards": []})
        project_sync.reload_from_disk(OWNER, force=True)

        with get_db_context() as db:
            assert db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first() is None
            assert db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first() is None
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_viz_dashboard_hooks_and_failure_isolation(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Hooks VD", OWNER)
        assert _viz_yaml(root) == {} and _dash_yaml(root) == {}

        _, vuuid = _make_viz("hk-viz", source_type="sql", sql_query="SELECT 1")
        project_sync.visualizations_changed(OWNER)
        assert vuuid in _viz_yaml(root)

        _, duuid = _make_dashboard("hk-board", DashboardLayout(tiles=[]))
        project_sync.dashboards_changed(OWNER)
        assert duuid in _dash_yaml(root)

        _clear_viz_dashboards()
        project_sync.visualizations_changed(OWNER)
        project_sync.dashboards_changed(OWNER)
        assert _viz_yaml(root) == {} and _dash_yaml(root) == {}

        def _boom(*_args, **_kwargs):
            raise RuntimeError("projection boom")

        monkeypatch.setattr(projection, "regenerate_visualizations_manifest", _boom)
        project_sync.visualizations_changed(OWNER)  # must not raise
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_viz_dashboard_changed_noop_without_active_project():
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _make_viz("orphan-viz", source_type="sql", sql_query="SELECT 1")
    try:
        project_sync.visualizations_changed(OWNER)  # no active project -> silent no-op
        project_sync.dashboards_changed(OWNER)
    finally:
        _clear_viz_dashboards()
