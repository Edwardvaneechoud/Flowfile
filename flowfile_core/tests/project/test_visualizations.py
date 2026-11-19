"""Visualization & dashboard projection: determinism, portable round-trip, prune.

Charts and dashboards round-trip by stable uuid (viz_uuid / dashboard_uuid). A viz carries its
GraphicWalker spec + a portable source ({catalog, schema} namespace + source table by name, or
inline SQL); a dashboard's tiles reference their viz by viz_uuid so the canvas re-links to local
ids on another machine. The base64 PNG thumbnail is never committed. These tests write rows
directly.
"""

import base64
import json
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from flowfile_core.catalog import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogDashboard, CatalogTable, CatalogVisualization
from flowfile_core.project import git_ops, importer, project_sync
from flowfile_core.project.normalize import write_yaml
from flowfile_core.project.service import write_export
from flowfile_core.schemas.catalog_schema import (
    DashboardChartFormat,
    DashboardColorAssignment,
    DashboardFilter,
    DashboardKpi,
    DashboardKpiTrend,
    DashboardLayout,
    DashboardTab,
    DashboardTableColumn,
    DashboardTableConfig,
    DashboardTile,
    DashboardTileImage,
)

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
            layout_version=1,
        )
        db.add(dashboard)
        db.commit()
        db.refresh(dashboard)
        return dashboard.id, dashboard.dashboard_uuid


def _viz_bytes(root: Path) -> dict[str, bytes]:
    """Every chart file of the folder, by file name."""
    return {p.name: p.read_bytes() for p in sorted((root / "visualizations").glob("*.visualization.yaml"))}


def _dash_bytes(root: Path) -> dict[str, bytes]:
    """Every dashboard file of the folder, by file name."""
    return {p.name: p.read_bytes() for p in sorted((root / "dashboards").glob("*.dashboard.yaml"))}


def _viz_yaml(root: Path) -> dict:
    return {v["viz_uuid"]: v for v in map(yaml.safe_load, _viz_bytes(root).values())}


def _dash_yaml(root: Path) -> dict:
    return {d["dashboard_uuid"]: d for d in map(yaml.safe_load, _dash_bytes(root).values())}


def _edit_dashboards(root: Path, change) -> None:
    """Rewrite each dashboard file after ``change(entry)``, as an edit by hand would."""
    for path in sorted((root / "dashboards").glob("*.dashboard.yaml")):
        entry = yaml.safe_load(path.read_text(encoding="utf-8"))
        change(entry)
        path.write_text(yaml.safe_dump(entry, sort_keys=False), encoding="utf-8")


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

        viz_bytes = _viz_bytes(root)
        write_export(root, OWNER)
        assert _viz_bytes(root) == viz_bytes, "re-projection must be byte-identical"

        # project -> import -> project must round-trip; upsert by viz_uuid must not duplicate.
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        import_project(root, OWNER)
        with get_db_context() as db:
            rows = db.query(CatalogVisualization).filter_by(created_by=OWNER).all()
            assert len(rows) == 1 and rows[0].viz_uuid == vuuid
            assert rows[0].sql_query == "SELECT region, sales FROM s" and rows[0].source_type == "sql"
        write_export(root, OWNER)
        assert _viz_bytes(root) == viz_bytes, "project->import->project must round-trip"
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
    tile_model = DashboardTile(
        id="tile-1", type="viz", viz_id=viz_id, title="Revenue", show_title=False, x=0, y=0, w=6, h=4
    )
    _, duuid = _make_dashboard("board", DashboardLayout(tiles=[tile_model]))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Dash", OWNER)
        tile = _dash_yaml(root)[duuid]["layout"]["tiles"][0]
        assert tile["viz_uuid"] == vuuid and "viz_id" not in tile  # portable tile reference
        assert (tile["title"], tile["show_title"]) == ("Revenue", False)

        dash_bytes = _dash_bytes(root)

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
            assert (stored_tile["title"], stored_tile["show_title"]) == ("Revenue", False)
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "dashboard must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_chart_tile_format_round_trips(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("format-chart", source_type="sql", sql_query="SELECT 1 AS amount")
    chart_format = DashboardChartFormat(number_format=",.2f", time_format="%Y-%m-%d", y_title="Revenue")
    layout = DashboardLayout(
        tiles=[
            DashboardTile(id="fmt-1", type="viz", viz_id=viz_id, chart_format=chart_format, x=0, y=0, w=6, h=4),
            DashboardTile(id="fmt-2", type="viz", viz_id=viz_id, x=6, y=0, w=6, h=4),
        ]
    )
    _, duuid = _make_dashboard("format-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Format", OWNER)
        tiles = _dash_yaml(root)[duuid]["layout"]["tiles"]
        assert tiles[0]["chart_format"] == chart_format.model_dump()
        assert tiles[1]["chart_format"] is None

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = json.loads(dashboard.layout_json)["tiles"]
            assert DashboardChartFormat.model_validate(stored[0]["chart_format"]) == chart_format
            assert stored[1]["chart_format"] is None
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "chart formats must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_tile_note_round_trips(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, _ = _make_viz("note-chart", source_type="sql", sql_query="SELECT 1 AS amount")
    note = "**Revenue** excludes refunds.\n\nSee [the definition](https://example.com)."
    layout = DashboardLayout(
        tiles=[
            DashboardTile(id="note-1", type="viz", viz_id=viz_id, note_md=note, x=0, y=0, w=6, h=4),
            DashboardTile(id="note-2", type="viz", viz_id=viz_id, x=6, y=0, w=6, h=4),
        ]
    )
    _, duuid = _make_dashboard("note-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Notes", OWNER)
        tiles = _dash_yaml(root)[duuid]["layout"]["tiles"]
        assert [t["note_md"] for t in tiles] == [note, None]

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = json.loads(dashboard.layout_json)["tiles"]
            assert [t["note_md"] for t in stored] == [note, None]
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "tile notes must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_image_tile_round_trips_with_its_bytes(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    png = "data:image/png;base64," + base64.b64encode(b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 40).decode()
    svg = "data:image/svg+xml;base64," + base64.b64encode(b'<svg xmlns="http://www.w3.org/2000/svg"/>').decode()
    layout = DashboardLayout(
        tiles=[
            DashboardTile(
                id="img-1", type="image", image=DashboardTileImage(data_url=png, alt="Logo"), x=0, y=0, w=8, h=3
            ),
            DashboardTile(
                id="img-2", type="image", image=DashboardTileImage(data_url=svg, fit="cover"), x=8, y=0, w=8, h=3
            ),
            DashboardTile(id="img-3", type="image", x=16, y=0, w=8, h=3),
        ]
    )
    _, duuid = _make_dashboard("image-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Images", OWNER)
        tiles = _dash_yaml(root)[duuid]["layout"]["tiles"]
        assert [t["image"] for t in tiles] == [
            {"data_url": png, "alt": "Logo", "fit": "contain"},
            {"data_url": svg, "alt": None, "fit": "cover"},
            None,
        ]

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = DashboardLayout.model_validate_json(dashboard.layout_json)
            assert [t.image for t in stored.tiles] == [t.image for t in layout.tiles]
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "image tiles must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_color_assignments_round_trip(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, _ = _make_viz("colour-chart", source_type="sql", sql_query="SELECT 'West' AS region")
    assignments = [
        DashboardColorAssignment(field="region", value="West", slot=0),
        DashboardColorAssignment(field="region", value="East", slot=1),
        DashboardColorAssignment(field="code", value=7, slot=0),
        DashboardColorAssignment(field="region", value=None, slot=2),
    ]
    layout = DashboardLayout(
        tiles=[DashboardTile(id="col-1", type="viz", viz_id=viz_id, x=0, y=0, w=6, h=4)],
        color_assignments=assignments,
    )
    _, duuid = _make_dashboard("colour-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Colours", OWNER)
        exported = _dash_yaml(root)[duuid]["layout"]["color_assignments"]
        assert exported == [a.model_dump() for a in assignments]
        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = DashboardLayout.model_validate_json(dashboard.layout_json)
            assert stored.color_assignments == assignments
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "colours must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_auto_refresh_interval_round_trips(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, timed_uuid = _make_dashboard("timed-board", DashboardLayout(auto_refresh_seconds=900))
    _, static_uuid = _make_dashboard("static-board", DashboardLayout())
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Refresh", OWNER)
        dashboards = _dash_yaml(root)
        assert dashboards[timed_uuid]["layout"]["auto_refresh_seconds"] == 900
        assert dashboards[static_uuid]["layout"]["auto_refresh_seconds"] is None
        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            rows = {
                d.dashboard_uuid: DashboardLayout.model_validate_json(d.layout_json)
                for d in db.query(CatalogDashboard).filter_by(created_by=OWNER)
            }
        assert rows[timed_uuid].auto_refresh_seconds == 900
        assert rows[static_uuid].auto_refresh_seconds is None
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "the interval must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_layout_without_auto_refresh_imports_with_it_off(tmp_path):
    """A dashboard file written before the field existed imports and exports again unchanged."""
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, duuid = _make_dashboard("old-board", DashboardLayout())
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Old", OWNER)
        _edit_dashboards(root, lambda entry: entry["layout"].pop("auto_refresh_seconds", None))

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            row = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert row is not None
            assert "auto_refresh_seconds" not in json.loads(row.layout_json)
            assert DashboardLayout.model_validate_json(row.layout_json).auto_refresh_seconds is None
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_kpi_tile_round_trips_and_relinks(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("kpi-chart", source_type="sql", sql_query="SELECT 1 AS amount")
    kpi = DashboardKpi(
        field="amount",
        agg="sum",
        label="Revenue",
        prefix="$",
        comparison="target",
        target=100.0,
        trend=DashboardKpiTrend(field="order_date", grain="quarter"),
    )
    layout = DashboardLayout(tiles=[DashboardTile(id="kpi-1", type="kpi", viz_id=viz_id, kpi=kpi, x=0, y=0, w=12, h=3)])
    _, duuid = _make_dashboard("kpi-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kpi", OWNER)
        tile = _dash_yaml(root)[duuid]["layout"]["tiles"][0]
        assert tile["viz_uuid"] == vuuid and "viz_id" not in tile
        assert tile["kpi"] == kpi.model_dump()
        assert tile["kpi"]["trend"] == {"field": "order_date", "grain": "quarter"}

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            new_viz = db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first()
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert new_viz is not None and dashboard is not None
            stored_tile = json.loads(dashboard.layout_json)["tiles"][0]
            assert stored_tile["viz_id"] == new_viz.id and "viz_uuid" not in stored_tile
            assert stored_tile["type"] == "kpi"
            assert DashboardKpi.model_validate(stored_tile["kpi"]) == kpi
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "KPI dashboard must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_table_tile_exports_a_portable_table_and_relinks(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    tname = f"rows_{uuid4().hex[:6]}"
    table_id = _make_table(tname)
    config = DashboardTableConfig(
        columns=[DashboardTableColumn(field="amount", label="Amount", decimals=2)],
        sort_by=["amount"],
        sort_direction="descending",
        page_size=50,
    )
    layout = DashboardLayout(
        tiles=[
            DashboardTile(id="tbl-1", type="table", table_id=table_id, table=config, x=0, y=0, w=24, h=8),
            DashboardTile(id="kpi-t", type="kpi", table_id=table_id, kpi=DashboardKpi(agg="count"), x=24, y=0, w=12, h=3),
        ]
    )
    _, duuid = _make_dashboard("table-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Rows", OWNER)
        tiles = _dash_yaml(root)[duuid]["layout"]["tiles"]
        for tile in tiles:
            assert tile["source_table"]["name"] == tname and "table_id" not in tile
        assert tiles[0]["table"] == config.model_dump()

        dash_bytes = _dash_bytes(root)

        # Rebuild with a new table id: both tiles re-link to it by {catalog, schema, name}.
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        _delete_table(tname)
        import_project(root, OWNER)
        with get_db_context() as db:
            table = SQLAlchemyCatalogRepository(db).get_table_by_name(tname, None)
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert table is not None and dashboard is not None
            stored = json.loads(dashboard.layout_json)["tiles"]
            assert [t["table_id"] for t in stored] == [table.id, table.id]
            assert all("source_table" not in t for t in stored)
            assert DashboardTableConfig.model_validate(stored[0]["table"]) == config
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "table tiles must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        _delete_table(tname)
        project_sync.close_project(OWNER)


def test_filter_tile_round_trips_with_its_filter(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    region = DashboardFilter(id="f-region", field_name="region", kind="categorical", state={"selected": ["West"]})
    layout = DashboardLayout(
        tiles=[DashboardTile(id="ft-1", type="filter", filter_id="f-region", x=0, y=0, w=12, h=2)],
        filters=[region],
    )
    _, duuid = _make_dashboard("filter-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Placed filter", OWNER)
        exported = _dash_yaml(root)[duuid]["layout"]
        assert (exported["tiles"][0]["type"], exported["tiles"][0]["filter_id"]) == ("filter", "f-region")
        assert exported["filters"][0]["id"] == "f-region"

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = json.loads(dashboard.layout_json)
            assert (stored["tiles"][0]["type"], stored["tiles"][0]["filter_id"]) == ("filter", "f-region")
            assert stored["filters"][0]["state"] == {"selected": ["West"]}
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "filter tiles must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_tabs_round_trip_with_their_tiles(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    layout = DashboardLayout(
        tabs=[DashboardTab(id="tab-a", title="Overview"), DashboardTab(id="tab-b", title="Operations")],
        tiles=[
            DashboardTile(id="txt-1", type="text", text_md="# A", tab_id="tab-a", x=0, y=0, w=48, h=3),
            DashboardTile(id="txt-2", type="text", text_md="# B", tab_id="tab-b", x=0, y=0, w=48, h=3),
        ],
    )
    _, duuid = _make_dashboard("tabbed-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Tabs", OWNER)
        exported = _dash_yaml(root)[duuid]["layout"]
        assert exported["tabs"] == [{"id": "tab-a", "title": "Overview"}, {"id": "tab-b", "title": "Operations"}]
        assert [t["tab_id"] for t in exported["tiles"]] == ["tab-a", "tab-b"]
        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert dashboard is not None
            stored = DashboardLayout.model_validate_json(dashboard.layout_json)
            assert stored.tabs == layout.tabs
            assert [t.tab_id for t in stored.tiles] == ["tab-a", "tab-b"]
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "tabs must round-trip byte-identically"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_layout_without_tabs_imports_as_one_canvas(tmp_path):
    """A dashboard file written before tabs existed imports with none."""
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, duuid = _make_dashboard(
        "flat-board", DashboardLayout(tiles=[DashboardTile(id="t", type="text", x=0, y=0, w=48, h=3)])
    )
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Flat", OWNER)

        def without_tabs(entry: dict) -> None:
            entry["layout"].pop("tabs", None)
            for tile in entry["layout"]["tiles"]:
                tile.pop("tab_id", None)

        _edit_dashboards(root, without_tabs)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            row = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert row is not None
            stored = DashboardLayout.model_validate_json(row.layout_json)
            assert stored.tabs == []
            assert [t.tab_id for t in stored.tiles] == [None]
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_table_tile_on_a_deleted_table_exports_no_source(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    layout = DashboardLayout(tiles=[DashboardTile(id="tbl-gone", type="table", table_id=987654, x=0, y=0, w=24, h=8)])
    _, duuid = _make_dashboard("gone-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Gone", OWNER)
        tile = _dash_yaml(root)[duuid]["layout"]["tiles"][0]
        assert tile["source_table"] is None and "table_id" not in tile
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_reimport_preserves_dashboard_layout_version_monotonicity(tmp_path):
    """The server-owned write counter (CAS token) never rewinds on a project re-import, and an
    unchanged layout leaves it where it is."""
    from flowfile_core.catalog import CatalogService
    from flowfile_core.project.importer import import_project
    from flowfile_core.schemas.catalog_schema import DashboardUpdate

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    dash_id, duuid = _make_dashboard("mono-board", DashboardLayout(tiles=[]))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Mono", OWNER)
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            bumped = svc.update_dashboard(dash_id, DashboardUpdate(description="bump"), user_id=OWNER)
        assert bumped.layout_version == 2

        import_project(root, OWNER)
        with get_db_context() as db:
            row = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert row is not None
            assert row.layout_version == bumped.layout_version
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_prune_removes_absent_viz_and_dashboard(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, vuuid = _make_viz("prune-chart", source_type="sql", sql_query="SELECT 1")
    _, duuid = _make_dashboard("prune-board", DashboardLayout(tiles=[]))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Prune VD", OWNER)
        assert vuuid in _viz_yaml(root) and duuid in _dash_yaml(root)

        for path in [*(root / "visualizations").iterdir(), *(root / "dashboards").iterdir()]:
            path.unlink()
        project_sync.reload_from_disk(OWNER, force=True)

        with get_db_context() as db:
            assert db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first() is None
            assert db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first() is None
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_filter_on_a_sql_chart_exports_its_viz_uuid_and_relinks(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("filter-source", source_type="sql", sql_query="SELECT 'West' AS region")
    region = DashboardFilter(
        id="f-region",
        field_name="region",
        kind="categorical",
        state={"selected": ["West"]},
        datasource_viz_id=viz_id,
        multiple=False,
    )
    layout = DashboardLayout(
        tiles=[DashboardTile(id="tile-1", type="viz", viz_id=viz_id, x=0, y=0, w=6, h=4)], filters=[region]
    )
    _, duuid = _make_dashboard("sql-filter-board", layout)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "SQL filter", OWNER)
        exported = _dash_yaml(root)[duuid]["layout"]["filters"][0]
        assert exported["datasource_viz_uuid"] == vuuid and "datasource_viz_id" not in exported
        assert exported["multiple"] is False

        dash_bytes = _dash_bytes(root)

        project_sync.close_project(OWNER)
        _clear_viz_dashboards()
        import_project(root, OWNER)
        with get_db_context() as db:
            new_viz = db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).first()
            dashboard = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            assert new_viz is not None and dashboard is not None
            stored = json.loads(dashboard.layout_json)["filters"][0]
            assert stored["datasource_viz_id"] == new_viz.id and "datasource_viz_uuid" not in stored
            assert (stored["multiple"], stored["state"]) == (False, {"selected": ["West"]})
        write_export(root, OWNER)
        assert _dash_bytes(root) == dash_bytes, "chart-sourced filter must round-trip"
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


# One file per chart and per dashboard, and the root lists a folder of format 2 or older holds them in.


def _to_older_layout(root: Path) -> None:
    """Rewrite the folder as format 2 held it: every chart in one root file, every dashboard in another."""
    for folder, pattern, key, id_key in (
        ("visualizations", "*.visualization.yaml", "visualizations", "viz_uuid"),
        ("dashboards", "*.dashboard.yaml", "dashboards", "dashboard_uuid"),
    ):
        files = sorted((root / folder).glob(pattern))
        entries = [yaml.safe_load(f.read_text(encoding="utf-8")) for f in files]
        write_yaml(root / f"{key}.yaml", {key: sorted(entries, key=lambda e: (e["name"], e[id_key]))})
        for f in files:
            f.unlink()
        (root / folder).rmdir()
    project = yaml.safe_load((root / "project.yaml").read_text(encoding="utf-8"))
    write_yaml(root / "project.yaml", {**project, "project_format": 2})


def _board(viz_id: int) -> DashboardLayout:
    return DashboardLayout(tiles=[DashboardTile(id="t", type="viz", viz_id=viz_id, x=0, y=0, w=6, h=4)])


def test_each_chart_and_dashboard_is_a_file_named_after_it(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("Sales / by region", source_type="sql", sql_query="SELECT 1")
    _, duuid = _make_dashboard("Sales overview", _board(viz_id))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Files", OWNER)
        assert list(_viz_bytes(root)) == ["Sales_by_region.visualization.yaml"]
        assert list(_dash_bytes(root)) == ["Sales_overview.dashboard.yaml"]
        assert not (root / "visualizations.yaml").exists() and not (root / "dashboards.yaml").exists()
        chart = yaml.safe_load(_viz_bytes(root)["Sales_by_region.visualization.yaml"])
        assert list(chart)[:2] == ["viz_uuid", "name"] and chart["viz_uuid"] == vuuid
        board = yaml.safe_load(_dash_bytes(root)["Sales_overview.dashboard.yaml"])
        assert board["dashboard_uuid"] == duuid and board["layout"]["tiles"][0]["viz_uuid"] == vuuid
        assert yaml.safe_load((root / "project.yaml").read_text(encoding="utf-8"))["project_format"] == 3
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_two_with_one_name_and_a_name_with_no_letters_get_distinct_files(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    uuids = sorted(_make_viz(name, source_type="sql")[1] for name in ("Twin", "twin", "///"))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Names", OWNER)
        by_uuid = {yaml.safe_load(text)["viz_uuid"]: name for name, text in _viz_bytes(root).items()}
        names = {yaml.safe_load(text)["name"]: name for name, text in _viz_bytes(root).items()}
        assert names["///"] == "chart.visualization.yaml"
        twins = sorted(u for u in uuids if by_uuid[u] != "chart.visualization.yaml")
        first, second = (by_uuid[u] for u in twins)
        assert first.lower() == "twin.visualization.yaml"
        assert second.lower() == f"twin_{twins[1][:8]}.visualization.yaml"
        assert sorted(_viz_yaml(root)) == uuids
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_a_rename_moves_the_file_and_is_one_changed_object(tmp_path):
    if not git_ops.git_available():
        pytest.skip("git not available")
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, _ = _make_viz("rename-chart", source_type="sql")
    dashboard_id, duuid = _make_dashboard("Before", _board(viz_id))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Rename", OWNER)
        project_sync.save_version(OWNER, "baseline")
        with get_db_context() as db:
            db.get(CatalogDashboard, dashboard_id).name = "After"
            db.commit()
        rows = project_sync.unsaved_changes(OWNER)
        assert [(r["change"], r["kind"], r["label"]) for r in rows] == [("modified", "dashboards", "After")]

        project_sync.save_version(OWNER, "renamed")
        assert list(_dash_bytes(root)) == ["After.dashboard.yaml"]
        assert _dash_yaml(root)[duuid]["name"] == "After"
        assert project_sync.unsaved_changes(OWNER) == []
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_a_folder_of_the_older_layout_opens_unchanged_and_the_next_save_converts_it(tmp_path):
    if not git_ops.git_available():
        pytest.skip("git not available")
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    viz_id, vuuid = _make_viz("older-chart", source_type="sql", sql_query="SELECT 1")
    _, duuid = _make_dashboard("older-board", _board(viz_id))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Older", OWNER)
        files = (_viz_bytes(root), _dash_bytes(root))
        _to_older_layout(root)
        git_ops.commit_all(root, "as format 2 held it")
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()

        _, result = project_sync.open_project(str(root), OWNER)
        assert (result.by_kind["visualizations"], result.by_kind["dashboards"]) == (1, 1)
        with get_db_context() as db:
            chart = db.query(CatalogVisualization).filter_by(viz_uuid=vuuid).one()
            board = db.query(CatalogDashboard).filter_by(dashboard_uuid=duuid).one()
            assert json.loads(board.layout_json)["tiles"][0]["viz_id"] == chart.id
        assert project_sync.unsaved_changes(OWNER) == []
        assert project_sync.plan(OWNER) == []
        assert (root / "visualizations.yaml").exists() and not (root / "visualizations").exists()

        project_sync.save_version(OWNER, "converted")
        assert not (root / "visualizations.yaml").exists() and not (root / "dashboards.yaml").exists()
        assert (_viz_bytes(root), _dash_bytes(root)) == files
        assert yaml.safe_load((root / "project.yaml").read_text(encoding="utf-8"))["project_format"] == 3
        assert not git_ops.is_dirty(root)
        assert project_sync.unsaved_changes(OWNER) == []
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_a_file_replaces_its_entry_in_the_older_list_and_the_rest_of_the_list_still_applies(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, kept = _make_viz("in-both", source_type="sql", sql_query="SELECT 1")
    _, listed = _make_viz("list-only", source_type="sql", sql_query="SELECT 2")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Both", OWNER)
        entries = _viz_yaml(root)
        write_yaml(
            root / "visualizations.yaml",
            {"visualizations": [{**entries[kept], "name": "from the list"}, entries[listed]]},
        )
        (root / "visualizations" / "list-only.visualization.yaml").unlink()
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()

        importer.import_project(root, OWNER)
        with get_db_context() as db:
            names = {v.viz_uuid: v.name for v in db.query(CatalogVisualization).filter_by(created_by=OWNER)}
        assert names == {kept: "in-both", listed: "list-only"}
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_a_file_under_another_owners_folder_or_naming_another_owner_is_reported_not_applied(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, mine = _make_viz("mine", source_type="sql")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Owners", OWNER)
        entry = _viz_yaml(root)[mine]
        theirs = {**entry, "viz_uuid": str(uuid4()), "name": "theirs"}
        (root / "visualizations" / "@alice").mkdir()
        write_yaml(root / "visualizations" / "@alice" / "theirs.visualization.yaml", theirs)
        write_yaml(root / "visualizations" / "named.visualization.yaml", {**theirs, "owner": "alice"})
        project_sync.close_project(OWNER)
        _clear_viz_dashboards()

        result = importer.import_project(root, OWNER)
        assert sorted(result.not_applied) == [
            "visualizations/@alice/theirs.visualization.yaml",
            "visualizations/named.visualization.yaml",
        ]
        with get_db_context() as db:
            assert [v.name for v in db.query(CatalogVisualization).filter_by(created_by=OWNER)] == ["mine"]
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_a_dashboard_file_with_an_invalid_layout_is_refused_by_its_path_before_anything_changes(tmp_path):
    project_sync.close_project(OWNER)
    _clear_viz_dashboards()
    _, duuid = _make_dashboard("broken-board", DashboardLayout())
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Broken", OWNER)
        _edit_dashboards(root, lambda entry: entry.update(layout={"tiles": "not a list"}))
        with pytest.raises(importer.ProjectFileError) as refused:
            importer.preflight_import_files_worktree(root)
        assert "dashboards/broken-board.dashboard.yaml" in str(refused.value)
        assert "invalid layout" in str(refused.value)

        write_yaml(root / "dashboards" / "broken-board.dashboard.yaml", {"dashboard_uuid": duuid})
        with pytest.raises(importer.ProjectFileError, match="has no 'name'"):
            importer.preflight_import_files_worktree(root)
    finally:
        _clear_viz_dashboards()
        project_sync.close_project(OWNER)


def test_the_cap_counts_files_and_the_entries_of_an_older_list():
    from flowfile_core.project import kinds

    cap = kinds.get("dashboards").cap
    files = {"dashboards": [f"dashboards/d{i}.dashboard.yaml" for i in range(cap - 1)] + ["dashboards.yaml"]}
    older = {"dashboards.yaml": {"dashboards": [{"name": "a"}]}}
    importer._assert_kind_caps(files, lambda rel: older[rel], only={"dashboards"})
    older["dashboards.yaml"]["dashboards"].append({"name": "b"})
    with pytest.raises(importer.ImportTooLargeError, match=f"dashboards manifest exceeds the {cap}-entry cap"):
        importer._assert_kind_caps(files, lambda rel: older[rel], only={"dashboards"})
