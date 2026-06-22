"""Notebook projection: determinism, portable round-trip, prune, hook isolation.

Notebooks are tracked per-file (like flows): one ``notebooks/<stem>.notebook.yaml`` keyed by stable
``notebook_uuid``, carrying the cells (literal-block source) and a portable ``{catalog, schema}``
namespace so they re-link to local ids on another machine. Metadata comes from the DB row, cells from
the on-disk content file. These tests write rows + content files directly and drive the project_sync
hooks, exactly as the catalog service does.
"""

from pathlib import Path

import yaml

from flowfile_core.catalog import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogNotebook
from flowfile_core.project import project_sync, projection
from flowfile_core.schemas.catalog_schema import NotebookCellModel

OWNER = 1

_CELLS = [
    NotebookCellModel(id="c1", type="markdown", source="# Orders\n\nExplore the orders table."),
    NotebookCellModel(id="c2", type="sql", source="SELECT region, sales\nFROM orders\nWHERE sales > 0", metadata={"max_rows": 100}),
    NotebookCellModel(id="c3", type="python", source="import polars as pl\n\ndf = pl.DataFrame({'a': [1, 2]})\nprint(df)"),
]


def _default_ns_id() -> int | None:
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        general = repo.get_namespace_by_name("General", None, owner_id=OWNER, include_public=True)
        if general is None:
            return None
        default = repo.get_namespace_by_name("default", general.id, owner_id=OWNER, include_public=True)
        return default.id if default else None


def _make_notebook(
    name: str,
    *,
    namespace_id: int | None = None,
    cells: list[NotebookCellModel] | None = None,
    description: str | None = None,
    default_kernel_id: str | None = None,
) -> tuple[int, str]:
    with get_db_context() as db:
        nb = CatalogNotebook(
            name=name,
            owner_id=OWNER,
            namespace_id=namespace_id,
            description=description,
            default_kernel_id=default_kernel_id,
        )
        db.add(nb)
        db.commit()
        db.refresh(nb)
        nb_id, nb_uuid = nb.id, nb.notebook_uuid
        ns_name = db.get(CatalogNamespace, namespace_id).name if namespace_id is not None else None
    notebook_store.write_notebook_file(
        OWNER,
        nb_uuid,
        name=name,
        description=description,
        namespace_name=ns_name,
        default_kernel_id=default_kernel_id,
        cells=cells or [],
    )
    return nb_id, nb_uuid


def _nb_yaml(root: Path) -> dict:
    out: dict = {}
    for p in (root / "notebooks").glob("*.notebook.yaml"):
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        out[data["notebook_uuid"]] = data
    return out


def _clear_notebooks() -> None:
    with get_db_context() as db:
        uuids = [r.notebook_uuid for r in db.query(CatalogNotebook).filter_by(owner_id=OWNER).all()]
        db.query(CatalogNotebook).filter_by(owner_id=OWNER).delete()
        db.commit()
    for u in uuids:
        notebook_store.delete_notebook_file(OWNER, u)


def test_notebook_round_trips_byte_identical(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_notebooks()
    _, nuuid = _make_notebook("explore-orders", cells=_CELLS, description="first look")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "NB", OWNER)
        entry = _nb_yaml(root)[nuuid]
        assert entry["name"] == "explore-orders" and entry["description"] == "first look"
        assert [c["type"] for c in entry["cells"]] == ["markdown", "sql", "python"]
        # Multi-line source is a literal block (clean diffs), and the SQL cell keeps its metadata.
        raw = (root / "notebooks" / "explore-orders.notebook.yaml").read_text(encoding="utf-8")
        assert "source: |" in raw
        assert entry["cells"][1]["metadata"] == {"max_rows": 100}

        nb_bytes = (root / "notebooks" / "explore-orders.notebook.yaml").read_bytes()
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "notebooks" / "explore-orders.notebook.yaml").read_bytes() == nb_bytes, "re-projection must be byte-identical"

        # project -> import -> project must round-trip; upsert by notebook_uuid must not duplicate.
        project_sync.close_project(OWNER)
        _clear_notebooks()
        import_project(root, OWNER)
        import_project(root, OWNER)
        with get_db_context() as db:
            rows = db.query(CatalogNotebook).filter_by(owner_id=OWNER).all()
            assert len(rows) == 1 and rows[0].notebook_uuid == nuuid
            assert rows[0].name == "explore-orders"
        # Cells survive the round-trip intact.
        assert notebook_store.read_notebook_cells(OWNER, nuuid) == _CELLS
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "notebooks" / "explore-orders.notebook.yaml").read_bytes() == nb_bytes, "project->import->project must round-trip"
    finally:
        _clear_notebooks()
        project_sync.close_project(OWNER)


def test_notebook_namespace_relinks_on_import(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _clear_notebooks()
    ns_id = _default_ns_id()
    assert ns_id is not None, "seeded General/default namespace expected"
    _, nuuid = _make_notebook("filed-nb", namespace_id=ns_id, cells=_CELLS)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "NB NS", OWNER)
        entry = _nb_yaml(root)[nuuid]
        assert entry["namespace"] == {"catalog": "General", "schema": "default"}  # portable, not a local id

        project_sync.close_project(OWNER)
        _clear_notebooks()
        import_project(root, OWNER)
        with get_db_context() as db:
            nb = db.query(CatalogNotebook).filter_by(notebook_uuid=nuuid).first()
            assert nb is not None and nb.namespace_id == ns_id
    finally:
        _clear_notebooks()
        project_sync.close_project(OWNER)


def test_prune_removes_absent_notebook(tmp_path):
    project_sync.close_project(OWNER)
    _clear_notebooks()
    _, nuuid = _make_notebook("prune-nb", cells=_CELLS)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Prune NB", OWNER)
        assert nuuid in _nb_yaml(root)

        # Files are authoritative on reload: drop the projected file, the DB row + content file follow.
        (root / "notebooks" / "prune-nb.notebook.yaml").unlink()
        project_sync.reload_from_disk(OWNER, force=True)

        with get_db_context() as db:
            assert db.query(CatalogNotebook).filter_by(notebook_uuid=nuuid).first() is None
        assert notebook_store.read_notebook_cells(OWNER, nuuid) == []  # content file gone
    finally:
        _clear_notebooks()
        project_sync.close_project(OWNER)


def test_notebook_hook_and_failure_isolation(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    _clear_notebooks()
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Hooks NB", OWNER)
        assert _nb_yaml(root) == {}

        _, nuuid = _make_notebook("hk-nb", cells=_CELLS)
        project_sync.notebooks_changed(OWNER)
        assert nuuid in _nb_yaml(root)

        # Deleting the notebook + re-running the hook prunes its file (one hook covers delete).
        _clear_notebooks()
        project_sync.notebooks_changed(OWNER)
        assert _nb_yaml(root) == {}

        def _boom(*_args, **_kwargs):
            raise RuntimeError("projection boom")

        monkeypatch.setattr(projection, "regenerate_notebooks", _boom)
        project_sync.notebooks_changed(OWNER)  # must not raise
    finally:
        _clear_notebooks()
        project_sync.close_project(OWNER)


def test_malformed_notebook_uuid_does_not_abort_import(tmp_path):
    """A notebooks/*.notebook.yaml with a non-UUID notebook_uuid (hand-edit / merge corruption) must
    not raise out of the import (notebook_store derives the content path via uuid.UUID()); a fresh
    valid uuid is minted and import completes with a consistent row + content file."""
    from flowfile_core.project.importer import import_project
    from flowfile_core.project.normalize import atomic_write

    project_sync.close_project(OWNER)
    _clear_notebooks()
    _, nuuid = _make_notebook("corrupt-nb", cells=_CELLS)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Corrupt NB", OWNER)
        # Corrupt the projected file's notebook_uuid to a non-UUID string.
        nb_file = root / "notebooks" / "corrupt-nb.notebook.yaml"
        data = yaml.safe_load(nb_file.read_text(encoding="utf-8"))
        data["notebook_uuid"] = "not-a-uuid"
        atomic_write(nb_file, yaml.safe_dump(data, sort_keys=False))

        project_sync.close_project(OWNER)
        _clear_notebooks()
        import_project(root, OWNER)  # must not raise

        with get_db_context() as db:
            rows = db.query(CatalogNotebook).filter_by(owner_id=OWNER).all()
            assert len(rows) == 1 and rows[0].name == "corrupt-nb"
            minted = rows[0].notebook_uuid
        import uuid as _uuid

        _uuid.UUID(minted)  # a well-formed uuid was minted (no raise)
        assert minted != "not-a-uuid"
        # The content file was written under the minted uuid and is readable.
        assert notebook_store.read_notebook_cells(OWNER, minted) == _CELLS
    finally:
        _clear_notebooks()
        project_sync.close_project(OWNER)


def test_notebooks_changed_noop_without_active_project():
    project_sync.close_project(OWNER)
    _clear_notebooks()
    _make_notebook("orphan-nb", cells=_CELLS)
    try:
        project_sync.notebooks_changed(OWNER)  # no active project -> silent no-op
    finally:
        _clear_notebooks()
