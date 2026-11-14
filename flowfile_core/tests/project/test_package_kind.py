"""A package adds a project kind through the ``flowfile.project_kinds`` entry-point group.

A dummy kind with a table of its own, offered through a patched entry-point scan, is ordered between the two core
kinds it names, exported and staged by a save, previewed, applied and deleted by a reload on the every-kind install,
and passes ``assert_kind_round_trips``. A kind closing an order cycle and one claiming a core kind's file are
rejected with core's kinds still working, and a failing apply rolls the whole reload back. Every registered kind
has an import step or its own ``apply``, so none is skipped by an import.
"""

import dataclasses
import importlib.metadata
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
import yaml

from flowfile_core.database.connection import engine, get_db_context
from flowfile_core.database.models import DatabaseConnection, FlowRegistration
from flowfile_core.project import git_ops, importer, kinds, project_sync, service
from flowfile_core.project.manifest import read_manifest
from flowfile_core.project.normalize import dump_yaml
from flowfile_core.project.testing import assert_kind_round_trips
from tests.project.test_apply import _edit
from tests.project.test_kinds import FLOW_A
from tests.project.test_kinds_list import TWELVE, every_kind  # noqa: F401

TABLE = sa.Table(
    "project_test_widgets",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("owner_id", sa.Integer, nullable=False),
    sa.Column("name", sa.String, nullable=False),
    sa.Column("value", sa.String),
)


def _export(ctx: kinds.ExportContext) -> dict[str, str]:
    rows = ctx.db.execute(
        sa.select(TABLE.c.name, TABLE.c.value).where(TABLE.c.owner_id == ctx.owner_id).order_by(TABLE.c.name)
    )
    return {f"widgets/{name}.yaml": dump_yaml({"name": name, "value": value}) for name, value in rows}


def _entries(files: dict[str, str]) -> dict:
    out = {}
    for path, text in files.items():
        name = (yaml.safe_load(text) or {}).get("name") or path
        out[name] = kinds.Entry(str(name), path, text)
    return out


def _delete(ctx: kinds.ExportContext, keys: list) -> None:
    ctx.db.execute(TABLE.delete().where(TABLE.c.owner_id == ctx.owner_id, TABLE.c.name.in_(keys)))
    ctx.db.commit()


def _apply(ctx: kinds.ExportContext, entries: dict) -> None:
    for key, entry in entries.items():
        data = yaml.safe_load(entry.text)
        ctx.db.execute(TABLE.delete().where(TABLE.c.owner_id == ctx.owner_id, TABLE.c.name == key))
        ctx.db.execute(TABLE.insert().values(owner_id=ctx.owner_id, name=data["name"], value=data.get("value")))


WIDGET = kinds.Kind(
    "widget",
    "widget",
    ("widgets/*.yaml",),
    _export,
    after=("namespaces",),
    before=("flow",),
    entries=_entries,
    delete_label="it is deleted",
    delete=_delete,
    apply=_apply,
)


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


@pytest.fixture(autouse=True)
def _registry():
    kinds.registered()
    saved, rejected = dict(kinds._KINDS), dict(kinds._REJECTED)
    yield
    kinds._KINDS.clear()
    kinds._KINDS.update(saved)
    kinds._REJECTED.clear()
    kinds._REJECTED.update(rejected)
    kinds._core_loaded = True


def _rescan(monkeypatch, *offered: kinds.Kind) -> None:
    """Scan the entry points again, as on a first call, with ``offered`` the only ones installed."""
    points = [SimpleNamespace(name=k.name, value=f"tests:{k.name}", load=lambda k=k: k) for k in offered]
    kinds._core_loaded = False
    with monkeypatch.context() as m:
        m.setattr(importlib.metadata, "entry_points", lambda group: points if group == kinds.ENTRY_POINT_GROUP else [])
        kinds.registered()


@pytest.fixture
def table():
    TABLE.metadata.create_all(engine)
    try:
        yield
    finally:
        TABLE.metadata.drop_all(engine)


@pytest.fixture
def widgets(monkeypatch, table):
    _rescan(monkeypatch, WIDGET)


def _put(owner_id: int, name: str, value: str) -> None:
    with get_db_context() as db:
        db.execute(TABLE.insert().values(owner_id=owner_id, name=name, value=value))
        db.commit()


def _rows(owner_id: int) -> dict[str, str]:
    with get_db_context() as db:
        return dict(db.execute(sa.select(TABLE.c.name, TABLE.c.value).where(TABLE.c.owner_id == owner_id)).all())


def _write(root: Path, name: str, value: str) -> None:
    (root / "widgets" / f"{name}.yaml").write_text(dump_yaml({"name": name, "value": value}), encoding="utf-8")


def test_a_package_kind_is_ordered_between_the_kinds_it_names(widgets):
    names = [k.name for k in kinds.ordered()]
    assert names.index("namespaces") < names.index("widget") < names.index("flow")
    assert "widget" not in kinds.rejected()


def test_every_kind_has_an_import_step_or_its_own_apply(widgets):
    names = [k.name for k in kinds.ordered()]
    assert "widget" in names and set(importer._IMPORT_STEPS) <= set(names)
    for kind in kinds.ordered():
        assert kind.name in importer._IMPORT_STEPS or (kind.entries is not None and kind.apply is not None), kind.name


def test_a_package_kind_is_saved_previewed_applied_and_deleted(tmp_path, every_kind, widgets):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "package", every_kind)
    assert "widget" in read_manifest(root).kinds
    _put(every_kind, "w1", "a")
    _put(every_kind, "w3", "c")
    project_sync.save_version(every_kind, "widgets")
    head = git_ops.read_tree_at(root, "HEAD")
    assert yaml.safe_load(head["widgets/w1.yaml"]) == {"name": "w1", "value": "a"}
    assert "widgets/w3.yaml" in head

    _write(root, "w1", "b")
    _write(root, "w2", "new")
    (root / "widgets" / "w3.yaml").unlink()
    plan = service._plan_changes(root, every_kind, None)
    assert sorted((c.kind, c.key, c.change) for c in plan) == [
        ("widget", "w1", "modified"),
        ("widget", "w2", "added"),
        ("widget", "w3", "removed"),
    ]

    result = project_sync.reload_from_disk(every_kind)

    assert result.by_kind["widget"] == 2
    assert _rows(every_kind) == {"w1": "b", "w2": "new"}
    assert project_sync.plan(every_kind) == []
    assert_kind_round_trips(kinds.get("widget"), every_kind)


def test_a_cycle_and_a_path_clash_are_rejected_and_core_keeps_working(monkeypatch):
    loop = dataclasses.replace(WIDGET, name="loop", paths=("loops/*.yaml",), after=("flow",), before=("namespaces",))
    ledger = dataclasses.replace(WIDGET, name="ledger", paths=("tables.yaml",), after=(), before=())
    _rescan(monkeypatch, loop, ledger)
    rejected = kinds.rejected()
    assert "cycle" in rejected["loop"]
    assert "'tables.yaml'" in rejected["ledger"]
    assert [k.name for k in kinds.ordered()] == TWELVE


def test_a_failing_apply_rolls_the_reload_back(tmp_path, monkeypatch, every_kind, table):  # noqa: F811
    def fail(ctx, entries):
        _apply(ctx, entries)
        raise RuntimeError("refused")

    _rescan(monkeypatch, dataclasses.replace(WIDGET, after=("flow",), before=(), apply=fail))
    _put(every_kind, "w1", "a")
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "package", every_kind)
    with get_db_context() as db:
        flow_file = Path(db.query(FlowRegistration).filter_by(flow_uuid=FLOW_A).one().flow_path)
    flow_text = flow_file.read_text(encoding="utf-8")
    _edit(root, "database_connection", "modified")
    _edit(root, "flow", "modified")
    _write(root, "w1", "b")

    with pytest.raises(ValueError, match="Could not apply widget: refused"):
        project_sync.reload_from_disk(every_kind)

    with get_db_context() as db:
        assert db.query(DatabaseConnection).filter_by(user_id=every_kind).one().host == "localhost"
    assert flow_file.read_text(encoding="utf-8") == flow_text
    assert _rows(every_kind) == {"w1": "a"}
