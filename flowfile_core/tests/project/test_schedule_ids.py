"""A schedule keeps its row across a reload: the file names it by ``id`` and the import updates it in place.

Real git, a temp folder and the test catalog database. Editing a cron line and reloading keeps the
row, its notification rule and its run state; a file written before schedules had ids reloads
without duplicating and gets the ids on the next save; an entry removed from the file is deleted;
an id another flow's schedule already holds is minted afresh.
"""

from datetime import datetime
from pathlib import Path

import pytest
import yaml

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration, FlowSchedule, NotificationChannel, NotificationRule
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, manifest, project_sync
from flowfile_core.project.importer import _import_schedules
from flowfile_core.schemas import input_schema, schemas

OWNER = 1
TRIGGERED = datetime(2026, 1, 2, 3, 4, 5)


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    project_sync.init_project(str(root), "Schedules", OWNER)
    created: list[str] = []
    channels: list[int] = []
    try:
        yield root, created, channels
    finally:
        project_sync.close_project(OWNER)
        with get_db_context() as db:
            for flow_uuid in created:
                reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
                if reg is not None:
                    CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)
            for channel_id in channels:
                db.query(NotificationRule).filter_by(channel_id=channel_id).delete()
                db.query(NotificationChannel).filter_by(id=channel_id).delete()
            db.commit()


def _register(tmp_path: Path, name: str, flow_id: int) -> tuple[int, str]:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).one()
        return reg.id, reg.flow_uuid


def _add_schedule(reg_id: int, **kwargs) -> int:
    with get_db_context() as db:
        return (
            CatalogService(SQLAlchemyCatalogRepository(db))
            .create_schedule(registration_id=reg_id, owner_id=OWNER, **kwargs)
            .id
        )


def _rows(reg_id: int) -> list[FlowSchedule]:
    with get_db_context() as db:
        rows = db.query(FlowSchedule).filter_by(registration_id=reg_id).order_by(FlowSchedule.id).all()
        db.expunge_all()
        return rows


def _schedule_file(root: Path) -> Path:
    [path] = sorted(manifest.schedules_dir(root).glob("*.yaml"))
    return path


def _entries(root: Path) -> list[dict]:
    return yaml.safe_load(_schedule_file(root).read_text())["schedules"]


def _write_entries(root: Path, entries: list[dict]) -> None:
    path = _schedule_file(root)
    data = yaml.safe_load(path.read_text())
    data["schedules"] = entries
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def _two_schedules(tmp_path, created, flow_id: int) -> tuple[int, int, int]:
    reg_id, flow_uuid = _register(tmp_path, f"sched{flow_id}", flow_id)
    created.append(flow_uuid)
    interval_id = _add_schedule(reg_id, schedule_type="interval", interval_seconds=3600, name="hourly")
    cron_id = _add_schedule(
        reg_id, schedule_type="cron", cron_expression="0 6 * * *", cron_timezone="UTC", name="morning"
    )
    return reg_id, interval_id, cron_id


def test_an_entry_carries_its_schedule_uuid(project, tmp_path):
    root, created, _ = project
    reg_id, _, _ = _two_schedules(tmp_path, created, 818101)
    by_name = {r.name: r.schedule_uuid for r in _rows(reg_id)}
    entries = _entries(root)
    assert [e["id"] for e in entries] == [by_name["hourly"], by_name["morning"]]
    assert all(e["id"] for e in entries)


def test_editing_a_cron_line_keeps_the_row_its_rule_and_its_run_state(project, tmp_path):
    root, created, channels = project
    reg_id, _, cron_id = _two_schedules(tmp_path, created, 818102)
    with get_db_context() as db:
        channel = NotificationChannel(owner_id=OWNER, name="ops", channel_type="generic", webhook_url_encrypted="x")
        db.add(channel)
        db.flush()
        channels.append(channel.id)
        rule = NotificationRule(owner_id=OWNER, channel_id=channel.id, schedule_id=cron_id)
        db.add(rule)
        db.query(FlowSchedule).filter_by(id=cron_id).update({"last_triggered_at": TRIGGERED})
        db.commit()
        rule_id = rule.id
    project_sync.save_version(OWNER, "schedules")

    entries = _entries(root)
    entries[1]["cron_expression"] = "30 7 * * 1-5"
    _write_entries(root, entries)
    project_sync.reload_from_disk(OWNER)

    [row] = [r for r in _rows(reg_id) if r.schedule_type == "cron"]
    assert row.id == cron_id
    assert row.cron_expression == "30 7 * * 1-5"
    assert row.last_triggered_at == TRIGGERED
    with get_db_context() as db:
        assert db.query(NotificationRule).filter_by(id=rule_id, schedule_id=cron_id).count() == 1


def test_a_file_without_ids_reloads_without_duplicating_and_the_next_save_writes_them(project, tmp_path):
    root, created, _ = project
    reg_id, interval_id, cron_id = _two_schedules(tmp_path, created, 818103)
    project_sync.save_version(OWNER, "schedules")

    _write_entries(root, [{k: v for k, v in e.items() if k != "id"} for e in _entries(root)])
    project_sync.reload_from_disk(OWNER)
    project_sync.reload_from_disk(OWNER)

    rows = _rows(reg_id)
    assert [r.id for r in rows] == [interval_id, cron_id]
    project_sync.save_version(OWNER, "ids")
    assert [e["id"] for e in _entries(root)] == [r.schedule_uuid for r in rows]


def test_an_entry_removed_from_the_file_is_deleted(project, tmp_path):
    root, created, _ = project
    reg_id, interval_id, _ = _two_schedules(tmp_path, created, 818104)
    project_sync.save_version(OWNER, "schedules")

    _write_entries(root, _entries(root)[:1])
    project_sync.reload_from_disk(OWNER)

    assert [r.id for r in _rows(reg_id)] == [interval_id]


def test_an_id_held_by_another_flows_schedule_is_minted_afresh(project, tmp_path):
    _, created, _ = project
    other_reg, other_interval, _ = _two_schedules(tmp_path, created, 818105)
    [taken] = [r.schedule_uuid for r in _rows(other_reg) if r.id == other_interval]
    reg_id, flow_uuid = _register(tmp_path, "sched818106", 818106)
    created.append(flow_uuid)

    entry = {"id": taken, "schedule_type": "interval", "interval_seconds": 7200, "name": "two-hourly"}
    assert _import_schedules({"flow_uuid": flow_uuid, "schedules": [entry]}, OWNER) == 1

    [row] = _rows(reg_id)
    assert row.interval_seconds == 7200
    assert row.schedule_uuid and row.schedule_uuid != taken
    assert [r.schedule_uuid for r in _rows(other_reg) if r.id == other_interval] == [taken]
