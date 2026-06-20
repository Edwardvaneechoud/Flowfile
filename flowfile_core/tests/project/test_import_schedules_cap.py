"""The schedule cap must abort an over-cap file BEFORE deleting existing schedules.

/open has no preflight (unlike restore/reload), so the cap check inside
``_import_schedules`` is the only guard. It must fire before the wholesale
delete, otherwise an over-cap file silently destroys the flow's schedules.
"""

from pathlib import Path

import pytest

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration, FlowSchedule
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import project_sync
from flowfile_core.project.importer import (
    _MAX_SCHEDULES_PER_FLOW,
    ImportTooLargeError,
    _import_schedules,
)
from flowfile_core.schemas import input_schema, schemas

OWNER = 1


def _make_flow(tmp_path: Path, name: str) -> str:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=525252, name=name, path="."))
    graph = handler.get_flow(525252)
    graph.add_node_promise(input_schema.NodePromise(flow_id=525252, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=525252, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
        assert reg is not None
        return reg.flow_uuid


def _existing_schedule_ids(flow_uuid: str) -> set[int]:
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        return {s.id for s in db.query(FlowSchedule).filter_by(registration_id=reg.id).all()}


def _cleanup(flow_uuid: str) -> None:
    project_sync.close_project(OWNER)
    with get_db_context() as db:
        for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
            db.query(FlowSchedule).filter_by(registration_id=reg.id).delete()
            if reg.flow_path and "/project/" in reg.flow_path.replace("\\", "/"):
                Path(reg.flow_path).unlink(missing_ok=True)
            db.delete(reg)
        db.commit()


def test_over_cap_schedule_file_aborts_before_deleting(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid = _make_flow(tmp_path, "proj_flow_cap")
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        svc.create_schedule(
            registration_id=reg.id,
            owner_id=OWNER,
            schedule_type="interval",
            interval_seconds=3600,
            enabled=True,
            name="keepme",
        )
    before = _existing_schedule_ids(flow_uuid)
    assert before, "fixture must have created a schedule"

    over_cap = {
        "flow_uuid": flow_uuid,
        "schedules": [
            {"schedule_type": "interval", "interval_seconds": 60, "name": f"s{i}"}
            for i in range(_MAX_SCHEDULES_PER_FLOW + 1)
        ],
    }
    try:
        with pytest.raises(ImportTooLargeError):
            _import_schedules(over_cap, OWNER)
        assert _existing_schedule_ids(flow_uuid) == before, "existing schedules must survive an over-cap abort"
    finally:
        _cleanup(flow_uuid)
