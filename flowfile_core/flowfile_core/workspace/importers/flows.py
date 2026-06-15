"""Rebuild runtime flow files + ``FlowRegistration`` rows from the tree.

The project file is merged with its ``.layout.yaml`` (canvas coordinates), data
paths are re-absolutized for this machine, a fresh runtime ``flowfile_id`` is
assigned, and the result is validated against ``FlowfileData`` before being
written into the runtime ``flows_directory``. The registration is upserted by the
file's ``flow_uuid`` so ``flow_runs`` history re-links to the same flow.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.orm import Session

from flowfile_core.database.models import FlowRegistration
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas import schemas
from flowfile_core.workspace.layout import FLOW_SUFFIX, ProjectLayout
from flowfile_core.workspace.normalize import (
    PathTokenizer,
    canonical_yaml_dump,
    canonical_yaml_load,
    denormalize_flow,
)

logger = logging.getLogger(__name__)


class _FlowImportResult:
    def __init__(self) -> None:
        self.reg_map: dict[str, int] = {}  # flow_uuid -> registration_id
        self.counts: dict[str, int] = {"flow": 0}
        self.warnings: list[str] = []


def _runtime_flows_dir() -> Path:
    from shared.storage_config import storage

    path = storage.flows_directory
    path.mkdir(parents=True, exist_ok=True)
    return path


def import_flows(
    db: Session, user_id: int, layout: ProjectLayout, tokenizer: PathTokenizer
) -> _FlowImportResult:
    result = _FlowImportResult()
    runtime_dir = _runtime_flows_dir()

    for flow_file in layout.iter_flow_files():
        slug = flow_file.name[: -len(FLOW_SUFFIX)]
        rel = layout.rel(flow_file)
        flow_doc = canonical_yaml_load(flow_file.read_text(encoding="utf-8")) or {}
        flow_uuid = flow_doc.get("flow_uuid")
        if not flow_uuid:
            result.warnings.append(f"{rel}: missing flow_uuid; skipped")
            continue

        layout_path = layout.flow_layout_path(slug)
        layout_doc = None
        if layout_path.exists():
            layout_doc = canonical_yaml_load(layout_path.read_text(encoding="utf-8"))

        runtime = denormalize_flow(flow_doc, layout_doc, tokenizer, create_unique_id())
        try:
            model = schemas.FlowfileData.model_validate(runtime)
        except Exception as exc:  # noqa: BLE001 - reject invalid merged artifacts, don't abort apply
            result.warnings.append(f"{rel}: invalid flow definition ({exc}); skipped")
            continue

        runtime_path = runtime_dir / f"{slug}{FLOW_SUFFIX}"
        runtime_path.write_text(canonical_yaml_dump(model.model_dump(mode="json")), encoding="utf-8")

        reg = _upsert_registration(db, user_id, flow_uuid, model.flowfile_name, str(runtime_path))
        result.reg_map[flow_uuid] = reg.id
        result.counts["flow"] += 1

    db.commit()
    # Refresh ids assigned on insert so the schedule importer can link them.
    for flow_uuid in list(result.reg_map):
        reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        if reg is not None:
            result.reg_map[flow_uuid] = reg.id
    return result


def _upsert_registration(
    db: Session, user_id: int, flow_uuid: str, name: str, flow_path: str
) -> FlowRegistration:
    reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
    if reg is None:
        reg = FlowRegistration(
            flow_uuid=flow_uuid,
            name=name,
            flow_path=flow_path,
            owner_id=user_id,
            namespace_id=None,
        )
        db.add(reg)
    else:
        reg.name = name
        reg.flow_path = flow_path
    db.flush()
    return reg
