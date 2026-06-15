"""Export registered flows into deterministic ``.flow.yaml`` + ``.layout.yaml``.

A flow belongs to the project iff it has a ``FlowRegistration`` row -- that row
carries the stable ``flow_uuid`` (cross-machine identity) and the on-disk path of
the flow file. We read that file, re-validate it through the canonical
``FlowfileData`` schema (so the export is independent of how it was saved), then
split it via :func:`normalize_flow`.

Filenames are derived from the flow's own ``flowfile_name`` and assigned in
``flow_uuid`` order so collision-suffixing is deterministic.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from flowfile_core.database.models import FlowRegistration
from flowfile_core.schemas import schemas
from flowfile_core.workspace.exporters import ExportBundle
from flowfile_core.workspace.layout import ProjectLayout, slugify
from flowfile_core.workspace.normalize import PathTokenizer, canonical_yaml_dump, normalize_flow

logger = logging.getLogger(__name__)


def _read_flow_file(path: Path) -> dict:
    """Load a flow file (YAML or JSON) into a canonical ``FlowfileData`` dump.

    JSON is a subset of YAML, so ``yaml.safe_load`` parses both. Re-validating
    through the schema normalizes structure and drops unknown fields.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    model = schemas.FlowfileData.model_validate(raw)
    return model.model_dump(mode="json")


def export_flows(
    db: Session, user_id: int, layout: ProjectLayout, tokenizer: PathTokenizer
) -> tuple[ExportBundle, dict[str, str]]:
    """Return ``(bundle, flow_index)`` where ``flow_index`` maps flow_uuid -> slug."""
    bundle = ExportBundle()
    flow_index: dict[str, str] = {}

    rows = (
        db.query(FlowRegistration)
        .filter(FlowRegistration.owner_id == user_id)
        .order_by(FlowRegistration.flow_uuid.asc())
        .all()
    )

    used_slugs: set[str] = set()
    for reg in rows:
        if not reg.flow_path:
            bundle.warnings.append(f"flow '{reg.name}' (uuid={reg.flow_uuid}) has no flow_path; skipped")
            continue
        path = Path(reg.flow_path)
        if not path.exists():
            bundle.warnings.append(
                f"flow '{reg.name}' (uuid={reg.flow_uuid}) references missing file {reg.flow_path}; skipped"
            )
            continue
        try:
            flow_dump = _read_flow_file(path)
        except Exception as exc:  # noqa: BLE001 - one bad flow must not kill the export
            bundle.warnings.append(f"flow '{reg.name}' (uuid={reg.flow_uuid}) failed to parse: {exc}; skipped")
            continue

        base_slug = slugify(flow_dump.get("flowfile_name") or reg.name)
        slug = base_slug
        if slug in used_slugs:
            slug = f"{base_slug}_{reg.flow_uuid[:8]}"
        used_slugs.add(slug)
        flow_index[reg.flow_uuid] = slug

        flow_doc, layout_doc = normalize_flow(flow_dump, reg.flow_uuid, tokenizer)
        bundle.artifacts[layout.rel(layout.flow_path(slug))] = canonical_yaml_dump(flow_doc)
        bundle.artifacts[layout.rel(layout.flow_layout_path(slug))] = canonical_yaml_dump(layout_doc)

    return bundle, flow_index
