"""Pure ORM->DTO converters for the catalog domain.

Anything that needs ``repo`` (cross-row enrichment, name lookups) lives
on its owning sub-service instead.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from flowfile_core.database.models import FlowRun, GlobalArtifact
from flowfile_core.schemas.catalog_schema import (
    CatalogTablePreview,
    FlowRunOut,
    GlobalArtifactOut,
)
from shared.delta_utils import make_json_safe


@dataclass(frozen=True, slots=True)
class VizEnrichment:
    """Enrichment fields for a visualization payload (table + namespace names)."""

    table_name: str | None
    table_namespace_name: str | None
    table_full_name: str | None
    table_type: str | None
    namespace_name: str | None


def run_to_out(run: FlowRun, *, has_log: bool) -> FlowRunOut:
    """Convert a FlowRun ORM row to its FlowRunOut DTO.

    ``has_log`` is supplied by the caller because it requires filesystem
    access (which is not pure).
    """
    return FlowRunOut(
        id=run.id,
        registration_id=run.registration_id,
        flow_name=run.flow_name,
        flow_path=run.flow_path,
        user_id=run.user_id,
        started_at=run.started_at,
        ended_at=run.ended_at,
        success=run.success,
        nodes_completed=run.nodes_completed,
        number_of_nodes=run.number_of_nodes,
        duration_seconds=run.duration_seconds,
        run_type=run.run_type,
        schedule_id=run.schedule_id,
        has_snapshot=run.flow_snapshot is not None,
        has_log=has_log,
    )


def artifact_to_out(artifact: GlobalArtifact) -> GlobalArtifactOut:
    """Convert a GlobalArtifact ORM instance to its Pydantic output schema."""
    tags: list[str] = []
    if hasattr(artifact, "tags") and artifact.tags:
        if isinstance(artifact.tags, list):
            tags = artifact.tags
        elif isinstance(artifact.tags, str):
            try:
                tags = json.loads(artifact.tags)
            except (json.JSONDecodeError, TypeError):
                tags = [t.strip() for t in artifact.tags.split(",") if t.strip()]

    return GlobalArtifactOut(
        id=artifact.id,
        name=artifact.name,
        version=artifact.version,
        status=artifact.status,
        description=getattr(artifact, "description", None),
        python_type=getattr(artifact, "python_type", None),
        python_module=getattr(artifact, "python_module", None),
        serialization_format=getattr(artifact, "serialization_format", None),
        size_bytes=getattr(artifact, "size_bytes", None),
        sha256=getattr(artifact, "sha256", None),
        tags=tags,
        namespace_id=artifact.namespace_id,
        source_registration_id=getattr(artifact, "source_registration_id", None),
        source_flow_id=getattr(artifact, "source_flow_id", None),
        source_node_id=getattr(artifact, "source_node_id", None),
        owner_id=getattr(artifact, "owner_id", None),
        created_at=getattr(artifact, "created_at", None),
        updated_at=getattr(artifact, "updated_at", None),
    )


def format_pyarrow_preview(pa_table, total_rows: int | None = None) -> CatalogTablePreview:
    """Convert a PyArrow table to a CatalogTablePreview."""
    columns = pa_table.column_names
    dtypes = [str(field.type) for field in pa_table.schema]
    rows_data = pa_table.to_pylist()
    rows = [[make_json_safe(row.get(c)) for c in columns] for row in rows_data]
    return CatalogTablePreview(
        columns=columns,
        dtypes=dtypes,
        rows=rows,
        total_rows=total_rows if total_rows is not None else len(rows_data),
    )
