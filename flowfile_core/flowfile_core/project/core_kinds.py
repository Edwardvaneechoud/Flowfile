"""Core's twelve project kinds, registered in the order an import applies them.

Each export is a thin call into ``projection``; the caps are the importer's own constants."""

from __future__ import annotations

from flowfile_core.project import importer, projection
from flowfile_core.project.kinds import ExportContext, Kind, register
from flowfile_core.project.normalize import dump_yaml


def _flows(ctx: ExportContext) -> dict[str, tuple[str, str]]:
    if "flows" not in ctx.memo:
        ctx.memo["flows"] = projection.export_flows(ctx.db, ctx.owner_id)
    return ctx.memo["flows"]


def _export_flows(ctx: ExportContext) -> dict[str, str]:
    return {f"flows/{stem}.flow.yaml": text for stem, text in _flows(ctx).values()}


def _export_schedules(ctx: ExportContext) -> dict[str, str]:
    stems = {flow_uuid: stem for flow_uuid, (stem, _) in _flows(ctx).items()}
    return projection.export_schedules(ctx.db, ctx.owner_id, stems)


def _manifest(path: str, build):
    return lambda ctx: {path: dump_yaml(build(ctx.db, ctx.owner_id))}


register(
    Kind(
        "secrets",
        "secrets",
        ("secrets.yaml",),
        _manifest("secrets.yaml", projection.secret_manifest_data),
        entries_key="required_secrets",
    )
)
register(
    Kind(
        "database_connection",
        "database connection",
        ("connections/database/*.yaml",),
        lambda ctx: projection.export_connections(ctx.db, ctx.owner_id, "database"),
        after=("secrets",),
        cap=importer._MAX_CONNECTIONS,
    )
)
register(
    Kind(
        "cloud_connection",
        "cloud connection",
        ("connections/cloud/*.yaml",),
        lambda ctx: projection.export_connections(ctx.db, ctx.owner_id, "cloud"),
        after=("secrets",),
        cap=importer._MAX_CONNECTIONS,
    )
)
register(
    Kind(
        "namespaces",
        "namespaces",
        ("namespaces.yaml",),
        _manifest("namespaces.yaml", projection.namespace_manifest_data),
        cap=importer._MAX_NAMESPACES,
        entries_key="namespaces",
    )
)
register(
    Kind(
        "flow",
        "flow",
        ("flows/*.flow.yaml",),
        _export_flows,
        after=("namespaces", "database_connection", "cloud_connection"),
        cap=importer._MAX_FLOWS,
    )
)
register(
    Kind("schedule", "schedule", ("schedules/*.yaml",), _export_schedules, after=("flow",), cap=importer._MAX_FLOWS)
)
register(
    Kind(
        "tables",
        "catalog tables",
        ("tables.yaml",),
        _manifest("tables.yaml", projection.tables_manifest_data),
        after=("namespaces", "flow"),
        cap=importer._MAX_TABLES,
        entries_key="tables",
        tracked_by_data_flag=True,
    )
)
register(
    Kind(
        "models",
        "models",
        ("models.yaml",),
        _manifest("models.yaml", projection.models_manifest_data),
        after=("namespaces", "flow"),
        cap=importer._MAX_ARTIFACTS,
        entries_key="models",
        tracked_by_data_flag=True,
    )
)
register(
    Kind(
        "kernels",
        "kernels",
        ("kernels.yaml",),
        _manifest("kernels.yaml", projection.kernels_manifest_data),
        cap=importer._MAX_KERNELS,
        entries_key="kernels",
    )
)
register(
    Kind(
        "visualizations",
        "visualizations",
        ("visualizations.yaml",),
        _manifest("visualizations.yaml", projection.visualizations_manifest_data),
        after=("namespaces", "tables"),
        cap=importer._MAX_VISUALIZATIONS,
        entries_key="visualizations",
    )
)
register(
    Kind(
        "dashboards",
        "dashboards",
        ("dashboards.yaml",),
        _manifest("dashboards.yaml", projection.dashboards_manifest_data),
        after=("visualizations",),
        cap=importer._MAX_DASHBOARDS,
        entries_key="dashboards",
    )
)
register(
    Kind(
        "notebook",
        "notebook",
        ("notebooks/*.notebook.yaml",),
        lambda ctx: projection.export_notebooks(ctx.db, ctx.owner_id),
        after=("namespaces", "kernels"),
        cap=importer._MAX_NOTEBOOKS,
    )
)
