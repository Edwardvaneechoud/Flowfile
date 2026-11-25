"""Core's twelve project kinds, registered in the order an import applies them.

Each export is a thin call into ``projection``; each ``cap`` is the most files or entries an import accepts.
Charts, dashboards and catalog tables are one file each; the root list an older folder holds them in stays among
their paths, so it is still read, and a save removes it."""

from __future__ import annotations

import json

import yaml

from flowfile_core.catalog.constants import is_seeded_namespace
from flowfile_core.project import importer, projection
from flowfile_core.project.kinds import Entry, ExportContext, Kind, register
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


def _load(text: str):
    return yaml.load(text, Loader=projection.YAML_LOADER)


def _canonical(value) -> str:
    """An entry's text for comparison: key order and comments do not count."""
    return json.dumps(value, sort_keys=True, default=str)


def _same_yaml(a: str, b: str) -> bool:
    """Whether two texts parse to the same value; ``False`` when either does not parse."""
    try:
        return _load(a) == _load(b)
    except Exception:
        return False


def _ns(entry: dict) -> tuple:
    namespace = entry.get("namespace") or {}
    return namespace.get("catalog"), namespace.get("schema")


def _dotted(*parts) -> str:
    return ".".join(str(p) for p in parts if p is not None)


def _list_entries(list_key: str, key, label):
    """Entries of a root file: one per item of its ``list_key`` list, compared by its parsed value."""

    def entries(files: dict[str, str]) -> dict:
        out = {}
        for path, text in files.items():
            for item in (_load(text) or {}).get(list_key) or []:
                out[key(item)] = Entry(label(item), path, _canonical(item))
        return out

    return entries


def _secret_entries(files: dict[str, str]) -> dict:
    return {
        name: Entry(name, path, name)
        for path, text in files.items()
        for name in (_load(text) or {}).get("required_secrets") or []
    }


def _namespace_entries(files: dict[str, str]) -> dict:
    """A catalog without its ``schemas`` list and each schema, so a schema change is one row. The namespaces the
    catalog never deletes are ``kept``, so no plan lists one as removed."""
    out = {}
    for path, text in files.items():
        for catalog in (_load(text) or {}).get("namespaces") or []:
            name = catalog.get("catalog")
            body = _canonical({k: v for k, v in catalog.items() if k != "schemas"})
            out[(name,)] = Entry(name, path, body, kept=is_seeded_namespace(name))
            for schema in catalog.get("schemas") or []:
                child = schema.get("name")
                out[(name, child)] = Entry(
                    _dotted(name, child), path, _canonical(schema), kept=is_seeded_namespace(name, child)
                )
    return out


def _per_file_entries(field: str, label_field: str | None = None):
    """Entries of a folder kind: one per file, keyed by ``field`` of the file, compared by the file's text."""

    def entries(files: dict[str, str]) -> dict:
        out = {}
        for path, text in files.items():
            data = _load(text) or {}
            out[data.get(field) or path] = Entry(str(data.get(label_field or field) or path), path, text)
        return out

    return entries


def _object_entries(list_key: str, field: str, label_field: str = "name", *, key=None, label=None):
    """Entries of a kind stored one object per file, keyed by ``field`` (or by ``key(object)``, a tuple for a
    composite key, with ``label(object)`` as its row). An older folder's root file gives one per item of its
    ``list_key`` list instead, which a file with the same key replaces; ``_same_yaml`` then calls an item and
    its file the same object."""
    key = key or (lambda obj: obj.get(field))
    label = label or (lambda obj: obj.get(label_field))

    def entries(files: dict[str, str]) -> dict:
        out = {}
        for path in sorted(files, key=lambda rel: "/" in rel):
            data = _load(files[path]) or {}
            if "/" in path:
                out[key(data) or path] = Entry(str(label(data) or path), path, files[path])
                continue
            for item in data.get(list_key) or []:
                out[key(item)] = Entry(str(label(item)), path, _canonical(item))
        return out

    return entries


def _file_stem(path: str) -> str:
    return path.rsplit("/", 1)[-1].split(".", 1)[0]


def _flow_entries(files: dict[str, str]) -> dict:
    """Keyed by the ``flow_uuid`` an exported file holds on its first line, so no flow file is parsed whole."""
    out = {}
    for path, text in files.items():
        first = text.split("\n", 1)[0]
        data = _load(first if first.startswith("flow_uuid:") else text) or {}
        out[data.get("flow_uuid") or path] = Entry(_file_stem(path), path, text)
    return out


def _schedule_entries(files: dict[str, str]) -> dict:
    """Keyed by the flow and the entry's ``id``, an entry without one by the importer's schedule signature."""
    out = {}
    for path, text in files.items():
        data = _load(text) or {}
        for entry in data.get("schedules") or []:
            key = (data.get("flow_uuid"), entry.get("id") or importer.schedule_signature(entry))
            label = f"{_file_stem(path)} {entry.get('name') or entry.get('schedule_type')}"
            out[key] = Entry(label, path, _canonical(entry))
    return out


register(
    Kind(
        "secrets",
        "secrets",
        ("secrets.yaml",),
        _manifest("secrets.yaml", projection.secret_manifest_data),
        entries_key="required_secrets",
        entries=_secret_entries,
    )
)
register(
    Kind(
        "database_connection",
        "database connection",
        ("connections/database/*.yaml",),
        lambda ctx: projection.export_connections(ctx.db, ctx.owner_id, "database"),
        after=("secrets",),
        cap=200,
        entries=_per_file_entries("connection_name"),
        delete_label="its stored password is not in any version",
        delete=importer._delete_database_connection,
        same=_same_yaml,
    )
)
register(
    Kind(
        "cloud_connection",
        "cloud connection",
        ("connections/cloud/*.yaml",),
        lambda ctx: projection.export_connections(ctx.db, ctx.owner_id, "cloud"),
        after=("secrets",),
        cap=200,
        entries=_per_file_entries("connection_name"),
        delete_label="its stored password is not in any version",
        delete=importer._delete_cloud_connection,
        same=_same_yaml,
    )
)
register(
    Kind(
        "namespaces",
        "namespaces",
        ("namespaces.yaml",),
        _manifest("namespaces.yaml", projection.namespace_manifest_data),
        cap=200,
        entries_key="namespaces",
        entries=_namespace_entries,
        delete_label="it is deleted once empty",
        delete=importer._delete_namespaces,
    )
)
register(
    Kind(
        "flow",
        "flow",
        ("flows/*.flow.yaml",),
        _export_flows,
        after=("namespaces", "database_connection", "cloud_connection"),
        cap=500,
        entries=_flow_entries,
        delete_label="its flow file is removed",
        delete=importer._delete_flow,
        same=_same_yaml,
    )
)
register(
    Kind(
        "schedule",
        "schedule",
        ("schedules/*.yaml",),
        _export_schedules,
        after=("flow",),
        cap=500,
        entries=_schedule_entries,
        delete_label="it no longer runs",
        delete=importer._delete_schedule,
    )
)
register(
    Kind(
        "tables",
        "catalog tables",
        ("tables/*.table.yaml", "tables.yaml"),
        lambda ctx: projection.export_tables(ctx.db, ctx.owner_id),
        after=("namespaces", "flow"),
        cap=500,
        tracked_by_data_flag=True,
        entries=_object_entries(
            "tables",
            "name",
            key=importer._table_key,
            label=lambda e: _dotted(*_ns(e), e.get("name")),
        ),
        delete_label="its data stays on disk",
        delete=importer._delete_tables,
        same=_same_yaml,
    )
)
register(
    Kind(
        "models",
        "models",
        ("models.yaml",),
        _manifest("models.yaml", projection.models_manifest_data),
        after=("namespaces", "flow"),
        cap=500,
        entries_key="models",
        tracked_by_data_flag=True,
        entries=_list_entries(
            "models",
            lambda e: (*_ns(e), e.get("name"), e.get("version")),
            lambda e: f"{_dotted(*_ns(e), e.get('name'))} v{e.get('version')}",
        ),
        delete_label="its file stays; the version is marked deleted",
        delete=importer._delete_models,
    )
)
register(
    Kind(
        "kernels",
        "kernels",
        ("kernels.yaml",),
        _manifest("kernels.yaml", projection.kernels_manifest_data),
        cap=50,
        entries_key="kernels",
        entries=_list_entries("kernels", lambda e: e.get("id"), lambda e: str(e.get("name") or e.get("id"))),
        delete_label="its configuration is deleted",
        delete=importer._delete_kernels,
    )
)
register(
    Kind(
        "visualizations",
        "visualizations",
        ("visualizations/*.visualization.yaml", "visualizations.yaml"),
        lambda ctx: projection.export_visualizations(ctx.db, ctx.owner_id),
        after=("namespaces", "tables"),
        cap=500,
        entries=_object_entries("visualizations", "viz_uuid"),
        delete_label="the chart is deleted",
        delete=importer._delete_visualizations,
        same=_same_yaml,
    )
)
register(
    Kind(
        "dashboards",
        "dashboards",
        ("dashboards/*.dashboard.yaml", "dashboards.yaml"),
        lambda ctx: projection.export_dashboards(ctx.db, ctx.owner_id),
        after=("visualizations",),
        cap=200,
        entries=_object_entries("dashboards", "dashboard_uuid"),
        delete_label="the dashboard is deleted",
        delete=importer._delete_dashboards,
        same=_same_yaml,
    )
)
register(
    Kind(
        "notebook",
        "notebook",
        ("notebooks/*.notebook.yaml",),
        lambda ctx: projection.export_notebooks(ctx.db, ctx.owner_id),
        after=("namespaces", "kernels"),
        cap=500,
        entries=_per_file_entries("notebook_uuid", "name"),
        delete_label="its content file is removed",
        delete=importer._delete_notebook,
        same=_same_yaml,
    )
)
