"""Catalog-cell metadata transport through the kernel client's authenticated routes.

Canvas notebook sessions retain their session transport. Catalog cells have no
canvas session and must never fall back to its unfiltered metadata lookup.
"""

import json
from typing import Any

import httpx

from kernel_runtime import flowfile_client as client


def _namespace(row: dict) -> dict:
    return {key: row[key] for key in ("id", "name", "parent_id")}


def _physical_path(path: str) -> str:
    # The graph API and shared package are installed by the notebook image.
    from shared.cloud_storage.uri import is_cloud_uri

    if is_cloud_uri(path):
        raise ValueError(
            "Cloud-backed physical tables are not supported by flowfile_frame reads or SQL in Catalog notebooks. "
            "Use a flow with a Catalog Reader node instead."
        )
    return client._translate_host_path_to_container(path)


def _table_info(row: dict) -> dict:
    path = row.get("file_path")
    return {
        "file_path": _physical_path(path) if path and row["table_type"] != "virtual" else None,
        "table_type": row["table_type"],
        # Core's producer fingerprint is not exposed by the catalog routes.
        "producer_fingerprint": None,
        "is_optimized": False,
        "source_table_versions": row.get("source_table_versions"),
        "authorized": True,  # Only reached after a successful authorized route response.
        "table_id": row["id"],
        "namespace_id": row.get("namespace_id"),
        "table_name": row["name"],
        "scd2_config": row.get("scd2"),
        "schema_json": json.dumps(row.get("schema_columns", [])),
        "cdc_enabled": row.get("cdc_enabled", False),
        "cdc_enabled_version": row.get("cdc_enabled_version"),
    }


def lookup(payload: dict[str, Any]) -> dict[str, Any]:
    """Answer the catalog metadata hook; HTTP failures propagate without local fallback."""
    kind, args = payload["kind"], payload.get("args", {})
    with httpx.Client(timeout=30.0, headers=client._get_internal_auth_headers()) as connection:

        def get(path: str, **params):
            return client._catalog_get(connection, "/catalog/" + path, params).json()

        def namespaces(parent_id=None):
            return [_namespace(row) for row in get("namespaces", parent_id=parent_id)]

        def all_namespaces():
            catalogs = namespaces()
            return catalogs + [schema for catalog in catalogs for schema in namespaces(catalog["id"])]

        if kind == "namespaces":
            result = namespaces(args.get("parent_id"))
        elif kind == "namespace_by_name":
            result = next((row for row in namespaces(args.get("parent_id")) if row["name"] == args["name"]), None)
        elif kind == "namespace":
            result = next((row for row in all_namespaces() if row["id"] == args["namespace_id"]), None)
        elif kind == "default_namespace_id":
            result = get("default-namespace-id")
        elif kind in ("namespace_full_name", "namespace_id_by_full_name"):
            rows = all_namespaces()
            names = {row["id"]: row["name"] for row in rows}
            paths = {
                row["id"]: f"{names[row['parent_id']]}.{row['name']}" if row["parent_id"] is not None else row["name"]
                for row in rows
            }
            if kind == "namespace_full_name":
                result = paths.get(args.get("namespace_id"))
            else:
                result = next((identity for identity, name in paths.items() if name == args["full_name"]), None)
        elif kind == "tables":
            result = get("tables", namespace_id=args["namespace_id"])
        elif kind == "catalog_table":
            table_id = args.get("catalog_table_id")
            if table_id is not None:
                row = get(f"tables/{table_id}")
            else:
                reference = args.get("catalog_full_table_name") or args.get("catalog_table_name")
                if not reference:
                    raise ValueError("A catalog table reference is required")
                row = client._resolve_catalog_table(
                    connection,
                    reference,
                    args.get("catalog_namespace_id"),
                    strict=True,
                )
                if row is None:
                    raise LookupError(f"Catalog table {reference!r} not found")
            result = _table_info(row)
            if row["table_type"] == "virtual":
                result["materialized"] = client._materialize_virtual_table(connection, row, row["name"])
        elif kind == "catalog_sql_tables":
            from shared.catalog_sql import is_table_reference

            query = args["sql_query"]
            paths, namespaces_by_name, materialized = {}, {}, {}
            for row in get("tables"):
                if not is_table_reference(row["name"], query):
                    continue
                if row["table_type"] == "virtual":
                    materialized[row["name"]] = client._materialize_virtual_table(connection, row, row["name"])
                elif row.get("file_path"):
                    paths[row["name"]] = _physical_path(row["file_path"])
                namespaces_by_name[row["name"]] = row.get("namespace_id")
            result = {
                "table_paths": paths,
                "virtual_tables": {},
                "table_namespaces": namespaces_by_name,
                "materialized_tables": materialized,
            }
        else:
            raise NotImplementedError(f"Catalog-cell metadata lookup {kind!r} is not supported")
    return {"result": result}
