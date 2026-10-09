"""Cloud storage nodes: connection resolution, change-feed read targets and cloud Delta writes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from fastapi.exceptions import HTTPException

from flowfile_core.auth import sharing
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_local_cloud_connection,
)
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.catalog_write import _delta_op
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageAuthMode,
    CloudStorageReadSettings,
    CloudStorageSettings,
    CloudStorageWriteSettings,
    FullCloudStorageConnection,
)
from flowfile_core.schemas.delta_write import MERGE_MODES
from shared.cloud_storage.uri import storage_type_for_uri
from shared.cloud_storage.utils import normalize_delta_path, validate_cloud_resource_path
from shared.delta_utils import (
    merge_into_delta,
)
from shared.delta_utils import write_delta as _write_delta

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


def get_cloud_connection_settings(
    connection_name: str | None,
    user_id: int,
    auth_mode: CloudStorageAuthMode,
    resource_path: str | None = None,
) -> FullCloudStorageConnection:
    """Resolve a cloud node's connection: the referenced saved one, else the process's own credentials.

    The ambient fallback is refused in multi-user (docker) mode before any credential lookup.

    Args:
        connection_name: The name of the saved connection, if any.
        user_id: The ID of the user running the node (own connections first, then group-granted).
        auth_mode: The authentication method specified on the node.
        resource_path: The node's parameter-resolved path; selects the ambient storage type.

    Returns:
        A FullCloudStorageConnection object with the connection details.

    Raises:
        HTTPException: If the referenced connection cannot be found, or no ambient mode applies.
        ValueError: If no connection is referenced and ambient credentials are not allowed.
    """
    if connection_name:
        connection = get_local_cloud_connection(connection_name, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="Cloud connection settings not found")
        return connection
    if not sharing.ambient_credentials_allowed():
        raise ValueError("Select a cloud storage connection; server credentials are not available in multi-user mode.")
    storage_type = storage_type_for_uri(resource_path or "") or "s3"
    if auth_mode == "aws-cli" and storage_type == "s3":
        auth_method = "aws-cli"
    elif auth_mode in ("env_vars", "auto", "aws-cli"):
        auth_method = "env_vars"
    else:
        raise HTTPException(status_code=400, detail="Cloud connection settings not found")
    return root().FullCloudStorageConnection(storage_type=storage_type, auth_method=auth_method, connection_name=None)


def _resolve_cloud_node_connection(
    settings: CloudStorageSettings, user_id: int, role: Literal["reader", "writer"]
) -> FullCloudStorageConnection:
    """Guard a cloud node's path (docker also refuses local paths), then resolve its connection."""
    validate_cloud_resource_path(
        settings.resource_path, role=role, allow_local_paths=sharing.ambient_credentials_allowed()
    )
    return get_cloud_connection_settings(
        settings.connection_name, user_id, settings.auth_mode, resource_path=settings.resource_path
    )


def _cloud_write_uses_delta_ops(settings: CloudStorageWriteSettings) -> bool:
    """Whether a cloud writer needs the catalog's Delta operations rather than the streaming sink."""
    return settings.file_format == "delta" and (settings.write_mode in MERGE_MODES or settings.track_changes)


def _write_cloud_delta_remote(flow_id: int, node: FlowNode, df: FlowDataEngine, op_type: str, op_kwargs: dict) -> None:
    """Run a cloud writer's Delta merge or tracked write on the worker; the table metadata it returns is unused."""
    root()._write_catalog_delta_remote(
        flow_id, node, df, op_type, op_kwargs, table_label=f"at '{op_kwargs['output_path']}'"
    )


def _write_cloud_delta(
    graph: FlowGraph,
    node: FlowNode,
    df: FlowDataEngine,
    settings: CloudStorageWriteSettings,
    connection: FullCloudStorageConnection,
    user_id: int,
) -> None:
    """Upsert/update/delete into, or write with change tracking to, a Delta table on a bare cloud path.

    Dispatches the same ``merge_delta`` / ``write_delta`` operations the catalog writer uses, with the
    connection shipped owner-encrypted for the worker to decrypt. Local execution merges in-process: the
    sanctioned local-execution collect, since headless runs have no worker to offload to.
    """
    if connection.storage_type == "gcs":
        raise ValueError("Delta upsert/update/delete and change tracking are not supported on Google Cloud Storage yet")
    path = normalize_delta_path(settings.resource_path)
    op_type, op_kwargs = _delta_op(
        settings.write_mode,
        output_path=path,
        merge_keys=settings.merge_keys,
        partition_by=settings.partition_by,
        enable_cdf=settings.track_changes,
    )
    if graph.execution_location != "local":
        op_kwargs["storage_payload"] = {"connection": connection.get_worker_interface(user_id).model_dump()}
        root()._write_cloud_delta_remote(graph.flow_id, node, df, op_type, op_kwargs)
        return
    storage_options = CloudStorageReader.get_storage_options(connection)
    if op_type == "merge_delta":
        merge_into_delta(
            df.data_frame.collect(),
            path,
            merge_mode=settings.write_mode,
            merge_keys=settings.merge_keys,
            partition_by=settings.partition_by,
            storage_options=storage_options,
            enable_cdf=settings.track_changes,
        )
    else:
        _write_delta(
            df.data_frame,
            path,
            mode=settings.write_mode,
            partition_by=settings.partition_by,
            storage_options=storage_options,
            enable_cdf=settings.track_changes,
        )


def _is_cloud_change_read(settings: CloudStorageReadSettings) -> bool:
    """Whether a cloud reader reads a Delta table's change feed instead of the table itself."""
    return settings.file_format == "delta" and settings.cdc_mode != "off"


def _cloud_change_read_target(settings: CloudStorageReadSettings, user_id: int) -> tuple[str, dict]:
    """The normalised Delta path and storage options a cloud change read opens the table with."""
    connection = _resolve_cloud_node_connection(settings, user_id, role="reader")
    if connection.storage_type == "gcs":
        raise ValueError("Reading Delta changes is not supported on Google Cloud Storage yet")
    return normalize_delta_path(settings.resource_path), CloudStorageReader.get_storage_options(connection)
