"""What a node needs from the compute that runs it, derived from its template and settings.

``requirements_for`` reads a node's ``NodeTemplate`` and its settings model and nothing else: no database, no
registry, no backend. Each family answers what its builder does on the backend: the operation it runs there
(``run_op``), the storage schemes the backend reads and writes for it, the plugin packages its work imports, the
stored connections it names and whether it runs on a notebook kernel. A node that only submits a plan, or works
in core, needs none of these.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.delta_write import MERGE_MODES
from flowfile_core.schemas.schemas import NodeTemplate
from shared.cloud_storage.uri import storage_type_for_uri
from shared.cloud_storage.utils import validate_cloud_resource_path
from shared.db_dialects import get_dialect_or_generic
from shared.path_utils import is_cloud_path, is_url, is_utf8_encoding

if TYPE_CHECKING:
    from flowfile_core.catalog.storage_backend import CatalogStorageTarget

_OBJECT_STORAGE_SCHEMES = {"s3": "s3", "adls": "az", "gcs": "gs"}
_LAZY_SCAN_FILE_TYPES = ("parquet", "ipc", "ndjson")
_FILE = frozenset({"file"})
_POLARS_DS = frozenset({"polars_ds"})
_POLARS_GROUPER = frozenset({"polars_grouper"})


@dataclass(frozen=True)
class NodeRequirements:
    """What one node needs from the backend that runs it.

    Attributes:
        operation: The operation the node runs on its backend, or ``None`` for a node that submits a plan or
            works in core.
        read_schemes: The schemes of the storage the backend reads for the node: ``file`` for a local path,
            ``s3``, ``az`` or ``gs`` for object storage, a URL's own ``http`` or ``https``, any other URI's own scheme.
        write_schemes: The schemes of the storage the backend writes for the node, spelled the same way.
        plugins: The plugin packages the node's work imports, as ``shared.compute_protocol.PLUGIN_PACKAGES``
            names them.
        credentials: The names of the stored connections the node uses. Secret names are not connections.
        needs_kernel: The node runs its code on a notebook kernel, which reads the node's inputs from the
            kernel shared volume, a ``file`` location on core's Docker host.
    """

    operation: OperationKind | None = None
    read_schemes: frozenset[str] = frozenset()
    write_schemes: frozenset[str] = frozenset()
    plugins: frozenset[str] = frozenset()
    credentials: frozenset[str] = frozenset()
    needs_kernel: bool = False


_ON_KERNEL = NodeRequirements(operation=OperationKind.file_write, write_schemes=_FILE, needs_kernel=True)
"""A kernel node: its inputs are written as parquet to the kernel shared volume, a ``file_write`` operation."""


def requirements_for(
    template: NodeTemplate,
    settings: Any,
    *,
    catalog_target: CatalogStorageTarget | None = None,
    artifact_scheme: str | None = None,
) -> NodeRequirements:
    """What the node with *template* and *settings* needs from the backend that runs it.

    A caller passes a ``FlowNode``'s ``node_template`` and ``setting_input``. A catalog reader's or writer's
    storage is its namespace's, which the caller resolves (``catalog.storage_backend.locate_for_namespace``) and
    passes as *catalog_target*; without one such a node claims no scheme and no connection. A Train Model node runs
    ``ml_train`` and an Apply Model node ``ml_apply``; the one that publishes its model to the catalog writes the
    artifact store and the one that takes its model from the catalog reads it, by the scheme the caller resolves from
    the store and passes as *artifact_scheme*, and neither claims a scheme otherwise or without one. A cloud reader
    or writer claims no scheme for a path it refuses itself when it runs (empty, or neither a URI nor absolute). A
    ``NodePromise``, a node not set up yet, answers only what its template says: whether it runs on a kernel.

    Args:
        template: The node's template.
        settings: The node's settings model, of the class its builder takes.
        catalog_target: Where a catalog node's table data lives, for a catalog reader or writer.
        artifact_scheme: The scheme the artifact store's bytes travel by, for a Train or Apply Model node: ``file``
            for the filesystem store, ``https`` for the S3 store's presigned URLs.
    """
    if isinstance(settings, input_schema.NodePromise):
        return NodeRequirements(needs_kernel=_kernel_by_template(template))
    if template.custom_node:
        return _custom_node(template, settings)
    if template.item == "catalog_reader":
        return _catalog_storage(catalog_target, write=False)
    if template.item == "catalog_writer":
        return _catalog_writer(settings, catalog_target)
    if template.item == "train_model":
        publishes = settings.train_input.publish_to_catalog and artifact_scheme is not None
        return NodeRequirements(
            operation=OperationKind.ml_train, write_schemes=frozenset({artifact_scheme}) if publishes else frozenset()
        )
    if template.item == "apply_model":
        fetches = settings.apply_input.source == "catalog" and artifact_scheme is not None
        return NodeRequirements(
            operation=OperationKind.ml_apply, read_schemes=frozenset({artifact_scheme}) if fetches else frozenset()
        )
    fixed = _BY_TEMPLATE.get(template.item)
    if fixed is not None:
        return fixed
    derive = _BY_SETTINGS.get(template.item)
    return derive(settings) if derive is not None else NodeRequirements()


def _scheme(location: str) -> str:
    """The scheme a backend reads or writes *location* by.

    A URL answers its own, ``http`` or ``https``; a path without a scheme is ``file``.
    """
    if is_url(location):
        return location.split("://", 1)[0]
    storage_type = storage_type_for_uri(location)
    if storage_type is not None:
        return _OBJECT_STORAGE_SCHEMES[storage_type]
    if is_cloud_path(location):
        return location.split("://", 1)[0]
    return "file"


def _names(*names: str | None) -> frozenset[str]:
    """The connection names that are set."""
    return frozenset(name for name in names if name)


def _kernel_by_template(template: NodeTemplate) -> bool:
    """Whether the template alone puts the node on a kernel: a Python Script node or a kernel custom node."""
    return template.item == "python_script" or bool(template.custom_node and template.execution_environment == "kernel")


def _custom_node(template: NodeTemplate, settings: input_schema.UserDefinedNode) -> NodeRequirements:
    """A custom node runs on a kernel when its manifest says so or its settings bind one, else as ``custom_node``."""
    if template.execution_environment == "kernel" or settings.kernel_id:
        return _ON_KERNEL
    return NodeRequirements(operation=OperationKind.custom_node)


def _catalog_storage(
    target: CatalogStorageTarget | None, *, write: bool, operation: OperationKind | None = None
) -> NodeRequirements:
    """A catalog node's scheme and connection, from the storage target of its namespace."""
    if target is None:
        return NodeRequirements(operation=operation)
    schemes = frozenset({_scheme(target.base)})
    return NodeRequirements(
        operation=operation,
        read_schemes=frozenset() if write else schemes,
        write_schemes=schemes if write else frozenset(),
        credentials=_names(target.connection_name),
    )


def _catalog_writer(settings: input_schema.NodeCatalogWriter, target: CatalogStorageTarget | None) -> NodeRequirements:
    """``virtual`` writes no data; ``scd2`` is ``delta_scd2``, a merge mode ``delta_merge``, else ``delta_write``."""
    write_mode = settings.catalog_write_settings.write_mode
    if write_mode == "virtual":
        return NodeRequirements()
    if write_mode == "scd2":
        operation = OperationKind.delta_scd2
    elif write_mode in MERGE_MODES:
        operation = OperationKind.delta_merge
    else:
        operation = OperationKind.delta_write
    return _catalog_storage(target, write=True, operation=operation)


def _read(settings: input_schema.NodeRead) -> NodeRequirements:
    """A parquet, IPC, NDJSON or UTF-8 CSV file is a lazy scan the node submits; any other is ``file_read``."""
    received = settings.received_file
    lazy_scan = received.file_type in _LAZY_SCAN_FILE_TYPES or (
        received.file_type == "csv" and is_utf8_encoding(received.table_settings.encoding)
    )
    return NodeRequirements(
        operation=None if lazy_scan else OperationKind.file_read, read_schemes=frozenset({_scheme(received.path)})
    )


def _output(settings: input_schema.NodeOutput) -> NodeRequirements:
    output = settings.output_settings
    return NodeRequirements(
        operation=OperationKind.file_write, write_schemes=frozenset({_scheme(output.abs_file_path or output.directory)})
    )


def _cloud_scheme(path: str, role: Literal["reader", "writer"]) -> frozenset[str]:
    """The scheme of a cloud node's path; none for one the node's own guard refuses, so it fails with that message."""
    try:
        validate_cloud_resource_path(path, role=role)
    except ValueError:
        return frozenset()
    return frozenset({_scheme(path)})


def _cloud_storage_reader(settings: input_schema.NodeCloudStorageReader) -> NodeRequirements:
    """A plan over the object-storage path, a change read included."""
    cloud = settings.cloud_storage_settings
    return NodeRequirements(
        read_schemes=_cloud_scheme(cloud.resource_path, "reader"), credentials=_names(cloud.connection_name)
    )


def _cloud_storage_writer(settings: input_schema.NodeCloudStorageWriter) -> NodeRequirements:
    """A Delta merge or a tracked Delta write runs the Delta operations; every other write is ``cloud_write``."""
    cloud = settings.cloud_storage_settings
    if cloud.file_format == "delta" and (cloud.write_mode in MERGE_MODES or cloud.track_changes):
        operation = OperationKind.delta_merge if cloud.write_mode in MERGE_MODES else OperationKind.delta_write
    else:
        operation = OperationKind.cloud_write
    return NodeRequirements(
        operation=operation,
        write_schemes=_cloud_scheme(cloud.resource_path, "writer"),
        credentials=_names(cloud.connection_name),
    )


def _database(
    database: input_schema.DatabaseSettings | input_schema.DatabaseWriteSettings,
    operation: OperationKind,
    *,
    write: bool,
) -> NodeRequirements:
    """Any mode but inline names a stored connection; an inline file-based database (SQLite, DuckDB) is a local file."""
    connection = database.database_connection
    file_based = (
        database.connection_mode == "inline"
        and connection is not None
        and get_dialect_or_generic(connection.database_type).file_based
    )
    schemes = _FILE if file_based else frozenset()
    by_reference = database.connection_mode != "inline"
    return NodeRequirements(
        operation=operation,
        read_schemes=frozenset() if write else schemes,
        write_schemes=schemes if write else frozenset(),
        credentials=_names(database.database_connection_name) if by_reference else frozenset(),
    )


def _database_reader(settings: input_schema.NodeDatabaseReader) -> NodeRequirements:
    return _database(settings.database_settings, OperationKind.database_read, write=False)


def _database_writer(settings: input_schema.NodeDatabaseWriter) -> NodeRequirements:
    return _database(settings.database_write_settings, OperationKind.database_write, write=True)


def _kafka_source(settings: input_schema.NodeKafkaSource) -> NodeRequirements:
    return NodeRequirements(
        operation=OperationKind.kafka_read, credentials=_names(settings.kafka_settings.kafka_connection_name)
    )


def _google_analytics_reader(settings: input_schema.NodeGoogleAnalyticsReader) -> NodeRequirements:
    return NodeRequirements(
        operation=OperationKind.google_analytics_read,
        credentials=_names(settings.google_analytics_settings.ga_connection_name),
    )


def _filter(settings: input_schema.NodeFilter) -> NodeRequirements:
    """An advanced filter is a formula; a basic one is built from a column, an operator and a value."""
    return NodeRequirements(plugins=_POLARS_DS if settings.filter_input.is_advanced() else frozenset())


def _gate(settings: input_schema.NodeGate) -> NodeRequirements:
    return NodeRequirements(plugins=_POLARS_DS if settings.gate_input.condition_source == "formula" else frozenset())


def _dynamic_rename(settings: input_schema.NodeDynamicRename) -> NodeRequirements:
    return NodeRequirements(
        plugins=_POLARS_DS if settings.dynamic_rename_input.rename_mode == "formula" else frozenset()
    )


_BY_TEMPLATE: dict[str, NodeRequirements] = {
    "python_script": _ON_KERNEL,
    "fuzzy_match": NodeRequirements(operation=OperationKind.fuzzy_join, plugins=frozenset({"polars_simed"})),
    "random_split": NodeRequirements(operation=OperationKind.random_split),
    "rest_api_reader": NodeRequirements(operation=OperationKind.rest_api_read),
    "graph_solver": NodeRequirements(plugins=_POLARS_GROUPER),
    "explode_hierarchy": NodeRequirements(plugins=_POLARS_GROUPER),
    "formula": NodeRequirements(plugins=_POLARS_DS),
    "multi_field_formula": NodeRequirements(plugins=_POLARS_DS),
}
"""The families whose requirements do not depend on their settings."""

_BY_SETTINGS: dict[str, Callable[[Any], NodeRequirements]] = {
    "read": _read,
    "output": _output,
    "cloud_storage_reader": _cloud_storage_reader,
    "cloud_storage_writer": _cloud_storage_writer,
    "database_reader": _database_reader,
    "database_writer": _database_writer,
    "kafka_source": _kafka_source,
    "google_analytics_reader": _google_analytics_reader,
    "filter": _filter,
    "gate": _gate,
    "dynamic_rename": _dynamic_rename,
}
"""The families whose requirements their settings decide."""
