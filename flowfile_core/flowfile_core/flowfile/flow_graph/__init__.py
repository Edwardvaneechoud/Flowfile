"""The flow graph package: `FlowGraph` plus its concern mixins; this root is the import facade and the
bind surface for the collaborators tests patch (read through ``_root.root()``).
"""

from flowfile_core.catalog import CatalogService
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalDatabaseWriter,
    ExternalGoogleAnalyticsFetcher,
    ExternalRestApiFetcher,
    MLApplyFetcher,
    MLTrainFetcher,
)
from flowfile_core.flowfile.flow_graph.builders.io import list_files_schema, scan_directory_to_frame
from flowfile_core.flowfile.flow_graph.builders.ml import ml_flow_model_path
from flowfile_core.flowfile.flow_graph.catalog_resolution import (
    _CDF_COLUMN_DTYPES,
    _accessible_catalog_table_ids,
    _authorize_catalog_write,
    _resolve_catalog_sql_tables,
    _resolve_catalog_table_info,
    _resolve_virtual_table,
)
from flowfile_core.flowfile.flow_graph.catalog_write import (
    _collect_source_table_versions,
    _register_catalog_table,
    _scd2_primitive_kwargs,
    _write_catalog_delta_local,
    _write_catalog_delta_remote,
)
from flowfile_core.flowfile.flow_graph.cloud import (
    _cloud_change_read_target,
    _write_cloud_delta_remote,
    get_cloud_connection_settings,
)
from flowfile_core.flowfile.flow_graph.connections import (
    add_connection,
    delete_connection,
    format_source_target_detail,
    insert_node_on_edge,
    node_is_source,
    restore_dynamic_input_connections,
    validate_connection,
)
from flowfile_core.flowfile.flow_graph.execution import _gate_formula_matches
from flowfile_core.flowfile.flow_graph.freshness import _catalog_reader_source_fingerprint
from flowfile_core.flowfile.flow_graph.graph import FlowGraph
from flowfile_core.flowfile.flow_graph.history import EDIT_LOCK_TIMEOUT_SECONDS, GraphTransaction, placement_check
from flowfile_core.flowfile.user_defined.registry import registry as user_defined_registry
from flowfile_core.kernel import get_kernel_manager
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.output_model import RunInformation
from shared.kafka.consumer import infer_topic_schema, read_kafka_source

__all__ = [
    "FlowGraph",
    "GraphTransaction",
    "RunInformation",
    "EDIT_LOCK_TIMEOUT_SECONDS",
    "placement_check",
    "add_connection",
    "delete_connection",
    "insert_node_on_edge",
    "restore_dynamic_input_connections",
    "validate_connection",
    "format_source_target_detail",
    "node_is_source",
    "get_cloud_connection_settings",
    "list_files_schema",
    "scan_directory_to_frame",
    "ml_flow_model_path",
    "_CDF_COLUMN_DTYPES",
    "_accessible_catalog_table_ids",
    "_authorize_catalog_write",
    "_catalog_reader_source_fingerprint",
    "_cloud_change_read_target",
    "_collect_source_table_versions",
    "_gate_formula_matches",
    "_register_catalog_table",
    "_resolve_catalog_sql_tables",
    "_resolve_catalog_table_info",
    "_resolve_virtual_table",
    "_scd2_primitive_kwargs",
    "_write_catalog_delta_local",
    "_write_catalog_delta_remote",
    "_write_cloud_delta_remote",
    "CloudStorageReader",
    "CatalogService",
    "ExternalDatabaseWriter",
    "ExternalGoogleAnalyticsFetcher",
    "ExternalRestApiFetcher",
    "FullCloudStorageConnection",
    "MLApplyFetcher",
    "MLTrainFetcher",
    "get_db_context",
    "get_kernel_manager",
    "infer_topic_schema",
    "read_kafka_source",
    "user_defined_registry",
]
