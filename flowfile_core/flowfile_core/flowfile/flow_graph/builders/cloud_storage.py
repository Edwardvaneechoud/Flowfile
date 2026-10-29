"""Object-storage reader and writer nodes (S3, ADLS, GCS)."""

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.configs.settings import FLOWFILE_COMPUTE_PROTOCOL
from flowfile_core.flowfile.catalog_cdc import (
    resolve_change_window,
)
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.payloads import CloudWritePayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.catalog_resolution import _CDF_COLUMN_DTYPES
from flowfile_core.flowfile.flow_graph.cloud import (
    _cloud_change_read_target,
    _cloud_change_scan_kwargs,
    _cloud_write_uses_delta_ops,
    _is_cloud_change_read,
    _resolve_cloud_node_connection,
    _write_cloud_delta,
)
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.schema_callbacks import (
    pl_schema_to_flowfile_columns,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettingsInternal
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from shared.delta_utils import (
    get_change_data_feed_floor,
    get_delta_head_version,
    scan_delta_changes,
)


class CloudStorageBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cloud_storage_writer(self, node_cloud_storage_writer: input_schema.NodeCloudStorageWriter) -> None:
        """Adds a node to write data to a cloud storage provider.

        Args:
            node_cloud_storage_writer: The settings for the cloud storage writer node.
        """

        node_type = "cloud_storage_writer"

        def _func(df: FlowDataEngine):
            df.lazy = True
            write_settings = node_cloud_storage_writer.cloud_storage_settings
            user_id = node_cloud_storage_writer.user_id
            connection = _resolve_cloud_node_connection(write_settings, user_id, role="writer")
            if _cloud_write_uses_delta_ops(write_settings):
                _write_cloud_delta(self, node, df, write_settings, connection, user_id)
                return df
            write = CloudWritePayload(
                plan=df.data_frame,
                settings=write_settings,
                connection=connection.get_worker_interface(user_id),
                flow_id=self.flow_id,
                node_id=node_cloud_storage_writer.node_id,
            )
            node.run_op(
                current_context(self).backend, OperationKind.cloud_write, op_payload(write), job_id=write.job_id
            )
            return df

        def schema_callback():
            logger.info("Starting to run the schema callback for cloud storage writer")
            if self.get_node(node_cloud_storage_writer.node_id).is_correct:
                return self.get_node(node_cloud_storage_writer.node_id).node_inputs.main_inputs[0].schema
            else:
                return [FlowfileColumn.from_input(column_name="__error__", data_type="String")]

        self.add_node_step(
            node_id=node_cloud_storage_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_cloud_storage_writer,
            schema_callback=schema_callback,
            input_node_ids=[node_cloud_storage_writer.depending_on_id],
        )

        node = self.get_node(node_cloud_storage_writer.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cloud_storage_reader(self, node_cloud_storage_reader: input_schema.NodeCloudStorageReader) -> None:
        """Adds a cloud storage read node to the flow graph.

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, a plain read takes its schema from the reader's backend, which resolves
        the read's connection. A change read, and every read with the flag off, resolves its schema in core.

        Args:
            node_cloud_storage_reader: The settings for the cloud storage read node.
        """
        node_type = "cloud_storage_reader"
        logger.info("Adding cloud storage reader")
        cloud_storage_read_settings = node_cloud_storage_reader.cloud_storage_settings
        read_changes = _is_cloud_change_read(cloud_storage_read_settings)

        def _read_changes() -> FlowDataEngine:
            """Read the Delta table's change feed for this run.

            Floor and head are resolved here, at execution time, for the same reason as the catalog
            reader's: the designer keeps one FlowGraph across runs. Strict by design — an untracked
            table is an actionable error, never a silent full read.
            """
            path, storage_options = _cloud_change_read_target(
                cloud_storage_read_settings, node_cloud_storage_reader.user_id
            )
            floor = get_change_data_feed_floor(path, storage_options=storage_options)
            if floor is None:
                raise ValueError(
                    f"Change tracking is not enabled on {path}. Enable it in the reader settings, "
                    "or write the table with 'Track changes' on, then re-run."
                )
            head = get_delta_head_version(path, storage_options=storage_options)
            starting_version = resolve_change_window(
                cloud_storage_read_settings.cdc_mode,
                cloud_storage_read_settings.cdc_from_version,
                cloud_storage_read_settings.cdc_from_timestamp,
                head=head,
                floor=floor,
                path=path,
                storage_options=storage_options,
            )
            self.flow_logger.get_node_logger(node_cloud_storage_reader.node_id).info(
                f"Reading changes from {path} (v{starting_version}..v{head})"
            )
            return FlowDataEngine(
                scan_delta_changes(
                    path,
                    starting_version,
                    head,
                    **_cloud_change_scan_kwargs(
                        cloud_storage_read_settings, node_cloud_storage_reader.user_id, storage_options
                    ),
                    include_preimage=cloud_storage_read_settings.cdc_include_preimage,
                )
            )

        def _cdc_schema_callback() -> list[FlowfileColumn]:
            """Predicted schema of a change read: the table's columns (log metadata only) plus the feed columns."""
            path, storage_options = _cloud_change_read_target(
                cloud_storage_read_settings, node_cloud_storage_reader.user_id
            )
            columns = pl_schema_to_flowfile_columns(
                pl.scan_delta(path, storage_options=storage_options).collect_schema()
            )
            columns.extend(
                FlowfileColumn.from_input(column_name=name, data_type=dtype) for name, dtype in _CDF_COLUMN_DTYPES
            )
            return columns

        def _func():
            if read_changes:
                return _read_changes()
            logger.info("Starting to run the schema callback for cloud storage reader")
            self.flow_logger.info("Starting to run the schema callback for cloud storage reader")
            settings = CloudStorageReadSettingsInternal(
                read_settings=cloud_storage_read_settings,
                connection=_resolve_cloud_node_connection(
                    cloud_storage_read_settings, node_cloud_storage_reader.user_id, role="reader"
                ),
            )
            backend = current_context(self).backend if FLOWFILE_COMPUTE_PROTOCOL else None
            fl = FlowDataEngine.from_cloud_storage_obj(
                settings, user_id=node_cloud_storage_reader.user_id, backend=backend
            )
            return fl

        node = self.add_node_step(
            node_id=node_cloud_storage_reader.node_id,
            function=_func,
            cache_results=node_cloud_storage_reader.cache_results,
            setting_input=node_cloud_storage_reader,
            node_type=node_type,
            schema_callback=_cdc_schema_callback if read_changes else None,
        )
        self.add_node_to_starting_list(node)
