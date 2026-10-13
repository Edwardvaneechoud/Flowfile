"""Connection-backed sources and sinks fetched on the worker: database, Kafka, Google Analytics, REST."""

import threading

from fastapi.exceptions import HTTPException

from flowfile_core.configs import logger
from flowfile_core.configs.app_settings import get_google_oauth_config
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_local_database_connection,
)
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    get_encrypted_credential,
    get_ga_connection,
)
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.payloads import (
    DatabaseReadPayload,
    DatabaseWritePayload,
    GoogleAnalyticsReadPayload,
    KafkaReadPayload,
    RestApiReadPayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import ComputeBackend, OperationKind, ResultRef
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.sources.external_sources.google_analytics_source import derive_schema
from flowfile_core.flowfile.sources.external_sources.rest_api_source import (
    resolve_auth_secret_encrypted,
)
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    SqlSource,
)
from flowfile_core.kafka.connection_manager import (
    build_consumer_config,
    get_kafka_connection,
    get_kafka_connection_by_name,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from flowfile_core.secret_manager.secret_manager import (
    _encrypt_with_master_key,
    decrypt_secret,
    get_encrypted_secret,
)
from shared.db_dialects import get_dialect_or_generic
from shared.kafka.consumer import make_kafka_commit_callback
from shared.kafka.models import KafkaReadSettings


def engine_from_read(backend: ComputeBackend, ref: ResultRef) -> FlowDataEngine:
    """The engine over a read operation's result, with the schema and the row count the reference carries.

    A deferred result is a plan that holds what it reads, so the backend's holding is released at once; a stored
    one stays held, because the plan scans the copy the backend stored (a worker read's Arrow file).
    """
    engine = FlowDataEngine(backend.plan_for(ref), schema=ref.schema, number_of_records=ref.row_count)
    if ref.state == "deferred":
        backend.release(ref)
    return engine


def _resolve_database_credentials(
    database_settings,
    user_id: int,
) -> tuple:
    """Resolve database connection and encrypted password from settings.

    Returns:
        (database_connection, encrypted_password, database_reference_settings)
        where database_reference_settings is the stored connection (or None for inline).
    """
    is_file_based = (
        database_settings.connection_mode == "inline"
        and database_settings.database_connection is not None
        and get_dialect_or_generic(database_settings.database_connection.database_type).file_based
    )
    if database_settings.connection_mode == "inline" and not is_file_based:
        database_connection = database_settings.database_connection
        encrypted_password = get_encrypted_secret(current_user_id=user_id, secret_name=database_connection.password_ref)
        if encrypted_password is None:
            raise HTTPException(status_code=400, detail="Password not found")
        return database_connection, encrypted_password, None
    elif is_file_based:
        return database_settings.database_connection, None, None
    else:
        ref_settings = get_local_database_connection(database_settings.database_connection_name, user_id)
        if ref_settings is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Database connection '{database_settings.database_connection_name}' not found "
                    "or not accessible for this user"
                ),
            )
        encrypted_password = ref_settings.password.get_secret_value()
        return ref_settings, encrypted_password, ref_settings


class ConnectorBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_database_writer(self, node_database_writer: input_schema.NodeDatabaseWriter):
        """Adds a node to write data to a database.

        Args:
            node_database_writer: The settings for the database writer node.
        """

        node_type = "database_writer"
        database_settings: input_schema.DatabaseWriteSettings = node_database_writer.database_write_settings

        def _func(df: FlowDataEngine):
            database_connection, encrypted_password, _ = _resolve_database_credentials(
                database_settings, node_database_writer.user_id
            )
            df.lazy = True
            table_name = (
                database_settings.schema_name + "." + database_settings.table_name
                if database_settings.schema_name
                else database_settings.table_name
            )
            write = DatabaseWritePayload(
                plan=df.data_frame,
                settings=node_database_writer,
                connection=database_connection,
                encrypted_password=encrypted_password,
                table_name=table_name,
                flow_id=node_database_writer.flow_id,
                node_id=node_database_writer.node_id,
            )
            node.run_op(
                current_context(self).backend, OperationKind.database_write, op_payload(write), job_id=write.job_id
            )
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(node_database_writer.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        self.add_node_step(
            node_id=node_database_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_database_writer,
            schema_callback=schema_callback,
            input_node_ids=[node_database_writer.depending_on_id],
        )
        node = self.get_node(node_database_writer.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_database_reader(self, node_database_reader: input_schema.NodeDatabaseReader):
        """Adds a node to read data from a database.

        Args:
            node_database_reader: The settings for the database reader node.
        """

        logger.info("Adding database reader")
        node_type = "database_reader"
        database_settings: input_schema.DatabaseSettings = node_database_reader.database_settings

        # Lazy and lock-memoized under the node's user_id: the schema callback runs on another thread.
        _creds: dict = {}
        _creds_lock = threading.Lock()

        def _get_creds():
            with _creds_lock:
                if "v" not in _creds:
                    _creds["v"] = _resolve_database_credentials(database_settings, node_database_reader.user_id)
                return _creds["v"]

        def _func():
            database_connection, encrypted_password, _ = _get_creds()
            backend = current_context(self).backend
            read = DatabaseReadPayload(
                flow_id=node_database_reader.flow_id,
                node_id=node_database_reader.node_id,
                settings=node_database_reader,
                connection=database_connection,
                encrypted_password=encrypted_password,
                cancel_check=lambda: self.flow_settings.is_canceled or node._execution_state.is_canceled,
            )
            # Every backend reads through shared.db_reader.read_sql_with_fallback.
            ref = node.run_op(backend, OperationKind.database_read, op_payload(read), job_id=read.job_id).refs["result"]
            fl = engine_from_read(backend, ref)
            node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback():
            # Prefer the cached schema so a saved flow renders columns without a live connection.
            if node_database_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_database_reader.fields]
            database_connection, encrypted_password, _ = _get_creds()
            sql_source = SqlSource(
                connection_string=sql_utils.construct_sql_uri(
                    database_type=database_connection.database_type,
                    host=database_connection.host,
                    port=database_connection.port,
                    database=database_connection.database,
                    username=database_connection.username,
                    password=decrypt_secret(encrypted_password) if encrypted_password else None,
                    ssl_enabled=bool(getattr(database_connection, "ssl_enabled", False)),
                    connect_timeout=10,
                ),
                query=None if database_settings.query_mode == "table" else database_settings.query,
                table_name=database_settings.table_name,
                schema_name=database_settings.schema_name,
                fields=node_database_reader.fields,
                database_type=database_connection.database_type,
            )
            return sql_source.get_schema()

        node = self.get_node(node_database_reader.node_id)
        is_new = node is None
        if node:
            # Persist so the lightweight callback survives the reset() that setting_input triggers.
            node.user_provided_schema_callback = schema_callback
            node.schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_database_reader
            node.node_settings.cache_results = node_database_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_database_reader.node_id,
                function=_func,
                setting_input=node_database_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            self._node_db[node_database_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_database_reader.node_id)
        self._notify_node_observers(node, is_new=is_new)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_kafka_source(self, node_kafka_source: input_schema.NodeKafkaSource):
        """Adds a node to read data from a Kafka or Redpanda topic.

        Follows the same pattern as add_database_reader: the execution context's
        backend consumes without committing and spills the rows to an IPC file;
        the offsets it reports are committed when the flow completes. Offset
        tracking is handled by Kafka consumer groups.

        Args:
            node_kafka_source: The settings for the Kafka source node.
        """

        logger.info("Adding kafka source")
        node_type = "kafka_source"
        kafka_settings = node_kafka_source.kafka_settings

        # Drop echoed fields when a schema-affecting setting changed, or the old topic's columns stick.
        prior_settings = getattr(self.get_node(node_kafka_source.node_id), "setting_input", None)
        if node_kafka_source.fields and isinstance(prior_settings, input_schema.NodeKafkaSource):
            prior_kafka = prior_settings.kafka_settings
            if (
                prior_kafka.topic_name != kafka_settings.topic_name
                or prior_kafka.value_format != kafka_settings.value_format
                or prior_kafka.kafka_connection_id != kafka_settings.kafka_connection_id
                or prior_kafka.kafka_connection_name != kafka_settings.kafka_connection_name
            ):
                node_kafka_source.fields = None

        # Lazy and lock-memoized under the node's user_id: the schema callback runs on another thread.
        _read_settings: dict = {}
        _read_settings_lock = threading.Lock()

        def _get_kafka_read_settings() -> KafkaReadSettings:
            with _read_settings_lock:
                if "v" not in _read_settings:
                    with get_db_context() as db:
                        db_conn = get_kafka_connection(
                            db, kafka_settings.kafka_connection_id, node_kafka_source.user_id
                        )
                        if db_conn is None:
                            if kafka_settings.kafka_connection_name:
                                db_conn = get_kafka_connection_by_name(
                                    db, kafka_settings.kafka_connection_name, node_kafka_source.user_id
                                )
                            if db_conn is None:
                                raise HTTPException(status_code=400, detail="Kafka connection not found")
                        consumer_config = build_consumer_config(db, db_conn, node_kafka_source.user_id)
                    _read_settings["v"] = KafkaReadSettings.from_consumer_config(
                        consumer_config,
                        topic=kafka_settings.topic_name,
                        value_format=kafka_settings.value_format,
                        group_id=kafka_settings.sync_name
                        or f"flowfile-{node_kafka_source.flow_id}-node-{node_kafka_source.node_id}",
                        start_offset=kafka_settings.start_offset,
                        max_messages=kafka_settings.max_messages,
                        poll_timeout_seconds=kafka_settings.poll_timeout_seconds,
                        flowfile_flow_id=node_kafka_source.flow_id,
                        flowfile_node_id=node_kafka_source.node_id,
                    )
                return _read_settings["v"]

        def _func():
            kafka_read_settings = _get_kafka_read_settings()
            backend = current_context(self).backend
            read = KafkaReadPayload(
                flow_id=node_kafka_source.flow_id,
                node_id=node_kafka_source.node_id,
                settings=kafka_read_settings,
                read_kafka_source=root().read_kafka_source,
            )
            result = node.run_op(backend, OperationKind.kafka_read, op_payload(read), job_id=read.job_id)
            fl = engine_from_read(backend, result.refs["result"])
            # Committed once the flow completes, so a failed or cancelled run commits nothing.
            offsets = result.metadata.get("offsets")
            if offsets is not None:
                node._on_flow_complete = make_kafka_commit_callback(
                    kafka_read_settings,
                    offsets,
                    node_kafka_source.node_id,
                    self.flow_logger,
                    _decrypt_fn,
                )
            # The read frame may have fewer columns than inferred (empty topic); align to the schema.
            expected_columns = schema_callback()
            fl = fl.align_to_schema(expected_columns)
            node_kafka_source.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def _decrypt_fn(encrypted: str) -> str:
            return decrypt_secret(encrypted).get_secret_value()

        def schema_callback():
            # Prefer the cached schema so a saved flow renders columns without sampling the topic.
            if node_kafka_source.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_kafka_source.fields]
            schema_pairs = root().infer_topic_schema(_get_kafka_read_settings(), sample_size=10, decrypt_fn=_decrypt_fn)
            # Since the schema callback takes quite some time, we only run the function once.
            if not schema_pairs:
                result = [
                    FlowfileColumn.from_input(column_name="_kafka_key", data_type="String"),
                    FlowfileColumn.from_input(column_name="_kafka_partition", data_type="Int64"),
                    FlowfileColumn.from_input(column_name="_kafka_offset", data_type="Int64"),
                    FlowfileColumn.from_input(column_name="_kafka_timestamp", data_type="Datetime"),
                ]
            else:
                result = [FlowfileColumn.create_from_polars_dtype(column_name=n, data_type=t) for n, t in schema_pairs]
            return result

        node = self.get_node(node_kafka_source.node_id)
        is_new = node is None
        if node:
            node.user_provided_schema_callback = schema_callback
            node.schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_kafka_source
            node.node_settings.cache_results = node_kafka_source.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_kafka_source.node_id,
                function=_func,
                setting_input=node_kafka_source,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            self._node_db[node_kafka_source.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_kafka_source.node_id)
        self._notify_node_observers(node, is_new=is_new)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_google_analytics_reader(self, node_ga_reader: input_schema.NodeGoogleAnalyticsReader) -> None:
        """Adds a node that reads from a Google Analytics 4 property.

        The actual API fetch (OAuth token refresh, ``run_report`` calls,
        pagination) runs on the worker backend in every execution mode, so the
        core's event loop stays responsive. The ``schema_callback`` is
        derived locally from the selected metrics/dimensions — no network call
        is made during schema prediction, keeping downstream nodes lazy.
        """
        logger.info("Adding google analytics reader")
        node_type = "google_analytics_reader"
        ga_settings = node_ga_reader.google_analytics_settings

        def _build_payload() -> GoogleAnalyticsReadPayload:
            # Deferred to run time under node_ga_reader.user_id: opening a flow never needs the connection.
            with get_db_context() as db:
                db_conn = get_ga_connection(db, ga_settings.ga_connection_name, node_ga_reader.user_id)
                if db_conn is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Google Analytics connection '{ga_settings.ga_connection_name}' not found "
                            "or has not completed sign-in"
                        ),
                    )
                auth_method = db_conn.auth_method
                encrypted_credential = get_encrypted_credential(
                    db, ga_settings.ga_connection_name, node_ga_reader.user_id
                )
                if encrypted_credential is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Google Analytics connection '{ga_settings.ga_connection_name}' has no stored credential"
                        ),
                    )
                # OAuth client config comes from the connection OWNER, not the run user (group-shared connections).
                oauth_cfg = get_google_oauth_config(db, db_conn.user_id) if auth_method == "oauth" else None

            common = dict(flow_id=node_ga_reader.flow_id, node_id=node_ga_reader.node_id, settings=node_ga_reader)
            if auth_method == "service_account":
                return GoogleAnalyticsReadPayload(
                    auth_method="service_account", encrypted_credential=encrypted_credential, **common
                )
            # oauth_cfg exists only for the oauth auth method; guard an unknown auth_method.
            if not oauth_cfg or not oauth_cfg["client_id"] or not oauth_cfg["client_secret"]:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Google OAuth is not configured on this instance. Open Admin → Google OAuth "
                        "and paste your OAuth client credentials before running this flow."
                    ),
                )
            return GoogleAnalyticsReadPayload(
                auth_method="oauth",
                encrypted_credential=encrypted_credential,
                oauth_client_id=oauth_cfg["client_id"],
                oauth_client_secret_encrypted=_encrypt_with_master_key(oauth_cfg["client_secret"]),
                **common,
            )

        # Stamp the predicted schema so downstream never needs _func (a worker -> Google round-trip).
        predicted_columns = derive_schema(metrics=ga_settings.metrics, dimensions=ga_settings.dimensions)
        node_ga_reader.fields = [c.get_minimal_field_info() for c in predicted_columns]

        def _func() -> FlowDataEngine:
            # On the worker in every mode: core has no Google Analytics reader of its own.
            backend = registry.backend(registry.WORKER_BACKEND_ID)
            read = _build_payload()
            result = node.run_op(backend, OperationKind.google_analytics_read, op_payload(read), job_id=read.job_id)
            ref = result.refs["result"]
            # A LazyFrame over the worker's Arrow IPC file; never collect in core.
            fl = engine_from_read(backend, ref)
            # Align (lazily) to the predicted schema so an empty report still yields stable columns.
            return fl.align_to_schema(schema_callback())

        def schema_callback() -> list[FlowfileColumn]:
            # Prefer the cached placeholder; derive_schema is the fallback when fields were cleared.
            if node_ga_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_ga_reader.fields]
            return derive_schema(metrics=ga_settings.metrics, dimensions=ga_settings.dimensions)

        node = self.get_node(node_ga_reader.node_id)
        is_new = node is None
        if node:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_ga_reader
            node.node_settings.cache_results = node_ga_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_ga_reader.node_id,
                function=_func,
                setting_input=node_ga_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            node.user_provided_schema_callback = schema_callback
            self._node_db[node_ga_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_ga_reader.node_id)
        self._notify_node_observers(node, is_new=is_new)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_rest_api_reader(self, node_rest_api_reader: input_schema.NodeRestApiReader) -> None:
        """Adds a node that reads from a REST API.

        All network I/O (HTTP round-trips, pagination, retries) runs on the
        execution context's backend — the worker unless the flow runs locally.
        The credential is resolved to an encrypted token here (from the user's
        secret store, or an inline plaintext) and the backend decrypts it
        just-in-time. A generic API's columns are unknown until a
        response is fetched, so ``schema_callback`` returns the columns cached on
        the node by the "Fetch sample" action — empty until the user samples or
        runs, in which case the fetched frame defines the schema.
        """
        logger.info("Adding rest api reader")
        node_type = "rest_api_reader"
        auth = node_rest_api_reader.rest_api_settings.auth

        # Inline plaintext is encrypted eagerly and nulled; the by-name lookup is deferred to run time.
        _inline_encrypted = _encrypt_with_master_key(auth.secret) if (auth.secret and not auth.secret_name) else None
        auth.secret = None

        def _resolve_secret_encrypted() -> str | None:
            if _inline_encrypted is not None:
                return _inline_encrypted
            return resolve_auth_secret_encrypted(auth, node_rest_api_reader.user_id)

        def _func() -> FlowDataEngine:
            backend = current_context(self).backend
            read = RestApiReadPayload(
                flow_id=node_rest_api_reader.flow_id,
                node_id=node_rest_api_reader.node_id,
                settings=node_rest_api_reader,
                encrypted_secret=_resolve_secret_encrypted(),
            )
            ref = node.run_op(backend, OperationKind.rest_api_read, op_payload(read), job_id=read.job_id).refs["result"]
            fl = engine_from_read(backend, ref)
            cols = schema_callback()
            # Align to the sampled schema when there is one; otherwise the fetched frame defines it.
            if cols:
                return fl.align_to_schema(cols)
            node_rest_api_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback() -> list[FlowfileColumn]:
            if node_rest_api_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_rest_api_reader.fields]
            return []

        node = self.get_node(node_rest_api_reader.node_id)
        is_new = node is None
        if node:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_rest_api_reader
            node.node_settings.cache_results = node_rest_api_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_rest_api_reader.node_id,
                function=_func,
                setting_input=node_rest_api_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            node.user_provided_schema_callback = schema_callback
            self._node_db[node_rest_api_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_rest_api_reader.node_id)
        self._notify_node_observers(node, is_new=is_new)
