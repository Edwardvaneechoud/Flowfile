"""Connection-backed sources and sinks fetched on the worker: database, Kafka, Google Analytics, REST."""

import os
import threading

import polars as pl
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
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalDatabaseFetcher,
    ExternalKafkaFetcher,
    fetch_kafka_offsets,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.sources.external_sources.google_analytics_source import derive_schema
from flowfile_core.flowfile.sources.external_sources.rest_api_source import (
    build_rest_api_worker_settings,
    resolve_auth_secret_encrypted,
)
from flowfile_core.flowfile.sources.external_sources.sql_source import models as sql_models
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    BaseSqlSource,
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
from shared.google_analytics.models import (
    GoogleAnalyticsFilter as WorkerGoogleAnalyticsFilter,
)
from shared.google_analytics.models import (
    GoogleAnalyticsOrderBy as WorkerGoogleAnalyticsOrderBy,
)
from shared.google_analytics.models import (
    GoogleAnalyticsReadSettings as WorkerGoogleAnalyticsReadSettings,
)
from shared.kafka.consumer import make_kafka_commit_callback
from shared.kafka.models import KafkaReadSettings


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
            database_connection, encrypted_password, database_reference_settings = _resolve_database_credentials(
                database_settings, node_database_writer.user_id
            )
            df.lazy = True
            table_name = (
                database_settings.schema_name + "." + database_settings.table_name
                if database_settings.schema_name
                else database_settings.table_name
            )

            if self.execution_location == "local":
                df.to_database_obj(
                    database_type=database_connection.database_type,
                    uri=sql_utils.construct_sql_uri(
                        database_type=database_connection.database_type,
                        host=database_connection.host,
                        port=database_connection.port,
                        database=database_connection.database,
                        username=database_connection.username,
                        password=decrypt_secret(encrypted_password) if encrypted_password else None,
                        ssl_enabled=bool(getattr(database_connection, "ssl_enabled", False)),
                        connect_timeout=10,
                    ),
                    table_name=table_name,
                    if_exists=database_settings.if_exists or "append",
                )
                return df

            database_external_write_settings = (
                sql_models.DatabaseExternalWriteSettings.create_from_from_node_database_writer(
                    node_database_writer=node_database_writer,
                    password=encrypted_password,
                    table_name=table_name,
                    database_reference_settings=(
                        database_reference_settings if database_settings.connection_mode == "reference" else None
                    ),
                    lf=df.data_frame,
                )
            )
            external_database_writer = root().ExternalDatabaseWriter(
                database_external_write_settings, wait_on_completion=False
            )
            node._fetch_cached_df = external_database_writer
            external_database_writer.get_result()
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

        # Resolve the connection lazily so opening/undoing a flow never requires
        # the current session to own the connection. Memoized so ``_func`` and
        # ``schema_callback`` share a single lookup; the lock matters because the
        # schema callback runs on a background thread (``SingleExecutionFuture``)
        # while ``_func`` runs on the execution thread. Runs under the node's
        # ``user_id`` (the flow owner at execution time).
        _creds: dict = {}
        _creds_lock = threading.Lock()

        def _get_creds():
            with _creds_lock:
                if "v" not in _creds:
                    _creds["v"] = _resolve_database_credentials(database_settings, node_database_reader.user_id)
                return _creds["v"]

        def _func():
            database_connection, encrypted_password, database_reference_settings = _get_creds()
            sql_source = BaseSqlSource(
                query=None if database_settings.query_mode == "table" else database_settings.query,
                table_name=database_settings.table_name,
                schema_name=database_settings.schema_name,
                fields=node_database_reader.fields,
            )

            # Local and worker reads share shared.db_reader.read_sql_with_fallback
            # (via SqlSource here, via read_sql_source in the worker).
            if self.execution_location == "local":
                local_source = SqlSource(
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
                    cancel_check=lambda: self.flow_settings.is_canceled or node._execution_state.is_canceled,
                    database_type=database_connection.database_type,
                )
                fl = FlowDataEngine(local_source.get_pl_df())
                fl.lazy = True
                node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
                return fl

            database_external_read_settings = (
                sql_models.DatabaseExternalReadSettings.create_from_from_node_database_reader(
                    node_database_reader=node_database_reader,
                    password=encrypted_password,
                    query=sql_source.query,
                    database_reference_settings=(
                        database_reference_settings if database_settings.connection_mode == "reference" else None
                    ),
                )
            )

            external_database_fetcher = ExternalDatabaseFetcher(
                database_external_read_settings, wait_on_completion=False
            )
            node._fetch_cached_df = external_database_fetcher
            fl = FlowDataEngine(external_database_fetcher.get_result())
            node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback():
            # Prefer the schema cached on the node so opening a saved flow renders
            # columns without a live connection. Fall back to the connection only
            # when fields were never captured (failures here are caught per-node).
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

        Follows the same pattern as add_database_reader: offloads consumption
        to the worker, which writes an IPC temp file and returns a serialized
        LazyFrame reference. Offset tracking is handled by Kafka consumer groups.

        Args:
            node_kafka_source: The settings for the Kafka source node.
        """

        logger.info("Adding kafka source")
        node_type = "kafka_source"
        kafka_settings = node_kafka_source.kafka_settings

        # Settings updates may echo back ``fields`` cached from a previous topic /
        # format / connection (the UI clears them, but programmatic callers may
        # not). Stale fields would make ``schema_callback`` report the old topic's
        # columns, so drop them whenever a schema-affecting setting changed.
        # Open/undo replays keep their fields: the replayed settings match the
        # node's previous ones (or the prior node is just a promise).
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

        # Resolve the connection lazily so opening/undoing a flow never requires
        # the current session to own the connection. Memoized so ``_func`` and
        # ``schema_callback`` share a single lookup; the lock matters because the
        # schema callback runs on a background thread (``SingleExecutionFuture``)
        # while ``_func`` runs on the execution thread. Runs under the node's
        # ``user_id`` (the flow owner at execution time).
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
            if self.execution_location == "local":
                # Local execution — consume directly in-process with spill-to-IPC
                import tempfile

                fd, spill_file = tempfile.mkstemp(suffix=".arrow", prefix="kafka_")
                os.close(fd)
                result, kafka_result = root().read_kafka_source(
                    kafka_read_settings,
                    commit=False,
                    decrypt_fn=_decrypt_fn,
                    spill_path=spill_file,
                )
                lf = result if isinstance(result, pl.LazyFrame) else result.lazy()
                fl = FlowDataEngine(lf)
                if kafka_result.messages_consumed > 0:
                    node._on_flow_complete = make_kafka_commit_callback(
                        kafka_read_settings,
                        kafka_result.new_offsets,
                        node_kafka_source.node_id,
                        self.flow_logger,
                        _decrypt_fn,
                    )
            else:
                # Remote execution — offload to worker (worker uses commit=False + sidecar)
                external_kafka_fetcher = ExternalKafkaFetcher(kafka_read_settings, wait_on_completion=False)
                node._fetch_cached_df = external_kafka_fetcher
                fl = FlowDataEngine(external_kafka_fetcher.get_result())
                offsets_data = fetch_kafka_offsets(external_kafka_fetcher.file_ref)
                if offsets_data and offsets_data.get("messages_consumed", 0) > 0:
                    node._on_flow_complete = make_kafka_commit_callback(
                        kafka_read_settings,
                        offsets_data["new_offsets"],
                        node_kafka_source.node_id,
                        self.flow_logger,
                        _decrypt_fn,
                    )
            # The worker DataFrame may have fewer columns than the inferred
            # schema (e.g. empty topic or starting at "latest"). Align to
            # the schema_callback result so downstream nodes see stable columns.
            expected_columns = schema_callback()
            fl = fl.align_to_schema(expected_columns)
            node_kafka_source.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def _decrypt_fn(encrypted: str) -> str:
            return decrypt_secret(encrypted).get_secret_value()

        def schema_callback():
            # Prefer the schema cached on the node so opening a saved flow renders
            # columns without sampling the topic (a live connection). Sampling only
            # runs when fields were never captured (failures are caught per-node).
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
        pagination) is offloaded to the worker via ``ExternalGoogleAnalyticsFetcher``,
        so the core's event loop stays responsive. The ``schema_callback`` is
        derived locally from the selected metrics/dimensions — no network call
        is made during schema prediction, keeping downstream nodes lazy.
        """
        logger.info("Adding google analytics reader")
        node_type = "google_analytics_reader"
        ga_settings = node_ga_reader.google_analytics_settings

        def _build_worker_settings() -> WorkerGoogleAnalyticsReadSettings:
            # Connection resolution is deferred to run time so that *opening* or
            # *undoing* a flow never requires the current session to own the
            # connection (mirrors ``add_cloud_storage_reader``). It runs under
            # ``node_ga_reader.user_id`` — the flow owner at execution time.
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
                # OAuth needs the per-instance client config; service accounts don't.
                # Resolved from the CONNECTION OWNER, not the run user: a group-shared
                # OAuth connection must use the owner's Google client config.
                oauth_cfg = get_google_oauth_config(db, db_conn.user_id) if auth_method == "oauth" else None

            common_kwargs = dict(
                property_id=ga_settings.property_id,
                start_date=ga_settings.start_date,
                end_date=ga_settings.end_date,
                metrics=ga_settings.metrics,
                dimensions=ga_settings.dimensions,
                limit=ga_settings.limit,
                filters=[
                    WorkerGoogleAnalyticsFilter(
                        field=f.field,
                        operator=f.operator,
                        value=f.value,
                        case_sensitive=f.case_sensitive,
                    )
                    for f in ga_settings.filters
                ],
                order_bys=[
                    WorkerGoogleAnalyticsOrderBy(field=ob.field, descending=ob.descending)
                    for ob in ga_settings.order_bys
                ],
                flowfile_flow_id=node_ga_reader.flow_id,
                flowfile_node_id=node_ga_reader.node_id,
            )

            if auth_method == "service_account":
                return WorkerGoogleAnalyticsReadSettings(
                    auth_method="service_account",
                    service_account_key_encrypted=encrypted_credential,
                    **common_kwargs,
                )
            # ``oauth_cfg`` is only fetched for the oauth auth method; guard against
            # an unknown auth_method value reaching this branch with ``None``.
            if not oauth_cfg or not oauth_cfg["client_id"] or not oauth_cfg["client_secret"]:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Google OAuth is not configured on this instance. Open Admin → Google OAuth "
                        "and paste your OAuth client credentials before running this flow."
                    ),
                )
            return WorkerGoogleAnalyticsReadSettings(
                auth_method="oauth",
                refresh_token_encrypted=encrypted_credential,
                oauth_client_id=oauth_cfg["client_id"],
                oauth_client_secret_encrypted=_encrypt_with_master_key(oauth_cfg["client_secret"]),
                **common_kwargs,
            )

        # Stamp the predicted schema onto the setting object now, so downstream
        # nodes can introspect columns without ever invoking ``_func`` (which
        # would trigger a worker → Google round-trip). ``derive_schema`` is
        # pure-Python and runs against the chosen metrics/dimensions only — no DB,
        # so it stays eager and keeps flow-open connection-free.
        predicted_columns = derive_schema(metrics=ga_settings.metrics, dimensions=ga_settings.dimensions)
        node_ga_reader.fields = [c.get_minimal_field_info() for c in predicted_columns]

        def _func() -> FlowDataEngine:
            fetcher = root().ExternalGoogleAnalyticsFetcher(_build_worker_settings(), wait_on_completion=False)
            node._fetch_cached_df = fetcher
            # ``get_result()`` returns a ``pl.LazyFrame`` deserialised from the
            # worker's Arrow IPC file — never collect on the core service.
            fl = FlowDataEngine(fetcher.get_result())
            # Align to the predicted schema so downstream nodes see stable columns
            # even when the report is empty. ``align_to_schema`` lowers to lazy
            # ``with_columns``/``select`` calls, so this stays lazy.
            return fl.align_to_schema(schema_callback())

        def schema_callback() -> list[FlowfileColumn]:
            # Prefer the cached placeholder so repeated schema lookups don't
            # re-walk the heuristic table. ``derive_schema`` is the fallback
            # for the (rare) case where ``fields`` got cleared.
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

        All network I/O (HTTP round-trips, pagination, retries) is offloaded to
        the worker via ``ExternalRestApiFetcher`` — the core never makes the
        external call. The credential is resolved to an encrypted token here
        (from the user's secret store, or an inline plaintext) and the worker
        decrypts it just-in-time. A generic API's columns are unknown until a
        response is fetched, so ``schema_callback`` returns the columns cached on
        the node by the "Fetch sample" action — empty until the user samples or
        runs, in which case the fetched frame defines the schema.
        """
        logger.info("Adding rest api reader")
        node_type = "rest_api_reader"
        auth = node_rest_api_reader.rest_api_settings.auth

        # Encrypt any *inline* plaintext credential eagerly and null it out so it is
        # never persisted on the node (a security guarantee, independent of who owns
        # the flow). The *by-name* secret-store lookup is deferred to run time so
        # opening/undoing a flow never requires the current session to own the
        # secret — it resolves under the node's ``user_id`` (the flow owner).
        _inline_encrypted = _encrypt_with_master_key(auth.secret) if (auth.secret and not auth.secret_name) else None
        auth.secret = None

        def _resolve_secret_encrypted() -> str | None:
            if _inline_encrypted is not None:
                return _inline_encrypted
            return resolve_auth_secret_encrypted(auth, node_rest_api_reader.user_id)

        def _func() -> FlowDataEngine:
            encrypted = _resolve_secret_encrypted()
            worker_settings = build_rest_api_worker_settings(node_rest_api_reader, encrypted)
            if self.execution_location == "local":
                # No worker service in local runs — fetch in-process (cf. add_database_reader).
                from shared.rest_api.fetch import fetch_rest_api

                secret = decrypt_secret(encrypted).get_secret_value() if encrypted else None
                fl = FlowDataEngine(fetch_rest_api(worker_settings, secret=secret).lazy())
            else:
                fetcher = root().ExternalRestApiFetcher(worker_settings, wait_on_completion=False)
                node._fetch_cached_df = fetcher
                fl = FlowDataEngine(fetcher.get_result())
            cols = schema_callback()
            # Align to the sampled schema (if any) so downstream nodes see stable
            # columns; with no sample yet, the fetched frame defines the schema.
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
