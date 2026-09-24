import typing

from sqlalchemy.exc import SQLAlchemyError

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.code_generator.param_codegen import SENTINEL_PREFIX
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.change_feed import ChangeFeedReadSettings


class ConnectorHandlersMixin(ConverterMixinBase):
    """External connector handlers (cloud storage, kafka, database, REST API, catalog readers/writers)."""

    def _handle_external_source(
        self, settings: input_schema.NodeExternalSource, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle external_source nodes - these are not supported for code generation."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "external_source",
                "External Source nodes use dynamic data sources that cannot be included in generated code",
            )
        )
        self._add_comment(f"# Node {settings.node_id}: External Source - Not supported for code export")
        self._add_comment("# (External data sources require runtime configuration)")

    def _handle_cloud_storage_reader(
        self, settings: input_schema.NodeCloudStorageReader, var_name: str, input_vars: dict[str, str]
    ):
        """Cloud storage nodes are not supported for standalone Polars code. Use FlowFrame export."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "cloud_storage_reader",
                "Cloud Storage Reader is not supported by Polars code generation. "
                "Please use FlowFrame code generation instead.",
            )
        )

    def _handle_cloud_storage_writer(
        self, settings: input_schema.NodeCloudStorageWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Cloud storage nodes are not supported for standalone Polars code. Use FlowFrame export."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "cloud_storage_writer",
                "Cloud Storage Writer is not supported by Polars code generation. "
                "Please use FlowFrame code generation instead.",
            )
        )

    def _handle_kafka_source(
        self, settings: input_schema.NodeKafkaSource, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Kafka source nodes are not supported for standalone Polars code. Use FlowFrame export."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "kafka_source",
                "Kafka Source is not supported by Polars code generation. "
                "Please use FlowFrame code generation instead.",
            )
        )

    def _handle_list_files(
        self, settings: input_schema.NodeListFiles, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit ``ff.list_files``, like the other Flowfile-native sources.

        There is no Polars equivalent for a directory listing, so the Polars dialect
        borrows the same call and unwraps the FlowFrame — exactly what the database
        and catalog readers do.
        """
        if not settings.path:
            self.unsupported_nodes.append((settings.node_id, "list_files", "List Files node has no folder selected"))
            return

        self.imports.add("import flowfile as ff")
        suffix = ".data" if self.framework == "pl" else ""

        self._add_code(f"{var_name} = ff.list_files(")
        self._add_code(f"    {self._py_str(settings.path)},")
        if settings.file_types:
            self._add_code(f"    file_types={settings.file_types!r},")
        if settings.recursive:
            self._add_code("    recursive=True,")
            if settings.max_depth != 5:
                self._add_code(f"    max_depth={settings.max_depth},")
        if settings.include_hidden:
            self._add_code("    include_hidden=True,")
        if not settings.include_files:
            self._add_code("    include_files=False,")
        if settings.include_directories:
            self._add_code("    include_directories=True,")
        if settings.max_files is not None:
            self._add_code(f"    max_files={settings.max_files},")
        self._add_code(f"){suffix}")
        self._add_code("")

    def _handle_database_reader(
        self, settings: input_schema.NodeDatabaseReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("import flowfile as ff")
        db_settings = settings.database_settings

        if db_settings.connection_mode != "reference":
            self.unsupported_nodes.append(
                (
                    settings.node_id,
                    "database_reader",
                    "Database Reader nodes with inline connections cannot be exported. "
                    "Please use a named connection (reference mode) instead.",
                )
            )
            return

        if not db_settings.database_connection_name:
            self.unsupported_nodes.append(
                (settings.node_id, "database_reader", "Database Reader node is missing a connection name")
            )
            return

        connection_name = db_settings.database_connection_name
        suffix = ".data" if self.framework == "pl" else ""

        if db_settings.query_mode == "query" and db_settings.query:
            query = db_settings.query.replace('"""', '\\"\\"\\"')
            self._add_code(f"{var_name} = ff.read_database(")
            self._add_code(f"    {self._py_str(connection_name)},")
            self._add_code('    query="""')
            for line in query.split("\n"):
                self._add_code(f"        {line}")
            self._add_code('    """,')
            self._add_code(f"){suffix}")
        else:
            self._add_code(f"{var_name} = ff.read_database(")
            self._add_code(f"    {self._py_str(connection_name)},")
            if db_settings.table_name:
                self._add_code(f"    table_name={self._py_str(db_settings.table_name)},")
            if db_settings.schema_name:
                self._add_code(f"    schema_name={self._py_str(db_settings.schema_name)},")
            self._add_code(f"){suffix}")

        self._add_code("")

    def _handle_database_writer(
        self, settings: input_schema.NodeDatabaseWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("import flowfile as ff")
        db_settings = settings.database_write_settings

        if db_settings.connection_mode != "reference":
            self.unsupported_nodes.append(
                (
                    settings.node_id,
                    "database_writer",
                    "Database Writer nodes with inline connections cannot be exported. "
                    "Please use a named connection (reference mode) instead.",
                )
            )
            return

        if not db_settings.database_connection_name:
            self.unsupported_nodes.append(
                (settings.node_id, "database_writer", "Database Writer node is missing a connection name")
            )
            return

        connection_name = db_settings.database_connection_name
        input_df = input_vars.get("main", "df")

        self._add_code("ff.write_database(")
        self._add_code(f"    {input_df},")
        self._add_code(f"    {self._py_str(connection_name)},")
        self._add_code(f"    {self._py_str(db_settings.table_name)},")
        if db_settings.schema_name:
            self._add_code(f"    schema_name={self._py_str(db_settings.schema_name)},")
        if db_settings.if_exists:
            self._add_code(f"    if_exists={self._py_str(db_settings.if_exists)},")
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

    _SENSITIVE_KEYS = frozenset(
        {
            "authorization",
            "x-api-key",
            "api-key",
            "apikey",
            "api_key",
            "token",
            "access_token",
            "refresh_token",
            "cookie",
            "password",
            "secret",
        }
    )

    @classmethod
    def _redact_sensitive(cls, mapping: dict) -> dict:
        """Mask values of well-known credential keys so a token placed directly in
        a header/param never lands verbatim in generated code."""
        placeholder = "<redacted: provide via env/secret>"
        return {
            key: (placeholder if isinstance(key, str) and key.lower() in cls._SENSITIVE_KEYS else val)
            for key, val in mapping.items()
        }

    def _handle_rest_api_reader(
        self, settings: input_schema.NodeRestApiReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("import flowfile as ff")
        s = settings.rest_api_settings
        suffix = ".data" if self.framework == "pl" else ""

        self._add_code(f"# Read from REST API: {s.method} {s.url}")
        self._add_code(f"{var_name} = ff.read_api(")
        self._add_code(f"    {s.url!r},")
        if s.method != "GET":
            self._add_code(f'    method="{s.method}",')
        if s.headers:
            self._add_code(f"    headers={self._redact_sensitive(s.headers)!r},")
        if s.query_params:
            self._add_code(f"    params={self._redact_sensitive(s.query_params)!r},")
        if s.json_body is not None:
            self._add_code(f"    json_body={s.json_body!r},")
        auth_arg = self._build_rest_api_auth_arg(s.auth)
        if auth_arg:
            self._add_code(f"    auth={auth_arg},")
        pagination_arg = self._build_rest_api_pagination_arg(s.pagination)
        if pagination_arg:
            self._add_code(f"    pagination={pagination_arg},")
        if s.record_path:
            self._add_code(f"    record_path={s.record_path!r},")
        if s.timeout_seconds != 30.0:
            self._add_code(f"    timeout_seconds={s.timeout_seconds},")
        if s.max_retries != 3:
            self._add_code(f"    max_retries={s.max_retries},")
        self._add_code(f"){suffix}")
        self._add_code("")

    @staticmethod
    def _build_rest_api_auth_arg(auth: input_schema.RestApiAuthSettings | None) -> str | None:
        """Build the ``auth=`` dict literal for ``read_api``, or None when no auth.

        The inline plaintext ``secret`` is never emitted: it is not persisted and
        would leak a credential into the generated script. Code references the
        stored ``secret_name`` instead, mirroring the database reader's reliance
        on a named connection.
        """
        if auth is None or auth.auth_type == "none":
            return None
        auth_dict: dict[str, typing.Any] = {"auth_type": auth.auth_type}
        if auth.auth_type == "api_key":
            if auth.api_key_name != "X-API-Key":
                auth_dict["api_key_name"] = auth.api_key_name
            if auth.api_key_location != "header":
                auth_dict["api_key_location"] = auth.api_key_location
        elif auth.auth_type == "basic" and auth.basic_username:
            auth_dict["basic_username"] = auth.basic_username
        if auth.secret_name:
            auth_dict["secret_name"] = auth.secret_name
        return repr(auth_dict)

    @staticmethod
    def _build_rest_api_pagination_arg(
        pagination: input_schema.RestApiPaginationSettings | None,
    ) -> str | None:
        """Build the ``pagination=`` dict literal for ``read_api``, or None when unpaginated."""
        if pagination is None or pagination.pagination_type == "none":
            return None
        p: dict[str, typing.Any] = {"pagination_type": pagination.pagination_type}
        if pagination.pagination_type == "offset":
            if pagination.offset_param != "offset":
                p["offset_param"] = pagination.offset_param
            if pagination.limit_param != "limit":
                p["limit_param"] = pagination.limit_param
            if pagination.page_size != 100:
                p["page_size"] = pagination.page_size
        elif pagination.pagination_type == "page":
            if pagination.page_param != "page":
                p["page_param"] = pagination.page_param
            if pagination.start_page != 1:
                p["start_page"] = pagination.start_page
            if pagination.page_size != 100:
                p["page_size"] = pagination.page_size
        elif pagination.pagination_type == "cursor":
            if pagination.cursor_param != "cursor":
                p["cursor_param"] = pagination.cursor_param
            if pagination.cursor_location != "body":
                p["cursor_location"] = pagination.cursor_location
            if pagination.cursor_response_path:
                p["cursor_response_path"] = pagination.cursor_response_path
            if pagination.initial_cursor:
                p["initial_cursor"] = pagination.initial_cursor
        if pagination.max_pages != 1000:
            p["max_pages"] = pagination.max_pages
        if pagination.max_records is not None:
            p["max_records"] = pagination.max_records
        if pagination.page_delay_seconds:
            p["page_delay_seconds"] = pagination.page_delay_seconds
        return repr(p)

    def _handle_catalog_reader(
        self, settings: input_schema.NodeCatalogReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("import flowfile as ff")

        if settings.sql_query:
            self._handle_catalog_sql_reader(settings, var_name)
            return

        table_name = settings.catalog_table_name
        table_id = settings.catalog_table_id

        if not table_name:
            reason = (
                "Catalog Reader configured by table ID only; code export requires a table name"
                if table_id
                else "Catalog Reader node has no table name or ID configured"
            )
            self.unsupported_nodes.append((settings.node_id, "catalog_reader", reason))
            return

        suffix = ".data" if self.framework == "pl" else ""
        self._add_code(f"# Read from catalog table: {table_name}")
        self._add_code(f"{var_name} = ff.read_catalog_table(")
        self._add_code(f"    {self._py_str(table_name)},")
        self._emit_catalog_namespace(None, settings.catalog_namespace_id)
        if settings.delta_version is not None:
            self._add_code(f"    delta_version={settings.delta_version},")
        if settings.scd2_view is not None:
            self._add_code(f"    scd2_view={self._py_str(settings.scd2_view)},")
        if settings.scd2_as_of is not None:
            self._add_code(f"    scd2_as_of={self._py_str(settings.scd2_as_of)},")
        self._emit_change_feed_kwargs(settings, settings.cdc_consumer_name, settings.cdc_start)
        self._add_code(f"){suffix}")
        self._add_code("")

    def _emit_change_feed_kwargs(
        self, feed: ChangeFeedReadSettings, consumer_name: str | None = None, start: str = "now"
    ) -> None:
        """Emit the change-feed kwargs shared by ``ff.read_catalog_table`` and ``ff.read_from_cloud_storage``.

        ``consumer_name`` and ``start`` are cursor settings only the catalog reader has; the cloud
        reader has no cursor store and leaves them at their defaults, which emit nothing.
        """
        if feed.cdc_mode == "since_last_run":
            self._add_code('    changes_since="last_run",')
        elif feed.cdc_mode == "since_version":
            version = feed.cdc_from_version
            # A ${param} ref arrives as a bare sentinel name; the post-pass rewrites it to the kwarg.
            bare = isinstance(version, int) or str(version).startswith(SENTINEL_PREFIX)
            self._add_code(f"    changes_since={version if bare else self._py_str(str(version))},")
        elif feed.cdc_mode == "since_timestamp":
            self._add_code(f"    changes_since={self._py_str(feed.cdc_from_timestamp)},")
        if feed.cdc_mode == "off":
            return
        if consumer_name:
            self._add_code(f"    changes_consumer={self._py_str(consumer_name)},")
        if start != "now":
            self._add_code(f"    changes_start={self._py_str(start)},")
        if feed.cdc_include_preimage:
            self._add_code("    include_change_preimage=True,")

    def _emit_catalog_namespace(self, full_name: str | None, namespace_id: int | None) -> None:
        """Emit the catalog target as a portable ``namespace_full_name="catalog.schema"`` kwarg.

        The numeric id is install-local and meaningless to a reader of the script, so it is only
        emitted when no name is stored and the id no longer resolves in this catalog.
        """
        full_name = full_name or self._resolve_catalog_namespace_full_name(namespace_id)
        if full_name:
            self._add_code(f"    namespace_full_name={self._py_str(full_name)},")
        elif namespace_id is not None:
            self._add_code(f"    namespace_id={namespace_id},")

    @staticmethod
    def _resolve_catalog_namespace_full_name(namespace_id: int | None) -> str | None:
        if namespace_id is None:
            return None
        try:
            with get_db_context() as db:
                return CatalogService(SQLAlchemyCatalogRepository(db)).resolve_namespace_full_name(namespace_id)
        except SQLAlchemyError:
            return None

    def _handle_catalog_sql_reader(self, settings: input_schema.NodeCatalogReader, var_name: str) -> None:
        sql_code = settings.sql_query.replace('"""', '\\"\\"\\"')
        suffix = ".data" if self.framework == "pl" else ""
        self._add_code("# SQL query against catalog tables")
        self._add_code(f'{var_name} = ff.read_catalog_sql("""')
        for line in sql_code.split("\n"):
            self._add_code(line)
        self._add_code(f'"""){suffix}')
        self._add_code("")

    def _handle_catalog_writer(
        self, settings: input_schema.NodeCatalogWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("import flowfile as ff")
        ws = settings.catalog_write_settings
        input_df = input_vars.get("main", "df")

        if not ws.table_name:
            self.unsupported_nodes.append(
                (settings.node_id, "catalog_writer", "Catalog Writer node has no table name configured")
            )
            return

        # An SCD2 write is the one mode whose output differs from its input (the generated columns
        # ride along), so its call result is bound instead of the input being passed through.
        is_scd2 = ws.write_mode == "scd2"
        self._add_code(f"# Write to catalog table: {ws.table_name}")
        self._add_code(f"{var_name} = ff.write_catalog_table(" if is_scd2 else "ff.write_catalog_table(")
        self._add_code(f"    {input_df},")
        self._add_code(f"    {self._py_str(ws.table_name)},")
        self._emit_catalog_namespace(ws.namespace_full_name, ws.namespace_id)
        self._add_code(f"    write_mode={self._py_str(ws.write_mode)},")
        if ws.merge_keys:
            self._add_code(f"    merge_keys={ws.merge_keys},")
        if ws.partition_by:
            self._add_code(f"    partition_by={ws.partition_by},")
        if ws.track_changes:
            self._add_code("    track_changes=True,")
        if is_scd2 and ws.scd2 is not None:
            s = ws.scd2
            if s.output_mode != "input":
                self._add_code(f"    scd2_output_mode={self._py_str(s.output_mode)},")
            if s.compare_columns:
                self._add_code(f"    scd2_compare_columns={s.compare_columns},")
            if s.full_snapshot:
                self._add_code("    scd2_full_snapshot=True,")
            if s.surrogate_key_column != "sk":
                self._add_code(f"    scd2_surrogate_key_column={self._py_str(s.surrogate_key_column)},")
            if s.valid_from_column != "valid_from":
                self._add_code(f"    scd2_valid_from_column={self._py_str(s.valid_from_column)},")
            if s.valid_to_column != "valid_to":
                self._add_code(f"    scd2_valid_to_column={self._py_str(s.valid_to_column)},")
            if s.is_current_column != "is_current":
                self._add_code(f"    scd2_is_current_column={self._py_str(s.is_current_column)},")
            if not s.partition_on_current:
                self._add_code("    scd2_partition_on_current=False,")
        if ws.description:
            self._add_code(f"    description={self._py_str(ws.description)},")
        self._add_code(")")
        if not is_scd2:
            self._add_code(f"{var_name} = {input_df}")
        self._add_code("")
