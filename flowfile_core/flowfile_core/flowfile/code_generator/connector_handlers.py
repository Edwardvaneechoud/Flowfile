import typing

from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.schemas import input_schema


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
            self._add_code(f"{var_name} = ff.read_database(")
            self._add_code(f'    "{connection_name}",')
            self._add_code('    query="""')
            for line in db_settings.query.split("\n"):
                self._add_code(f"        {line}")
            self._add_code('    """,')
            self._add_code(f"){suffix}")
        else:
            self._add_code(f"{var_name} = ff.read_database(")
            self._add_code(f'    "{connection_name}",')
            if db_settings.table_name:
                self._add_code(f'    table_name="{db_settings.table_name}",')
            if db_settings.schema_name:
                self._add_code(f'    schema_name="{db_settings.schema_name}",')
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
        self._add_code(f'    "{connection_name}",')
        self._add_code(f'    "{db_settings.table_name}",')
        if db_settings.schema_name:
            self._add_code(f'    schema_name="{db_settings.schema_name}",')
        if db_settings.if_exists:
            self._add_code(f'    if_exists="{db_settings.if_exists}",')
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

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
            self._add_code(f"    headers={s.headers!r},")
        if s.query_params:
            self._add_code(f"    params={s.query_params!r},")
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

        if not table_name and not table_id:
            self.unsupported_nodes.append(
                (settings.node_id, "catalog_reader", "Catalog Reader node has no table name or ID configured")
            )
            return

        label = table_name or f"id={table_id}"
        suffix = ".data" if self.framework == "pl" else ""
        self._add_code(f"# Read from catalog table: {label}")
        self._add_code(f"{var_name} = ff.read_catalog_table(")
        if table_name:
            self._add_code(f'    "{table_name}",')
        if settings.catalog_namespace_id is not None:
            self._add_code(f"    namespace_id={settings.catalog_namespace_id},")
        if settings.delta_version is not None:
            self._add_code(f"    delta_version={settings.delta_version},")
        self._add_code(f"){suffix}")
        self._add_code("")

    def _handle_catalog_sql_reader(self, settings: input_schema.NodeCatalogReader, var_name: str) -> None:
        sql_code = settings.sql_query.replace('"""', '\\"\\"\\"')
        self._add_code("# SQL query against catalog tables")
        self._add_code(f'{var_name} = ff.read_catalog_sql("""')
        for line in sql_code.split("\n"):
            self._add_code(line)
        self._add_code('""")')
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

        self._add_code(f"# Write to catalog table: {ws.table_name}")
        self._add_code("ff.write_catalog_table(")
        self._add_code(f"    {input_df},")
        self._add_code(f'    "{ws.table_name}",')
        if ws.namespace_id is not None:
            self._add_code(f"    namespace_id={ws.namespace_id},")
        self._add_code(f'    write_mode="{ws.write_mode}",')
        if ws.merge_keys:
            self._add_code(f"    merge_keys={ws.merge_keys},")
        if ws.description:
            self._add_code(f'    description="{ws.description}",')
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}")
        self._add_code("")
