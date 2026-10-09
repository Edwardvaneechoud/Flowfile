"""Nodes that run user code: Polars code, SQL query and the kernel-backed Python script."""

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
    execute_polars_code,
    execute_sql_query,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    validate_sql_query,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)


class CodeBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_polars_code(self, node_polars_code: input_schema.NodePolarsCode):
        """Adds a node that executes custom Polars code.

        Args:
            node_polars_code: The settings for the Polars code node.
        """

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            return execute_polars_code(*flowfile_tables, code=node_polars_code.polars_code_input.polars_code)

        self.add_node_step(
            node_id=node_polars_code.node_id,
            function=_func,
            node_type="polars_code",
            setting_input=node_polars_code,
            input_node_ids=node_polars_code.depending_on_ids,
        )

        try:
            polars_code_parser.validate_code(node_polars_code.polars_code_input.polars_code)
        except Exception as e:
            node = self.get_node(node_id=node_polars_code.node_id)
            node.results.errors = str(e)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sql_query(self, node_sql_query: input_schema.NodeSqlQuery):
        """Adds a node that executes a SQL query against connected data sources.

        Args:
            node_sql_query: The settings for the SQL query node.
        """

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            return execute_sql_query(*flowfile_tables, sql_code=node_sql_query.sql_query_input.sql_code)

        self.add_node_step(
            node_id=node_sql_query.node_id,
            function=_func,
            node_type="sql_query",
            setting_input=node_sql_query,
            input_node_ids=node_sql_query.depending_on_ids,
        )

        node = self.get_node(node_id=node_sql_query.node_id)

        def schema_callback() -> list[FlowfileColumn]:
            # Resolve the schema by planning over 0-row upstream frames; nothing is collected.
            inputs = [
                v.get_predicted_resulting_data(src_handle) if v is not None else FlowDataEngine()
                for v, src_handle in node._slot_input_pairs()
            ]
            return execute_sql_query(*inputs, sql_code=node_sql_query.sql_query_input.sql_code).schema

        node.schema_callback = schema_callback

        try:
            validate_sql_query(node_sql_query.sql_query_input.sql_code)
        except Exception as e:
            node.results.errors = str(e)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_python_script(self, node_python_script: input_schema.NodePythonScript):
        """Adds a node that executes Python code on a kernel container."""

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            kernel_id = node_python_script.python_script_input.kernel_id
            if not kernel_id:
                raise ValueError("No kernel selected for python_script node")
            result = self._execute_on_kernel(
                node_id=node_python_script.node_id,
                kernel_id=kernel_id,
                code=node_python_script.python_script_input.code,
                output_names=node_python_script.output_names,
                flow_data_engine=flowfile_tables,
            )
            return result or (flowfile_tables[0] if flowfile_tables else FlowDataEngine(pl.LazyFrame()))

        def schema_callback():
            """Best-effort schema prediction for python_script nodes.

            Declared ``output_schemas`` win; an output without a declaration (or a node
            without any) predicts the first input's schema, since most python_script
            nodes transform and pass through. If nothing is available, returns [] —
            never raises.
            """
            try:
                node = self.get_node(node_python_script.node_id)
                if node is None:
                    return []

                main_inputs = node.node_inputs.main_inputs
                input_schema_: list[FlowfileColumn] = (main_inputs[0].schema or []) if main_inputs else []
                declared = node_python_script.output_schemas
                if not declared:
                    return input_schema_
                named = {
                    output_handle(i): (
                        [FlowfileColumn.from_input(f.name, f.data_type) for f in declared[name]]
                        if name in declared
                        else list(input_schema_)
                    )
                    for i, name in enumerate(node_python_script.output_names)
                }
                node._named_schemas = named
                return named.get(DEFAULT_OUTPUT_HANDLE, [])
            except Exception:
                return []

        previous = self.get_node(node_python_script.node_id)
        previous_declared = (
            previous.setting_input.output_schemas
            if previous is not None and isinstance(previous.setting_input, input_schema.NodePythonScript)
            else None
        )

        self.add_node_step(
            node_id=node_python_script.node_id,
            function=_func,
            node_type="python_script",
            setting_input=node_python_script,
            input_node_ids=node_python_script.depending_on_ids,
            schema_callback=schema_callback,
        )

        node = self.get_node(node_python_script.node_id)
        if node is not None:
            node._executes_on_kernel = bool(node_python_script.python_script_input.kernel_id)
            if previous is not None and previous_declared != node_python_script.output_schemas:
                node.refresh_predicted_schema()
        output_names = node_python_script.output_names
        if len(output_names) > 1:
            if node is not None:
                node.node_template = node.node_template.model_copy(update={"output": len(output_names)})
