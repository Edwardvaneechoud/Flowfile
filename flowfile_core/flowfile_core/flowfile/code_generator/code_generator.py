import builtins
import keyword
import re

import polars as pl
from polars_expr_transformer import PolarsCodeGenError, to_flowframe_code

from flowfile_core.configs import logger
from flowfile_core.flowfile.code_generator.chain_fusion import NodeEmission, render_pipeline
from flowfile_core.flowfile.code_generator.connector_handlers import ConnectorHandlersMixin
from flowfile_core.flowfile.code_generator.custom_node_handlers import CustomNodeHandlersMixin
from flowfile_core.flowfile.code_generator.expression_helpers import ExpressionHelpersMixin
from flowfile_core.flowfile.code_generator.join_handlers import JoinHandlersMixin
from flowfile_core.flowfile.code_generator.transform_handlers import TransformHandlersMixin
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.execution_orderer import determine_execution_order
from flowfile_core.schemas import input_schema, transform_schema


class UnsupportedNodeError(Exception):
    """Raised when code generation encounters a node type that cannot be converted to standalone code."""

    def __init__(self, node_type: str, node_id: int, reason: str):
        self.node_type = node_type
        self.node_id = node_id
        self.reason = reason
        super().__init__(f"Cannot generate code for node '{node_type}' (node_id={node_id}): {reason}")


def _try_translate_to_ff_code(formula: str) -> str | None:
    """Translate a flowfile formula into native ``ff.``-prefixed FlowFrame expression code.

    Returns the validated code string, or None so callers fall back to the
    legacy ``flowfile_formula(s)`` emission (which is always correct). Mirrors
    flowfile_frame.flow_frame._try_translate_flowfile_formulas: generate via
    polars_expr_transformer.to_flowframe_code, then validate by eval'ing in a
    restricted namespace and checking the result is a FlowFrame Expr.
    """
    try:
        generated = to_flowframe_code(formula)
    except PolarsCodeGenError:
        return None
    except Exception as e:
        logger.debug(f"to_flowframe_code failed for {formula!r}: {e}")
        return None
    if not generated:
        return None
    try:
        from flowfile_frame.expr import Expr as FlowFrameExpr
    except ImportError:
        logger.debug("flowfile package unavailable; cannot validate generated ff code")
        return None
    try:
        result = _eval_in_validation_namespace(generated)
    except Exception as e:
        logger.debug(f"Generated ff code failed validation for {formula!r}: {e}")
        return None
    return generated if isinstance(result, FlowFrameExpr) else None


def _eval_in_validation_namespace(code: str):
    """Eval generated ff code in the restricted namespace used for validation.

    Lazy imports: flowfile/flowfile_frame import flowfile_core, so module-level
    imports here would be circular. Raises on any failure. The namespace mirrors
    what generated snippets may reference — callers emitting a validated snippet
    must also emit the matching imports (see _translate_to_ff_code).
    """
    import datetime

    import flowfile as ff_module

    return eval(code, {"__builtins__": {}}, {"ff": ff_module, "pl": pl, "datetime": datetime})  # noqa: S307


# Operation-based variable labels for the few boundary variables that survive
# chain fusion. A bare label is used when it occurs once; a numeric suffix is
# appended only when a label is shared by several boundaries (see _plan_boundary_names).
NODE_TYPE_VAR_LABEL: dict[str, str] = {
    "read": "source",
    "csv_read": "source",
    "excel_read": "source",
    "manual_input": "source",
    "catalog_reader": "source",
    "catalog_sql_reader": "source",
    "cloud_storage_reader": "source",
    "database_reader": "source",
    "rest_api_reader": "source",
    "kafka_source": "source",
    "external_source": "source",
    "filter": "filtered",
    "formula": "computed",
    "select": "selected",
    "dynamic_rename": "renamed",
    "sort": "ordered",
    "group_by": "grouped",
    "pivot": "pivoted",
    "pivot_no_index": "pivoted",
    "unpivot": "unpivoted",
    "join": "joined",
    "cross_join": "joined",
    "fuzzy_match": "joined",
    "union": "combined",
    "unique": "deduped",
    "record_id": "with_record_id",
    "record_count": "counted",
    "sample": "sampled",
    "text_to_rows": "exploded",
    "polars_code": "transformed",
    "graph_solver": "solved",
    "window_functions": "windowed",
    "train_model": "trained",
    "apply_model": "scored",
    "evaluate_model": "evaluation",
    "wait_for": "ready",
}

# Generated variable names are uniquified against these so they never shadow a
# Python builtin or keyword (e.g. a split literally named ``sorted`` or ``type``).
_RESERVED_NAMES: frozenset[str] = frozenset(keyword.kwlist) | frozenset(dir(builtins))


class FlowGraphCodeConverter(
    JoinHandlersMixin,
    TransformHandlersMixin,
    ConnectorHandlersMixin,
    CustomNodeHandlersMixin,
    ExpressionHelpersMixin,
):
    """
    Base class for converting a FlowGraph into executable Python code.

    Node-type handlers live in the mixins above; this class owns orchestration
    (dispatch, chain fusion, boundary naming, final code assembly).

    Subclasses set `framework` to control whether code targets Polars or FlowFrame.
    """

    framework: str = "pl"

    def __init__(self, flow_graph: FlowGraph):
        self.flow_graph = flow_graph
        self.node_var_mapping: dict[int, str] = {}
        # (upstream_node_id, output_handle) -> variable name; populated by
        # multi-output handlers and consulted by _get_input_vars.
        self.node_handle_var_mapping: dict[tuple[int, str], str] = {}
        self.imports: set[str] = set()
        self.code_lines: list[str] = []
        self.output_nodes: list[tuple[int, str]] = []
        self.last_node_var: str | None = None
        self.unsupported_nodes: list[tuple[int, str, str]] = []
        self.custom_node_classes: dict[str, str] = {}
        # (node, effective_var, start, end) per emitting node; (start, end) slices code_lines.
        self._node_spans: list[tuple[FlowNode, str, int, int]] = []
        # node_id -> upstream node_id for nodes that emit nothing (passthroughs).
        self._passthrough: dict[int, int] = {}

    def convert(self) -> str:
        """
        Main method to convert the FlowGraph to Polars code.

        Returns:
            str: Complete Python code that can be executed standalone

        Raises:
            UnsupportedNodeError: If the graph contains nodes that cannot be converted
                to standalone code (e.g., database nodes, explore_data, external_source).
        """
        stages = determine_execution_order(
            all_nodes=[node for node in self.flow_graph.nodes if node.is_correct],
            flow_starts=self.flow_graph._flow_starts + self.flow_graph.get_implicit_starter_nodes(),
        )

        for node in (node for stage in stages for node in stage):
            self._generate_node_code(node)

        if self.unsupported_nodes:
            error_messages = []
            for node_id, node_type, reason in self.unsupported_nodes:
                error_messages.append(f"  - Node {node_id} ({node_type}): {reason}")
            raise UnsupportedNodeError(
                node_type=self.unsupported_nodes[0][1],
                node_id=self.unsupported_nodes[0][0],
                reason=(
                    f"The flow contains {len(self.unsupported_nodes)} node(s) that cannot be converted to code:\n"
                    + "\n".join(error_messages)
                ),
            )

        return self._build_final_code()

    def handle_output_node(self, node: FlowNode, var_name: str) -> None:
        settings = node.setting_input
        if hasattr(settings, "is_flow_output") and settings.is_flow_output:
            self.output_nodes.append((node.node_id, var_name))

    def _generate_node_code(self, node: FlowNode) -> None:
        """Generate code for a node and record the line span it emitted.

        Output bookkeeping runs *after* the handler and reads the effective var
        from ``node_var_mapping`` so passthrough/remap handlers are honoured. A
        handler that emits nothing is recorded as a passthrough to its input.
        """
        node_type = node.node_type
        settings = node.setting_input
        if isinstance(settings, input_schema.NodePromise):
            self._add_comment(f"# Skipping uninitialized node: {node.node_id}")
            return
        node_reference = getattr(settings, "node_reference", None)
        var_name = node_reference if node_reference else f"df_{node.node_id}"
        self.node_var_mapping[node.node_id] = var_name
        input_vars = self._get_input_vars(node)

        start = len(self.code_lines)
        if isinstance(settings, input_schema.UserDefinedNode) or getattr(settings, "is_user_defined", False):
            self._handle_user_defined(node, var_name, input_vars)
        else:
            handler = getattr(self, f"_handle_{node_type}", None)
            if handler:
                handler(settings, var_name, input_vars)
            else:
                self.unsupported_nodes.append(
                    (node.node_id, node_type, f"No code generator implemented for node type '{node_type}'")
                )
                self._add_comment(
                    f"# WARNING: Cannot generate code for node type '{node_type}' (node_id={node.node_id})"
                )
                self._add_comment("# This node type is not supported for code export")
        end = len(self.code_lines)

        effective_var = self.node_var_mapping[node.node_id]
        self.handle_output_node(node, effective_var)
        if node.node_template.output > 0:
            self.last_node_var = effective_var

        if end == start:
            main_inputs = node.node_inputs.main_inputs or []
            if len(main_inputs) == 1:
                self._passthrough[node.node_id] = main_inputs[0].node_id
        else:
            self._node_spans.append((node, effective_var, start, end))

    def _resolve_upstream_var(self, downstream: FlowNode, upstream_id: int, default: str) -> str:
        """Resolve the variable name for an upstream node, honouring its output handle.

        Multi-output upstream nodes register per-handle variable names in
        ``node_handle_var_mapping``; for single-output upstreams the legacy
        ``node_var_mapping`` is the single source of truth.
        """
        handle = downstream._input_output_handles.get(upstream_id, "output-0")
        per_handle = self.node_handle_var_mapping.get((upstream_id, handle))
        if per_handle is not None:
            return per_handle
        return self.node_var_mapping.get(upstream_id, default)

    def _get_input_vars(self, node: FlowNode) -> dict[str, str]:
        """Get input variable names for a node."""
        input_vars = {}

        if node.node_inputs.main_inputs:
            if len(node.node_inputs.main_inputs) == 1:
                input_vars["main"] = self._resolve_upstream_var(
                    node, node.node_inputs.main_inputs[0].node_id, "df"
                )
            else:
                for i, input_node in enumerate(node.node_inputs.main_inputs):
                    input_vars[f"main_{i}"] = self._resolve_upstream_var(node, input_node.node_id, f"df_{i}")

        if node.node_inputs.left_input:
            input_vars["left"] = self._resolve_upstream_var(
                node, node.node_inputs.left_input.node_id, "df_left"
            )

        if node.node_inputs.right_input:
            input_vars["right"] = self._resolve_upstream_var(
                node, node.node_inputs.right_input.node_id, "df_right"
            )

        return input_vars

    def _handle_csv_read(self, file_settings: input_schema.ReceivedTable, var_name: str):
        if file_settings.table_settings.encoding.lower() in ("utf-8", "utf8"):
            encoding = "utf8-lossy"
            self._add_code(f"{var_name} = {self.framework}.scan_csv(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
            self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
            self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
            self._add_code(f'    encoding="{encoding}",')
            self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
            self._add_code(")")
        else:
            self._handle_csv_read_non_utf8(file_settings, var_name)

    def _handle_csv_read_non_utf8(self, file_settings: input_schema.ReceivedTable, var_name: str):
        self._add_code(f"{var_name} = {self.framework}.read_csv(")
        self._add_code(f'    "{file_settings.abs_file_path}",')
        self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
        self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
        self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
        if file_settings.table_settings.encoding:
            self._add_code(f'    encoding="{file_settings.table_settings.encoding}",')
        self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
        self._add_code(").lazy()")

    def _handle_read(self, settings: input_schema.NodeRead, var_name: str, input_vars: dict[str, str]) -> None:
        file_settings = settings.received_file
        if file_settings.file_type == "csv":
            self._handle_csv_read(file_settings, var_name)
        elif file_settings.file_type == "parquet":
            self._add_code(f'{var_name} = {self.framework}.scan_parquet("{file_settings.abs_file_path}")')
        elif file_settings.file_type in ("xlsx", "excel"):
            self._handle_excel_read(file_settings, var_name)
        self._add_code("")

    def _handle_excel_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        self._add_code(f"{var_name} = {self.framework}.read_excel(")
        self._add_code(f'    "{file_settings.abs_file_path}",')
        if file_settings.table_settings.sheet_name:
            self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
        self._add_code(")")

    def _generate_pl_schema_with_typing(self, flowfile_schema: list[FlowfileColumn]) -> str:
        polars_schema_str = (
            f"{self.framework}.Schema(["
            + ", ".join(
                f'("{flowfile_column.column_name}", {self.framework}.{flowfile_column.data_type})'
                for flowfile_column in flowfile_schema
            )
            + "])"
        )
        return polars_schema_str

    def get_manual_schema_input(self, flowfile_schema: list[FlowfileColumn]) -> str:
        polars_schema_str = self._generate_pl_schema_with_typing(flowfile_schema)
        is_valid_pl_schema = self._validate_pl_schema(polars_schema_str)
        if is_valid_pl_schema:
            return polars_schema_str
        else:
            return "[" + ", ".join([f'"{c.name}"' for c in flowfile_schema]) + "]"

    @staticmethod
    def _validate_pl_schema(pl_schema_str: str) -> bool:
        try:
            _globals = {"pl": pl}
            eval(pl_schema_str, _globals)
            return True
        except Exception as e:
            logger.error(f"Invalid Polars schema: {e}")
            return False

    def _handle_manual_input(
        self, settings: input_schema.NodeManualInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle manual data input nodes."""
        data = settings.raw_data_format.data
        flowfile_schema = list(
            FlowfileColumn.create_from_minimal_field_info(c) for c in settings.raw_data_format.columns
        )
        schema = self.get_manual_schema_input(flowfile_schema)
        self._add_code(f"{var_name} = {self.framework}.LazyFrame({data}, schema={schema}, strict=False)")
        self._add_code("")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle filter nodes."""
        input_df = input_vars.get("main", "df")

        if settings.split_mode:
            self._handle_filter_split(settings, var_name, input_df)
            return

        if settings.filter_input.is_advanced():
            self.imports.add(
                "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
            )
            self._add_code(f"{var_name} = {input_df}.filter(")
            self._add_code(f'simple_function_to_expr("{settings.filter_input.advanced_filter}")')
            self._add_code(")")
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                filter_expr = self._create_basic_filter_expr(basic)
                self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _handle_filter_split(self, settings: input_schema.NodeFilter, var_name: str, input_df: str) -> None:
        """Emit a pass/fail split filter (output-0 = pass, output-1 = fail).

        Mirrors FlowDataEngine.filter_split: ``df.filter(pred)`` for pass,
        ``df.filter(~pred)`` for fail. Rows where the predicate is null are
        dropped from both (polars filter semantics).
        """
        node_id = settings.node_id
        pred_var = f"_filter_{node_id}_pred"
        if settings.filter_input.is_advanced():
            self.imports.add(
                "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
            )
            self._add_code(f'{pred_var} = simple_function_to_expr("{settings.filter_input.advanced_filter}")')
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                self._add_code(f"{pred_var} = {self._create_basic_filter_expr(basic)}")
            else:
                # No predicate -> pass keeps everything, fail is empty.
                self._add_code(f"{pred_var} = {self.framework}.lit(True)")
        pass_var = f"{var_name}_pass"
        fail_var = f"{var_name}_fail"
        self._add_code(f"{pass_var} = {input_df}.filter({pred_var})")
        self._add_code(f"{fail_var} = {input_df}.filter(~({pred_var}))")
        self.node_handle_var_mapping[(node_id, "output-0")] = pass_var
        self.node_handle_var_mapping[(node_id, "output-1")] = fail_var
        self.node_var_mapping[node_id] = pass_var
        self._add_code("")

    def _handle_record_count(self, settings: input_schema.NodeRecordCount, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.select({self.framework}.len().alias('number_of_records'))")

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        from_col_name = settings.graph_solver_input.col_from
        to_col_name = settings.graph_solver_input.col_to
        output_col_name = settings.graph_solver_input.output_column_name
        self._add_code(
            f'{var_name} = {input_df}.with_columns(graph_solver({self.framework}.col("{from_col_name}"), '
            f'{self.framework}.col("{to_col_name}"))'
            f'.alias("{output_col_name}"))'
        )
        self._add_code("")
        self.imports.add("from polars_grouper import graph_solver")

    def _handle_select(self, settings: input_schema.NodeSelect, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle select/rename nodes."""
        input_df = input_vars.get("main", "df")
        select_exprs = []
        for select_input in settings.select_input:
            if select_input.keep and select_input.is_available:
                if select_input.old_name != select_input.new_name:
                    expr = f'{self.framework}.col("{select_input.old_name}").alias("{select_input.new_name}")'
                else:
                    expr = f'{self.framework}.col("{select_input.old_name}")'

                if (select_input.data_type_change or select_input.is_altered) and select_input.data_type:
                    polars_dtype = self._get_polars_dtype(select_input.data_type)
                    expr = f"{expr}.cast({polars_dtype})"

                select_exprs.append(expr)

        if select_exprs:
            self._add_code(f"{var_name} = {input_df}.select([")
            for expr in select_exprs:
                self._add_code(f"    {expr},")
            self._add_code("])")
            self._add_code("")
        else:
            # Nothing selected -> transparent passthrough; remap and emit nothing.
            self.node_var_mapping[settings.node_id] = input_df

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: dict[str, str]) -> None:
        input_df = input_vars.get("main", "df")
        output_settings = settings.output_settings

        if output_settings.file_type == "csv":
            self._add_code(f"{input_df}.sink_csv(")
            self._add_code(f'    "{output_settings.abs_file_path}",')
            self._add_code(f'    separator="{output_settings.table_settings.delimiter}"')
            self._add_code(")")
        elif output_settings.file_type == "parquet":
            self._add_code(f'{input_df}.sink_parquet("{output_settings.abs_file_path}")')
        elif output_settings.file_type == "excel":
            self._handle_output_excel(input_df, output_settings)

        self._add_code("")

    def _handle_output_excel(self, input_df: str, output_settings) -> None:
        self._add_code(f"{input_df}.write_excel(")
        self._add_code(f'    "{output_settings.abs_file_path}",')
        self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}"')
        self._add_code(")")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle custom Polars code nodes."""
        # TODO(FlowFrame): When framework == "ff", this generates `ff.LazyFrame` in the
        # function signature, but flowfile doesn't export LazyFrame. User-written polars code
        # also uses pl.col, pl.LazyFrame directly. Options:
        # (a) Always use pl.LazyFrame in signatures (polars_code is inherently polars),
        # (b) Override _handle_polars_code in FlowFrameConverter to add `import polars as pl`,
        # (c) Add LazyFrame export to the flowfile package.
        code = settings.polars_code_input.polars_code.strip()
        if len(input_vars) == 0:
            params = ""
            args = ""
        elif len(input_vars) == 1:
            params = f"input_df: {self.framework}.LazyFrame"
            input_df = list(input_vars.values())[0]
            args = input_df
        else:
            param_list = []
            arg_list = []
            i = 1
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    param_list.append(f"input_df_{i}: {self.framework}.LazyFrame")
                    arg_list.append(input_vars[key])
                    i += 1
            params = ", ".join(param_list)
            args = ", ".join(arg_list)

        is_expression = "output_df" not in code

        self._add_code("# Custom Polars code")
        self._add_code(f"def _polars_code_{settings.node_id}({params}):")

        if is_expression:
            self._add_code(f"    return {code}")
        else:
            for line in code.split("\n"):
                if line.strip():
                    self._add_code(f"    {line}")

            if "return" not in code:
                lines = [line.strip() for line in code.split("\n") if line.strip() and "=" in line]
                if lines:
                    last_assignment = lines[-1]
                    if "=" in last_assignment:
                        output_var = last_assignment.split("=")[0].strip()
                        self._add_code(f"    return {output_var}")

        self._add_code("")

        self._add_code(f"{var_name} = _polars_code_{settings.node_id}({args})")
        self._add_code("")

    # Handlers for unsupported node types - these add nodes to the unsupported list

    def _handle_explore_data(
        self, settings: input_schema.NodeExploreData, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Elide explore_data (interactive-only): remap to its input and emit nothing.

        Downstream references and the return resolve straight to the upstream
        frame, so no dead ``var = input`` passthrough appears in the script —
        matching ``--run-flow``, which already drops UI-only explore_data nodes.
        """
        self.node_var_mapping[settings.node_id] = input_vars.get("main", "df")

    # Helper methods

    def _add_code(self, line: str) -> None:
        """Add a line of code."""
        self.code_lines.append(line)

    def _add_comment(self, comment: str) -> None:
        """Add a comment line."""
        self.code_lines.append(comment)

    def _raw_producer_ids(self, node: FlowNode) -> list[int]:
        """Upstream node ids feeding ``node`` (main + left + right)."""
        ids = [inp.node_id for inp in (node.node_inputs.main_inputs or [])]
        if node.node_inputs.left_input:
            ids.append(node.node_inputs.left_input.node_id)
        if node.node_inputs.right_input:
            ids.append(node.node_inputs.right_input.node_id)
        return ids

    def _resolve_producer(self, node_id: int) -> int:
        """Hop through elided passthrough nodes to the real producing node."""
        seen: set[int] = set()
        while node_id in self._passthrough and node_id not in seen:
            seen.add(node_id)
            node_id = self._passthrough[node_id]
        return node_id

    @staticmethod
    def _is_flow_output(node: FlowNode) -> bool:
        return bool(getattr(node.setting_input, "is_flow_output", False))

    @staticmethod
    def _is_filter_split(node: FlowNode) -> bool:
        return node.node_type == "filter" and bool(getattr(node.setting_input, "split_mode", False))

    def _var_label(self, node: FlowNode) -> str:
        if self._is_filter_split(node):
            return "split"
        return NODE_TYPE_VAR_LABEL.get(node.node_type, "df")

    def _render_body(self) -> list[str]:
        """Fuse linear single-use chains and give the surviving boundaries clean names."""
        emissions: list[NodeEmission] = []
        node_by_id: dict[int, FlowNode] = {}
        for node, effective_var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and lines[-1] == "":
                lines = lines[:-1]
            if not lines:
                continue
            resolved = {self._resolve_producer(pid) for pid in self._raw_producer_ids(node)}
            num_inputs = len(resolved)
            main_producer_id = next(iter(resolved)) if num_inputs == 1 else None
            pinned = bool(getattr(node.setting_input, "node_reference", None))
            emissions.append(
                NodeEmission(
                    node.node_id, effective_var, lines, main_producer_id,
                    num_inputs, self._is_flow_output(node), pinned,
                )
            )
            node_by_id[node.node_id] = node

        consumers: dict[int, list[int]] = {em.node_id: [] for em in emissions}
        for em in emissions:
            node = node_by_id[em.node_id]
            for pid in {self._resolve_producer(p) for p in self._raw_producer_ids(node)}:
                if pid in consumers:
                    consumers[pid].append(em.node_id)

        body, survivors = render_pipeline(emissions, consumers)
        rename = self._plan_boundary_names(emissions, survivors, node_by_id)
        return self._apply_renames(body, rename)

    @staticmethod
    def _is_renameable(em: NodeEmission, node: FlowNode) -> bool:
        """Renameable boundaries: single-output ``df_N`` assignments and filter splits.

        Multi-output nodes (random_split, ML train/apply) bind a *derived* var
        (``df_N_train``) with sibling outputs, so renaming only the primary would
        break consistency — leave their tokens untouched.
        """
        if em.pinned:
            return False
        if FlowGraphCodeConverter._is_filter_split(node):
            return True
        return em.var_name == f"df_{em.node_id}"

    def _plan_boundary_names(
        self, emissions: list[NodeEmission], survivors: set[int], node_by_id: dict[int, FlowNode]
    ) -> dict[str, str]:
        """Map provisional ``df_N`` tokens to clean labels; suffix only on collision."""
        ordered = [em for em in emissions if em.node_id in survivors]
        renameable = [em for em in ordered if self._is_renameable(em, node_by_id[em.node_id])]
        labels = {em.node_id: self._var_label(node_by_id[em.node_id]) for em in renameable}
        counts: dict[str, int] = {}
        for label in labels.values():
            counts[label] = counts.get(label, 0) + 1

        used: set[str] = {em.var_name for em in ordered if em.pinned} | set(_RESERVED_NAMES)
        final: dict[int, str] = {}
        seen: dict[str, int] = {}
        for em in renameable:
            label = labels[em.node_id]
            if counts[label] > 1:
                seen[label] = seen.get(label, 0) + 1
                name = f"{label}_{seen[label]}"
            else:
                name = label
            name = self._uniquify(name, used)
            final[em.node_id] = name

        rename: dict[str, str] = {}
        for em in renameable:
            name = final[em.node_id]
            nid = em.node_id
            if self._is_filter_split(node_by_id[nid]):
                rename[f"df_{nid}_pass"] = f"{name}_pass"
                rename[f"df_{nid}_fail"] = f"{name}_fail"
                rename[f"_filter_{nid}_pred"] = f"{name}_pred"
            else:
                rename[em.var_name] = name

        # random_split: name each output by its split (train/test/val), not df_N_<split>.
        for em in ordered:
            node = node_by_id[em.node_id]
            if em.pinned or node.node_type != "random_split":
                continue
            for split in getattr(node.setting_input, "splits", []):
                rename[f"df_{em.node_id}_{split.name}"] = self._uniquify(split.name, used)
        return rename

    @staticmethod
    def _uniquify(name: str, used: set[str]) -> str:
        """Return ``name`` (or a numbered variant) not already in ``used``; records it."""
        candidate, bump = name, 2
        while candidate in used:
            candidate, bump = f"{name}_{bump}", bump + 1
        used.add(candidate)
        return candidate

    def _apply_renames(self, body: list[str], rename: dict[str, str]) -> list[str]:
        """Substitute provisional tokens with final names across body + return targets."""
        if not rename:
            return body
        # `(?!=)` leaves keyword-argument names (``param=``, no surrounding spaces)
        # untouched while still renaming assignment targets (``var = ``) and value
        # references — e.g. a notebook call ``run(df_1=df_1.data)`` keeps its
        # contract param ``df_1`` but renames the upstream value.
        patterns = [(re.compile(r"\b" + re.escape(old) + r"\b(?!=)"), new) for old, new in rename.items()]
        out = []
        for line in body:
            for pat, new in patterns:
                line = pat.sub(new, line)
            out.append(line)
        self.output_nodes = [(nid, rename.get(var, var)) for nid, var in self.output_nodes]
        if self.last_node_var is not None:
            self.last_node_var = rename.get(self.last_node_var, self.last_node_var)
        return out

    def add_return_code(self, lines: list[str]) -> None:
        if self.output_nodes:
            if len(self.output_nodes) == 1:
                _, var_name = self.output_nodes[0]
                lines.append(f"    return {var_name}")
            else:
                lines.append("    return {")
                for node_id, var_name in self.output_nodes:
                    lines.append(f'        "node_{node_id}": {var_name},')
                lines.append("    }")
        elif self.last_node_var:
            lines.append(f"    return {self.last_node_var}")
        else:
            lines.append("    return None")

    def _build_final_code(self) -> str:
        """Build the final Python code."""
        lines = []

        lines.extend(sorted(self.imports))
        lines.append("")
        lines.append("")

        if self.custom_node_classes:
            lines.append("# Custom Node Class Definitions")
            lines.append("# These classes are user-defined nodes that were included in the flow")
            lines.append("")
            for _class_name, source_code in self.custom_node_classes.items():
                for source_line in source_code.split("\n"):
                    lines.append(source_line)
                lines.append("")
            lines.append("")

        lines.append("def run_etl_pipeline():")
        lines.append('    """')
        lines.append(f"    ETL Pipeline: {self.flow_graph.__name__}")
        lines.append("    Generated from Flowfile")
        lines.append('    """')
        lines.append("    ")

        for line in self._render_body():
            if line:
                lines.append(f"    {line}")
            else:
                lines.append("")
        lines.append("")
        self.add_return_code(lines)
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    pipeline_output = run_etl_pipeline()")

        return "\n".join(lines)


class FlowGraphToPolarsConverter(FlowGraphCodeConverter):
    """Generates standalone Polars code from a FlowGraph."""

    framework = "pl"

    def __init__(self, flow_graph: FlowGraph):
        super().__init__(flow_graph)
        self.imports.add("import polars as pl")

    def _handle_random_split(
        self, settings: input_schema.NodeRandomSplit, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Inline a polars-equivalent random split that mirrors FlowDataEngine.random_split.

        The shuffled frame is materialised once so each split shares the same
        permutation; lengths use the FlowDataEngine offset accumulator (last
        split absorbs the remainder) so generated output equals flow output
        for the same seed.
        """
        input_df = input_vars.get("main", "df")
        node_id = settings.node_id
        if settings.seed is None:
            self.imports.add("import random")
            seed_expr = "random.randint(0, 2**31 - 1)"
        else:
            seed_expr = str(settings.seed)
        self._add_code(f"_split_{node_id}_seed = {seed_expr}")
        self._add_code(f"_split_{node_id}_shuffled = (")
        self._add_code(f"    {input_df}")
        self._add_code(
            f"    .with_columns(pl.int_range(0, pl.len()).shuffle(seed=_split_{node_id}_seed)"
            ".alias('__split_rank__'))"
        )
        self._add_code("    .sort('__split_rank__').drop('__split_rank__')")
        self._add_code("    .collect()")
        self._add_code(")")
        self._add_code(f"_split_{node_id}_total = _split_{node_id}_shuffled.height")
        self._add_code(f"_split_{node_id}_off = 0")
        splits = settings.splits
        for i, s in enumerate(splits):
            split_var = f"{var_name}_{s.name}"
            if i == len(splits) - 1:
                self._add_code(
                    f"{split_var} = _split_{node_id}_shuffled.slice("
                    f"_split_{node_id}_off, max(0, _split_{node_id}_total - _split_{node_id}_off)"
                    ").lazy()"
                )
            else:
                self._add_code(
                    f"_split_{node_id}_len = int(round(_split_{node_id}_total * {s.percentage} / 100.0))"
                )
                self._add_code(
                    f"{split_var} = _split_{node_id}_shuffled.slice("
                    f"_split_{node_id}_off, max(0, _split_{node_id}_len)"
                    ").lazy()"
                )
                self._add_code(f"_split_{node_id}_off += _split_{node_id}_len")
            self.node_handle_var_mapping[(node_id, f"output-{i}")] = split_var
        default_var = f"{var_name}_{splits[0].name}"
        self.node_var_mapping[node_id] = default_var
        self._add_code("")

    def _handle_catalog_reader(
        self, settings: input_schema.NodeCatalogReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Catalog Reader is not supported for standalone Polars code. Use FlowFrame export."""
        msg = (
            "Catalog SQL Reader requires FlowFrame code generation. " "Please use FlowFrame code generation instead."
            if settings.sql_query
            else "Catalog Reader requires a FlowFrame and is not supported by Polars code generation. "
            "Please use FlowFrame code generation instead."
        )
        self.unsupported_nodes.append((settings.node_id, "catalog_reader", msg))

    def _handle_catalog_writer(
        self, settings: input_schema.NodeCatalogWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Catalog Writer is not supported for standalone Polars code. Use FlowFrame export."""
        self.unsupported_nodes.append(
            (
                settings.node_id,
                "catalog_writer",
                "Catalog Writer requires a FlowFrame and is not supported by Polars code generation. "
                "Please use FlowFrame code generation instead.",
            )
        )

    def _handle_csv_read_non_utf8(self, file_settings: input_schema.ReceivedTable, var_name: str):
        self._add_code(f"{var_name} = {self.framework}.read_csv(")
        self._add_code(f'    "{file_settings.abs_file_path}",')
        self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
        self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
        self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
        if file_settings.table_settings.encoding:
            self._add_code(f'    encoding="{file_settings.table_settings.encoding}",')
        self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
        self._add_code(").lazy()")

    def _handle_excel_read(self, file_settings: input_schema.ReceivedTable, var_name: str) -> None:
        self._add_code(f"{var_name} = {self.framework}.read_excel(")
        self._add_code(f'    "{file_settings.abs_file_path}",')
        if file_settings.table_settings.sheet_name:
            self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
        self._add_code(").lazy()")

    def _build_final_code(self) -> str:
        """Build the final Python code with a performance note when flowfile is used."""
        code = super()._build_final_code()
        if "import flowfile as ff" in code:
            perf_note = (
                "# NOTE: This pipeline uses flowfile (ff) for some I/O operations (e.g. database, catalog).\n"
                "# For better performance, consider exporting as FlowFrame code instead of Polars.\n"
                "# FlowFrame keeps the entire pipeline lazy and avoids unnecessary data materialization.\n"
            )
            code = perf_note + code
        return code

    def _handle_output_excel(self, input_df: str, output_settings) -> None:
        self._add_code(f"{input_df}.collect().write_excel(")
        self._add_code(f'    "{output_settings.abs_file_path}",')
        self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}"')
        self._add_code(")")


class FlowGraphToFlowFrameConverter(FlowGraphCodeConverter):
    """Generates FlowFrame code from a FlowGraph. Supports all node types including I/O."""

    framework = "ff"

    def __init__(self, flow_graph: FlowGraph):
        super().__init__(flow_graph)
        self.imports.add("import flowfile as ff")

    def _translate_to_ff_code(self, formula: str) -> str | None:
        """Translate a formula to native ff code, registering the imports the snippet needs.

        The validation namespace includes ``pl`` and ``datetime``, so generated
        snippets may reference them (e.g. ``today()`` translates to
        ``ff.lit(datetime.datetime.today())``); the emitted script must import
        whatever the snippet uses or it fails with NameError at runtime.
        """
        ff_code = _try_translate_to_ff_code(formula)
        if ff_code:
            if re.search(r"\bdatetime\.", ff_code):
                self.imports.add("import datetime")
            if re.search(r"\bpl\.", ff_code):
                self.imports.add("import polars as pl")
        return ff_code

    def _native_cast_type(self, data_type: str) -> str | None:
        """Render *data_type* as an ``ff.``-prefixed cast target, or None when it can't be.

        ``str()`` of nested types (e.g. ``List(Int64)``) contains inner type
        names the generated script has no bindings for, and bare container
        names ("List") don't even instantiate, so validate the candidate the
        same way translated formulas are validated and let the caller fall
        back to the legacy emission when that fails.
        """
        try:
            output_type = convert_pl_type_to_string(cast_str_to_polars_type(data_type))
        except Exception:
            return None
        if not output_type.startswith(f"{self.framework}."):
            output_type = f"{self.framework}.{output_type}"
        try:
            _eval_in_validation_namespace(output_type)
        except Exception:
            return None
        return output_type

    def _handle_random_split(
        self, settings: input_schema.NodeRandomSplit, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Delegate to FlowFrame.random_split, which already returns a tuple of frames."""
        input_df = input_vars.get("main", "df")
        node_id = settings.node_id
        split_vars = [f"{var_name}_{s.name}" for s in settings.splits]
        splits_arg = ", ".join(f'"{s.name}": {s.percentage}' for s in settings.splits)
        seed_arg = "" if settings.seed is None else f", seed={settings.seed}"
        self._add_code(
            f"{', '.join(split_vars)} = {input_df}.random_split({{{splits_arg}}}{seed_arg})"
        )
        for i, sv in enumerate(split_vars):
            self.node_handle_var_mapping[(node_id, f"output-{i}")] = sv
        self.node_var_mapping[node_id] = split_vars[0]
        self._add_code("")

    def _handle_manual_input(
        self, settings: input_schema.NodeManualInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        self.imports.add("from flowfile_core.schemas.input_schema import RawData")
        raw_data = settings.raw_data_format
        self._add_code(f"{var_name} = ff.from_raw_data(RawData(**{raw_data.model_dump()}))")
        self._add_code("")

    def _handle_cloud_storage_reader(
        self, settings: input_schema.NodeCloudStorageReader, var_name: str, input_vars: dict[str, str]
    ):
        cs = settings.cloud_storage_settings
        self._add_code(f"{var_name} = ff.read_from_cloud_storage(")
        self._add_code(f'    "{cs.resource_path}",')
        self._add_code(f'    file_format="{cs.file_format}",')
        if cs.connection_name:
            self._add_code(f'    connection_name="{cs.connection_name}",')
        if cs.scan_mode and cs.scan_mode != "single_file":
            self._add_code(f'    scan_mode="{cs.scan_mode}",')
        if cs.file_format == "csv":
            if cs.csv_delimiter != ";":
                self._add_code(f'    delimiter="{cs.csv_delimiter}",')
            if not cs.csv_has_header:
                self._add_code(f"    has_header={cs.csv_has_header},")
            if cs.csv_encoding != "utf8":
                self._add_code(f'    encoding="{cs.csv_encoding}",')
        if cs.file_format == "delta" and cs.delta_version is not None:
            self._add_code(f"    delta_version={cs.delta_version},")
        self._add_code(")")
        self._add_code("")

    def _handle_cloud_storage_writer(
        self, settings: input_schema.NodeCloudStorageWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        input_df = input_vars.get("main", "df")
        cs = settings.cloud_storage_settings
        self._add_code("ff.write_to_cloud_storage(")
        self._add_code(f"    {input_df},")
        self._add_code(f'    "{cs.resource_path}",')
        self._add_code(f'    file_format="{cs.file_format}",')
        if cs.connection_name:
            self._add_code(f'    connection_name="{cs.connection_name}",')
        if cs.file_format == "csv":
            if cs.csv_delimiter != ";":
                self._add_code(f'    delimiter="{cs.csv_delimiter}",')
            if cs.csv_encoding != "utf8":
                self._add_code(f'    encoding="{cs.csv_encoding}",')
        if cs.file_format == "parquet" and cs.parquet_compression != "snappy":
            self._add_code(f'    compression="{cs.parquet_compression}",')
        if cs.file_format == "delta" and cs.write_mode != "overwrite":
            self._add_code(f'    write_mode="{cs.write_mode}",')
        if cs.file_format == "delta" and cs.partition_by:
            self._add_code(f"    partition_by={cs.partition_by},")
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle filter nodes using FlowFrame's native flowfile_formula parameter."""
        input_df = input_vars.get("main", "df")

        if settings.split_mode:
            self._handle_filter_split(settings, var_name, input_df)
            return

        if settings.filter_input.is_advanced():
            ff_code = self._translate_to_ff_code(settings.filter_input.advanced_filter)
            if ff_code:
                self._add_code(f"{var_name} = {input_df}.filter({ff_code})")
            else:
                self._add_code(
                    f"{var_name} = {input_df}.filter(flowfile_formula={settings.filter_input.advanced_filter!r})"
                )
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                filter_expr = self._create_basic_filter_expr(basic)
                self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _handle_filter_split(self, settings: input_schema.NodeFilter, var_name: str, input_df: str) -> None:
        """FlowFrame variant: delegate to FlowFrame.filter_split which already returns (pass, fail)."""
        node_id = settings.node_id
        pass_var = f"{var_name}_pass"
        fail_var = f"{var_name}_fail"
        if settings.filter_input.is_advanced():
            ff_code = self._translate_to_ff_code(settings.filter_input.advanced_filter)
            if ff_code:
                self._add_code(f"{pass_var}, {fail_var} = {input_df}.filter_split({ff_code})")
            else:
                self._add_code(
                    f"{pass_var}, {fail_var} = {input_df}.filter_split("
                    f"flowfile_formula={settings.filter_input.advanced_filter!r})"
                )
        else:
            basic = settings.filter_input.basic_filter
            if basic is not None and basic.field:
                filter_expr = self._create_basic_filter_expr(basic)
                self._add_code(f"{pass_var}, {fail_var} = {input_df}.filter_split({filter_expr})")
            else:
                # No predicate -> mirror polars-variant fallback (pass keeps all, fail empty).
                self._add_code(
                    f'{pass_var}, {fail_var} = {input_df}.filter_split(flowfile_formula="True")'
                )
        self.node_handle_var_mapping[(node_id, "output-0")] = pass_var
        self.node_handle_var_mapping[(node_id, "output-1")] = fail_var
        self.node_var_mapping[node_id] = pass_var
        self._add_code("")

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle formula nodes, preferring native ff expressions over the flowfile_formulas parameter."""
        input_df = input_vars.get("main", "df")
        formula = settings.function.function
        col_name = settings.function.field.name
        data_type = settings.function.field.data_type

        ff_code = self._translate_to_ff_code(formula)
        cast_type = None
        if ff_code and data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            cast_type = self._native_cast_type(data_type)
            if cast_type is None:
                ff_code = None  # cast target not expressible natively; use the legacy emission
        if ff_code:
            expr_str = f'({ff_code}).alias("{col_name}")'
            if cast_type:
                expr_str += f".cast({cast_type})"
            self._add_code(f"{var_name} = {input_df}.with_columns({expr_str})")
        elif data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            self._add_code(
                f"{var_name} = {input_df}.with_columns("
                f"flowfile_formulas=[{repr(formula)}], output_column_names=[{repr(col_name)}], "
                f"output_column_datatypes=[{repr(data_type)}])"
            )
        else:
            self._add_code(
                f"{var_name} = {input_df}.with_columns("
                f"flowfile_formulas=[{repr(formula)}], output_column_names=[{repr(col_name)}])"
            )
        self._add_code("")

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        gs = settings.graph_solver_input
        self._add_code(
            f'{var_name} = {input_df}.solve_graph("{gs.col_from}", "{gs.col_to}", '
            f'output_column_name="{gs.output_column_name}")'
        )
        self._add_code("")

    def _execute_join_with_post_processing(
        self,
        settings: input_schema.NodeJoin,
        var_name: str,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
        after_join_drop_cols: list[str],
        reverse_action: dict | None,
    ) -> None:
        """FlowFrame override: use coalesce for right/outer joins instead of .collect()/.lazy().

        Passing coalesce explicitly routes FlowFrame through its Polars code path,
        giving the same join semantics as Polars without needing .collect()/.lazy()
        which FlowFrame doesn't support. FlowFrame's native join always drops right
        join keys, which is incorrect for right and outer joins.
        """
        if settings.join_input.how not in ("right", "outer"):
            super()._execute_join_with_post_processing(
                settings, var_name, left_df, right_df, left_on, right_on, after_join_drop_cols, reverse_action
            )
            return

        how = settings.join_input.how
        # coalesce=True for right joins (Polars default: drop left key, keep right key)
        # coalesce=False for outer joins (preserve both join keys for post-processing)
        coalesce = how == "right"

        has_post = bool(after_join_drop_cols) or bool(reverse_action)
        self._add_code(f"{var_name} = {'(' if has_post else ''}{left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{how}",')
        self._add_code(f"        coalesce={coalesce}")
        self._add_code("    )")

        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")

        if reverse_action:
            self._add_code(f".rename({reverse_action})")

        if has_post:
            self._add_code(")")

    def _handle_kafka_source(
        self, settings: input_schema.NodeKafkaSource, var_name: str, input_vars: dict[str, str]
    ) -> None:
        ks = settings.kafka_settings

        if not ks.kafka_connection_name and not ks.kafka_connection_id:
            self.unsupported_nodes.append(
                (settings.node_id, "kafka_source", "Kafka Source node has no connection configured")
            )
            return

        if not ks.kafka_connection_name:
            self.unsupported_nodes.append(
                (
                    settings.node_id,
                    "kafka_source",
                    "Kafka Source node uses a connection ID instead of a name. "
                    "Please use a named connection for code export.",
                )
            )
            return

        self._add_code(f"# Read from Kafka topic: {ks.topic_name}")
        self._add_code(f"{var_name} = ff.read_kafka(")
        self._add_code(f'    "{ks.kafka_connection_name}",')
        self._add_code(f'    topic_name="{ks.topic_name}",')
        if ks.max_messages != 100_000:
            self._add_code(f"    max_messages={ks.max_messages},")
        if ks.start_offset != "latest":
            self._add_code(f'    start_offset="{ks.start_offset}",')
        if ks.poll_timeout_seconds != 30.0:
            self._add_code(f"    poll_timeout_seconds={ks.poll_timeout_seconds},")
        if ks.value_format != "json":
            self._add_code(f'    value_format="{ks.value_format}",')
        self._add_code(")")
        self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input
        self._add_code(f"{var_name} = ({input_df}")
        self._add_code(f'    .with_columns({self.framework}.lit(1).alias("_temp_index_"))')
        self._add_code("    .pivot(")
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code('        index=["_temp_index_"],')
        self._add_code(f'        on="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("_temp_index_")')
        self._add_code(")")
        self._add_code("")

    def _handle_pivot(self, settings: input_schema.NodePivot, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle pivot nodes."""
        input_df = input_vars.get("main", "df")
        pivot_input = settings.pivot_input
        if len(pivot_input.aggregations) > 1:
            logger.error("Multiple aggregations are not convertable to polars code. " "Taking the first value")
        if len(pivot_input.aggregations) > 0:
            agg_func = pivot_input.aggregations[0]
        else:
            agg_func = "first"
        if len(settings.pivot_input.index_columns) == 0:
            self._handle_pivot_no_index(settings, var_name, input_df, agg_func)
        else:
            self._add_code(f"{var_name} = {input_df}.pivot(")
            self._add_code(f"    values='{pivot_input.value_col}',")
            self._add_code(f"    index={pivot_input.index_columns},")
            self._add_code(f"    on='{pivot_input.pivot_column}',")

            self._add_code(f"    aggregate_function='{agg_func}'")
            self._add_code(")")
            self._add_code("")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle custom Polars code nodes for FlowFrame.

        Replaces pl. references with ff. and uses ff.FlowFrame in type hints.
        """
        code = settings.polars_code_input.polars_code.strip()
        code = code.replace("pl.", "ff.")
        code = code.replace("ff.LazyFrame", "ff.FlowFrame")
        code = code.replace("ff.DataFrame", "ff.FlowFrame")

        if len(input_vars) == 0:
            params = ""
            args = ""
        elif len(input_vars) == 1:
            params = "input_df: ff.FlowFrame"
            input_df = list(input_vars.values())[0]
            args = input_df
        else:
            param_list = []
            arg_list = []
            i = 1
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    param_list.append(f"input_df_{i}: ff.FlowFrame")
                    arg_list.append(input_vars[key])
                    i += 1
            params = ", ".join(param_list)
            args = ", ".join(arg_list)

        is_expression = "output_df" not in code

        self._add_code("# Custom Polars code")
        self._add_code(f"def _polars_code_{settings.node_id}({params}):")

        if is_expression:
            self._add_code(f"    return {code}")
        else:
            for line in code.split("\n"):
                if line.strip():
                    self._add_code(f"    {line}")

            if "return" not in code:
                lines = [line.strip() for line in code.split("\n") if line.strip() and "=" in line]
                if lines:
                    last_assignment = lines[-1]
                    if "=" in last_assignment:
                        output_var = last_assignment.split("=")[0].strip()
                        self._add_code(f"    return {output_var}")

        self._add_code("")
        self._add_code(f"{var_name} = _polars_code_{settings.node_id}({args})")
        self._add_code("")

    def _handle_fuzzy_match(
        self, settings: input_schema.NodeFuzzyMatch, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle fuzzy match nodes using FlowFrame's native fuzzy_join method."""
        fuzzy_match_handler = transform_schema.FuzzyMatchInputManager(settings.join_input)
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))

        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")

        if fuzzy_match_handler.left_select.has_drop_cols():
            left_drop_cols = [c.old_name for c in fuzzy_match_handler.left_select.non_jk_drop_columns]
            self._add_code(f"{left_df} = {left_df}.drop({left_drop_cols})")
        if fuzzy_match_handler.right_select.has_drop_cols():
            right_drop_cols = [c.old_name for c in fuzzy_match_handler.right_select.non_jk_drop_columns]
            self._add_code(f"{right_df} = {right_df}.drop({right_drop_cols})")

        fuzzy_join_mapping_settings = self._transform_fuzzy_mappings_to_string(
            fuzzy_match_handler.join_mapping, prefix="ff."
        )
        self._add_code(
            f"{var_name} = {left_df}.fuzzy_join(\n"
            f"       {right_df},\n"
            f"       fuzzy_mappings={fuzzy_join_mapping_settings}\n"
            f"       )"
        )

    def _handle_train_model(
        self, settings: input_schema.NodeTrainModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Train Model nodes — emit ``df.train_model(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.train_input
        args = [f"target={s.target_column!r}"]
        if s.feature_columns:
            args.append(f"features={s.feature_columns!r}")
        if s.model_type != "linear_regression":
            args.append(f"model_type={s.model_type!r}")
        if s.params:
            args.append(f"params={s.params!r}")
        if s.publish_to_catalog:
            args.append("publish_to_catalog=True")
            if s.model_name:
                args.append(f"model_name={s.model_name!r}")
            if s.namespace_id is not None:
                args.append(f"namespace_id={s.namespace_id}")
            if s.catalog_description:
                args.append(f"catalog_description={s.catalog_description!r}")
            if s.catalog_tags:
                args.append(f"catalog_tags={s.catalog_tags!r}")
        self._add_code(f"{var_name} = {input_df}.train_model({', '.join(args)})")
        self._add_code("")

    def _handle_apply_model(
        self, settings: input_schema.NodeApplyModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Apply Model nodes — emit ``df.apply_model(...)`` for both upstream and catalog modes."""
        input_df = input_vars.get("main", "df")
        s = settings.apply_input
        args: list[str] = []

        if s.source == "upstream":
            if s.upstream_node_id is None:
                self.unsupported_nodes.append(
                    (settings.node_id, "apply_model", "apply_model in upstream mode has no upstream_node_id")
                )
                return
            upstream_var = self.node_var_mapping.get(s.upstream_node_id)
            if upstream_var is None:
                self.unsupported_nodes.append(
                    (
                        settings.node_id,
                        "apply_model",
                        f"apply_model upstream_node_id={s.upstream_node_id} is not present in the exported graph",
                    )
                )
                return
            args.append(f"upstream={upstream_var}")
        else:
            if not s.model_name:
                self.unsupported_nodes.append(
                    (settings.node_id, "apply_model", "apply_model in catalog mode has no model_name configured")
                )
                return
            args.append(f"model_name={s.model_name!r}")
            if s.model_version is not None:
                args.append(f"version={s.model_version}")
            if s.namespace_id is not None:
                args.append(f"namespace_id={s.namespace_id}")

        if s.output_column != "prediction":
            args.append(f"output_column={s.output_column!r}")

        self._add_code(f"{var_name} = {input_df}.apply_model({', '.join(args)})")
        self._add_code("")

    def _handle_evaluate_model(
        self, settings: input_schema.NodeEvaluateModel, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Evaluate Model nodes — emit ``df.evaluate_model(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.evaluate_input
        args = [repr(s.actual_column)]
        if s.predicted_column != "prediction":
            args.append(f"predicted_column={s.predicted_column!r}")
        if s.task_type != "auto":
            args.append(f"task_type={s.task_type!r}")
        if s.upstream_train_node_id is not None:
            upstream_var = self.node_var_mapping.get(s.upstream_train_node_id)
            # Drop upstream silently when unresolvable: evaluate_model's task_type="auto"
            # falls back to "regression", so the export is degraded but still runs.
            # (Contrast _handle_apply_model, which marks unsupported — apply needs the model.)
            if upstream_var is not None:
                args.append(f"upstream={upstream_var}")
        self._add_code(f"{var_name} = {input_df}.evaluate_model({', '.join(args)})")
        self._add_code("")

    def _handle_wait_for(
        self, settings: input_schema.NodeWaitFor, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Wait For nodes — emit ``df.wait_for(dependency)``."""
        main_df = input_vars.get("main", "df")
        dep_df = input_vars.get("right")
        if dep_df is None:
            self.unsupported_nodes.append(
                (settings.node_id, "wait_for", "wait_for node has no dependency input wired to its right handle")
            )
            return
        self._add_code(f"{var_name} = {main_df}.wait_for({dep_df})")
        self._add_code("")

    def _handle_dynamic_rename(
        self, settings: input_schema.NodeDynamicRename, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle Dynamic Rename nodes — emit ``df.dynamic_rename(...)``."""
        input_df = input_vars.get("main", "df")
        s = settings.dynamic_rename_input
        args = [f"mode={s.rename_mode!r}"]
        if s.prefix:
            args.append(f"prefix={s.prefix!r}")
        if s.suffix:
            args.append(f"suffix={s.suffix!r}")
        if s.formula:
            args.append(f"formula={s.formula!r}")
        if s.selection_mode == "list":
            args.append(f"columns={s.selected_columns!r}")
        elif s.selection_mode == "data_type" and s.selected_data_type is not None:
            args.append(f"data_type={s.selected_data_type!r}")
        self._add_code(f"{var_name} = {input_df}.dynamic_rename({', '.join(args)})")
        self._add_code("")


def export_flow_to_polars(flow_graph: FlowGraph) -> str:
    converter = FlowGraphToPolarsConverter(flow_graph)
    return converter.convert()


def export_flow_to_flowframe(flow_graph: FlowGraph) -> str:
    converter = FlowGraphToFlowFrameConverter(flow_graph)
    return converter.convert()
