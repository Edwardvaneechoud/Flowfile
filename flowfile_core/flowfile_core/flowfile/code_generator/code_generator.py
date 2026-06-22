import inspect
import re
import typing

import polars as pl
from pl_fuzzy_frame_match.models import FuzzyMapping
from polars_expr_transformer import PolarsCodeGenError, to_flowframe_code, to_polars_code

from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
from flowfile_core.flowfile.code_generator.chain_fusion import NodeEmission, render_pipeline
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.execution_orderer import determine_execution_order
from flowfile_core.schemas import input_schema, transform_schema

# Operation-based labels for generated variable names; chain fusion removes most
# of these, so the few that remain read like hand-written intermediates.
NODE_TYPE_VAR_LABEL = {
    "read": "source",
    "manual_input": "source",
    "csv_read": "source",
    "filter": "filtered",
    "select": "selected",
    "join": "joined",
    "cross_join": "joined",
    "fuzzy_match": "fuzzy_matched",
    "group_by": "grouped",
    "sort": "sorted_data",
    "record_id": "with_record_id",
    "record_count": "record_count",
    "formula": "with_columns",
    "window_functions": "windowed",
    "union": "combined",
    "unique": "deduplicated",
    "sample": "sampled",
    "pivot": "pivoted",
    "unpivot": "unpivoted",
    "graph_solver": "graph_solved",
    "polars_code": "custom",
    "output": "output",
}


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


class FlowGraphCodeConverter:
    """
    Base class for converting a FlowGraph into executable Python code.

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
        # Variable names planned up front in data-flow order (node_id -> name).
        self._planned_names: dict[int, str] = {}
        # (node, var, start, end) line spans, in emission order, for the fusion pass.
        self._node_spans: list[tuple[FlowNode, str, int, int]] = []
        # Passthrough/no-op nodes (e.g. explore_data): node_id -> upstream node id.
        self._passthrough_producer: dict[int, int] = {}

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
        ordered_nodes = [node for stage in stages for node in stage]
        self._plan_variable_names(ordered_nodes)

        for node in ordered_nodes:
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
        """Generate Polars code for a specific node."""
        node_type = node.node_type
        settings = node.setting_input
        if isinstance(settings, input_schema.NodePromise):
            self._add_comment(f"# Skipping uninitialized node: {node.node_id}")
            return
        var_name = self._planned_names.get(node.node_id) or f"df_{node.node_id}"
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

        # Output bookkeeping runs after the handler so passthrough nodes (which
        # remap node_var_mapping to their input) record the effective variable.
        effective_var = self.node_var_mapping[node.node_id]
        self.handle_output_node(node, effective_var)
        if node.node_template.output > 0:
            self.last_node_var = effective_var

        if end == start:
            # No emitted code: a passthrough/no-op node, transparent to data flow.
            main_inputs = node.node_inputs.main_inputs or []
            if len(main_inputs) == 1:
                self._passthrough_producer[node.node_id] = main_inputs[0].node_id
        else:
            self._node_spans.append((node, effective_var, start, end))

    def _plan_variable_names(self, ordered_nodes: list[FlowNode]) -> None:
        """Assign operation-based variable names in data-flow order.

        Honours a user-set ``node_reference`` when present; otherwise names each
        node ``{operation}_{step}`` with a global step index so the script reads
        top-to-bottom. Chain fusion later removes the names of fused-away nodes.
        """
        step = 0
        for node in ordered_nodes:
            settings = node.setting_input
            if isinstance(settings, input_schema.NodePromise):
                continue
            step += 1
            reference = getattr(settings, "node_reference", None)
            if reference:
                self._planned_names[node.node_id] = reference
            else:
                label = NODE_TYPE_VAR_LABEL.get(node.node_type, "df")
                self._planned_names[node.node_id] = f"{label}_{step}"

    @staticmethod
    def _raw_producer_ids(node: FlowNode) -> set[int]:
        ids = {n.node_id for n in (node.node_inputs.main_inputs or [])}
        if node.node_inputs.left_input:
            ids.add(node.node_inputs.left_input.node_id)
        if node.node_inputs.right_input:
            ids.add(node.node_inputs.right_input.node_id)
        return ids

    def _render_body(self) -> list[str]:
        """Render the function body, fusing linear single-use chains into pipes."""

        def resolve(node_id: int) -> int:
            seen: set[int] = set()
            while node_id in self._passthrough_producer and node_id not in seen:
                seen.add(node_id)
                node_id = self._passthrough_producer[node_id]
            return node_id

        emissions: list[NodeEmission] = []
        resolved_producers: dict[int, set[int]] = {}
        for node, var, start, end in self._node_spans:
            lines = self.code_lines[start:end]
            while lines and not lines[-1].strip():
                lines.pop()
            if not lines:
                continue
            resolved = {resolve(pid) for pid in self._raw_producer_ids(node)}
            resolved_producers[node.node_id] = resolved
            main_producer_id = next(iter(resolved)) if len(resolved) == 1 else None
            is_flow_output = bool(getattr(node.setting_input, "is_flow_output", False))
            pinned = bool(getattr(node.setting_input, "node_reference", None))
            emissions.append(
                NodeEmission(node.node_id, var, lines, main_producer_id, len(resolved), is_flow_output, pinned)
            )

        consumers: dict[int, list[int]] = {em.node_id: [] for em in emissions}
        for em in emissions:
            for producer_id in resolved_producers[em.node_id]:
                if producer_id in consumers:
                    consumers[producer_id].append(em.node_id)
        return render_pipeline(emissions, consumers)

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
        self._add_code(f"{var_name} = {pass_var}")
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
            # No effective projection: transparent passthrough to the input frame.
            self.node_var_mapping[settings.node_id] = input_df

    def _handle_join(self, settings: input_schema.NodeJoin, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle join nodes by routing to appropriate join type handler.

        This is the main entry point for processing join operations. It determines
        the type of join and delegates to the appropriate handler method.

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the joined DataFrame
            input_vars: Dictionary mapping input names to DataFrame variable names

        Returns:
            None: Modifies internal state by adding generated code
        """
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))
        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")

        if settings.join_input.how in ("semi", "anti"):
            self._handle_semi_anti_join(settings, var_name, left_df, right_df)
        else:
            self._handle_standard_join(settings, var_name, left_df, right_df)

    def _handle_semi_anti_join(
        self, settings: input_schema.NodeJoin, var_name: str, left_df: str, right_df: str
    ) -> None:
        """Handle semi and anti joins which only return rows from the left DataFrame.

        Semi joins return rows from left DataFrame that have matches in right.
        Anti joins return rows from left DataFrame that have no matches in right.
        These joins are simpler as they don't require column management from right DataFrame.

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            None: Modifies internal state by adding generated code
        """
        left_on = [jm.left_col for jm in settings.join_input.join_mapping]
        right_on = [jm.right_col for jm in settings.join_input.join_mapping]

        self._add_code(f"{var_name} = ({left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{settings.join_input.how}"')
        self._add_code("    )")
        self._add_code(")")

    def _handle_standard_join(
        self, settings: input_schema.NodeJoin, var_name: str, left_df: str, right_df: str
    ) -> None:
        """Handle standard joins (left, right, inner, outer) with full column management.

        Standard joins may include columns from both DataFrames and require careful
        management of column names, duplicates, and transformations. This method
        orchestrates the complete join process including pre/post transformations.

        Process:
        1. Auto-rename columns to avoid conflicts
        2. Extract join keys
        3. Apply pre-join transformations (renames, drops)
        4. Handle join-specific key transformations
        5. Execute join with post-processing

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            None: Modifies internal state by adding generated code
        """
        join_input_manager = transform_schema.JoinInputManager(settings.join_input)
        join_input_manager.auto_rename()
        left_on, right_on = self._get_join_keys(join_input_manager)

        left_df, right_df = self._apply_pre_join_transformations(join_input_manager, left_df, right_df)
        left_on, right_on, reverse_action, after_join_drop_cols = self._handle_join_key_transformations(
            join_input_manager, left_df, right_df, left_on, right_on
        )
        self._execute_join_with_post_processing(
            settings, var_name, left_df, right_df, left_on, right_on, after_join_drop_cols, reverse_action
        )

    @staticmethod
    def _get_join_keys(settings: transform_schema.JoinInputManager) -> tuple[list[str], list[str]]:
        """Extract join keys based on join type.

        Different join types require different handling of join keys:
        - For outer/right joins: Uses renamed column names for right DataFrame
        - For other joins: Uses original column names from join mapping

        Args:
            settings: NodeJoin settings containing join configuration

        Returns:
            Tuple[List[str], List[str]]: Lists of (left_on, right_on) column names
        """
        left_on = [jm.left_col for jm in settings.get_names_for_table_rename()]

        if settings.how in ("outer", "right"):
            right_on = [jm.right_col for jm in settings.get_names_for_table_rename()]
        else:
            right_on = [jm.right_col for jm in settings.join_mapping]

        return left_on, right_on

    def _apply_pre_join_transformations(
        self, settings: transform_schema.JoinInputManager, left_df: str, right_df: str
    ) -> tuple[str, str]:
        """Apply column renames and drops before the join operation.

        Pre-join transformations prepare DataFrames by:
        - Renaming columns according to user specifications
        - Dropping columns marked as not to keep (except join keys)
        - Special handling for right/outer joins where join keys may need preservation

        Args:
            settings: NodeJoin settings containing column rename/drop specifications
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            Tuple[str, str]: The same DataFrame variable names (left_df, right_df)
                Note: DataFrames are modified via generated code, not new variables
        """
        right_renames = {
            column.old_name: column.new_name
            for column in settings.right_select.renames
            if column.old_name != column.new_name and not column.join_key or settings.how in ("outer", "right")
        }

        left_renames = {
            column.old_name: column.new_name
            for column in settings.left_select.renames
            if column.old_name != column.new_name
        }

        left_drop_columns = [
            column.old_name for column in settings.left_select.renames if not column.keep and not column.join_key
        ]

        right_drop_columns = [
            column.old_name for column in settings.right_select.renames if not column.keep and not column.join_key
        ]

        if right_renames:
            self._add_code(f"{right_df} = {right_df}.rename({right_renames})")
        if left_renames:
            self._add_code(f"{left_df} = {left_df}.rename({left_renames})")
        if left_drop_columns:
            self._add_code(f"{left_df} = {left_df}.drop({left_drop_columns})")
        if right_drop_columns:
            self._add_code(f"{right_df} = {right_df}.drop({right_drop_columns})")

        return left_df, right_df

    def _handle_join_key_transformations(
        self,
        settings: transform_schema.JoinInputManager,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
    ) -> tuple[list[str], list[str], dict | None, list[str]]:
        """Route to appropriate join-specific key transformation handler.

        Different join types require different strategies for handling join keys
        to avoid conflicts and preserve necessary columns.

        Args:
            settings: NodeJoin settings containing join configuration
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Potentially modified list of left join columns
                - right_on: Potentially modified list of right join columns
                - reverse_action: Dictionary for renaming columns after join (or None)
                - after_join_drop_cols: List of columns to drop after join
        """
        join_type = settings.how

        if join_type in ("left", "inner"):
            return self._handle_left_inner_join_keys(settings, right_df, left_on, right_on)
        elif join_type == "right":
            return self._handle_right_join_keys(settings, left_df, left_on, right_on)
        elif join_type == "outer":
            return self._handle_outer_join_keys(settings, right_df, left_on, right_on)
        else:
            return left_on, right_on, None, []

    def _handle_left_inner_join_keys(
        self, settings: transform_schema.JoinInputManager, right_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], dict, list[str]]:
        """Handle key transformations for left and inner joins.

        For left/inner joins:
        - Join keys from left DataFrame are preserved
        - Right DataFrame join keys are temporarily renamed with __DROP__ prefix
        - After join, these temporary columns can be renamed back if needed

        Args:
            settings: NodeJoin settings containing join configuration
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Unchanged left join columns
                - right_on: Unchanged right join columns
                - reverse_action: Mapping to rename __DROP__ columns after join
                - after_join_drop_cols: Left join keys marked for dropping
        """
        [jk.new_name for jk in settings.left_select.join_key_selects if jk.keep]
        join_key_duplication_command = [
            f'{self.framework}.col("{rjk.old_name}").alias("__DROP__{rjk.new_name}__DROP__")'
            for rjk in settings.right_select.join_key_selects
            if rjk.keep
        ]

        reverse_action = {
            f"__DROP__{rjk.new_name}__DROP__": rjk.new_name
            for rjk in settings.right_select.join_key_selects
            if rjk.keep
        }

        if join_key_duplication_command:
            self._add_code(f"{right_df} = {right_df}.with_columns([{', '.join(join_key_duplication_command)}])")

        after_join_drop_cols = [k.new_name for k in settings.left_select.join_key_selects if not k.keep]

        return left_on, right_on, reverse_action, after_join_drop_cols

    def _handle_right_join_keys(
        self, settings: transform_schema.JoinInputManager, left_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], None, list[str]]:
        """Handle key transformations for right joins.

        For right joins:
        - Join keys from right DataFrame are preserved
        - Left DataFrame join keys are prefixed with __jk_ to avoid conflicts
        - Polars appends "_right" suffix to conflicting column names

        Args:
            settings: NodeJoin settings containing join configuration
            left_df: Variable name of the left DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Modified left join columns with __jk_ prefix where needed
                - right_on: Unchanged right join columns
                - reverse_action: None (no post-join renaming needed)
                - after_join_drop_cols: Right join keys marked for dropping
        """
        join_key_duplication_command = [
            f'{self.framework}.col("{ljk.new_name}").alias("__jk_{ljk.new_name}")'
            for ljk in settings.left_select.join_key_selects
            if ljk.keep
        ]

        for position, left_on_key in enumerate(left_on):
            left_on_select = settings.left_select.get_select_input_on_new_name(left_on_key)
            if left_on_select and left_on_select.keep:
                left_on[position] = f"__jk_{left_on_select.new_name}"

        if join_key_duplication_command:
            self._add_code(f"{left_df} = {left_df}.with_columns([{', '.join(join_key_duplication_command)}])")

        left_join_keys_keep = {jk.new_name for jk in settings.left_select.join_key_selects if jk.keep}
        after_join_drop_cols_right = [
            jk.new_name if jk.new_name not in left_join_keys_keep else jk.new_name + "_right"
            for jk in settings.right_select.join_key_selects
            if not jk.keep
        ]
        after_join_drop_cols = list(set(after_join_drop_cols_right))
        return left_on, right_on, None, after_join_drop_cols

    def _handle_outer_join_keys(
        self, settings: transform_schema.JoinInputManager, right_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], dict, list[str]]:
        """Handle key transformations for outer joins.

        For outer joins:
        - Both left and right join keys may need to be preserved
        - Right DataFrame join keys are prefixed with __jk_ when they conflict
        - Post-join renaming reverses the __jk_ prefix

        Args:
            settings: NodeJoin settings containing join configuration
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Unchanged left join columns
                - right_on: Modified right join columns with __jk_ prefix where needed
                - reverse_action: Mapping to remove __jk_ prefix after join
                - after_join_drop_cols: Combined list of columns to drop from both sides
        """
        left_join_keys = {jk.new_name for jk in settings.left_select.join_key_selects}

        join_keys_to_keep_and_rename = [
            rjk for rjk in settings.right_select.join_key_selects if rjk.keep and rjk.new_name in left_join_keys
        ]

        join_key_rename_command = {rjk.new_name: f"__jk_{rjk.new_name}" for rjk in join_keys_to_keep_and_rename}

        for position, right_on_key in enumerate(right_on):
            right_on_select = settings.right_select.get_select_input_on_new_name(right_on_key)
            if right_on_select and right_on_select.keep and right_on_select.new_name in left_join_keys:
                right_on[position] = f"__jk_{right_on_select.new_name}"

        if join_key_rename_command:
            self._add_code(f"{right_df} = {right_df}.rename({join_key_rename_command})")

        reverse_action = {f"__jk_{rjk.new_name}": rjk.new_name for rjk in join_keys_to_keep_and_rename}

        after_join_drop_cols_left = [jk.new_name for jk in settings.left_select.join_key_selects if not jk.keep]
        after_join_drop_cols_right = [
            jk.new_name if jk.new_name not in left_join_keys else jk.new_name + "_right"
            for jk in settings.right_select.join_key_selects
            if not jk.keep
        ]
        after_join_drop_cols = after_join_drop_cols_left + after_join_drop_cols_right

        return left_on, right_on, reverse_action, after_join_drop_cols

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
        """Execute the join operation and apply post-processing steps.

        Generates the actual join code with any necessary post-processing:
        1. Executes the join operation
        2. For right joins: Collects to eager mode (Polars requirement)
        3. Drops unnecessary columns
        4. Renames temporary columns back to final names
        5. For right joins: Converts back to lazy mode

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on
            after_join_drop_cols: List of columns to drop after join
            reverse_action: Dictionary for renaming columns after join (or None)

        Returns:
            None: Modifies internal state by adding generated code
        """
        self._add_code(f"{var_name} = ({left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{settings.join_input.how}"')
        self._add_code("    )")

        # TODO(FlowFrame): The .collect().lazy() pattern for right joins returns a
        # pl.LazyFrame, breaking the FlowFrame chain. The FlowFrame converter may
        # need to override join handling or use framework-aware collect/lazy.
        if settings.join_input.how == "right":
            self._add_code(".collect()")  # Right join needs to be collected first cause of issue with rename

        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")

        if reverse_action:
            self._add_code(f".rename({reverse_action})")

        if settings.join_input.how == "right":
            self._add_code(".lazy()")

        self._add_code(")")

    def _handle_group_by(self, settings: input_schema.NodeGroupBy, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle group by nodes."""
        input_df = input_vars.get("main", "df")

        group_cols = []
        agg_exprs = []

        for agg_col in settings.groupby_input.agg_cols:
            if agg_col.agg == "groupby":
                group_cols.append(agg_col.old_name)
            else:
                agg_func = self._get_agg_function(agg_col.agg)
                expr = f'{self.framework}.col("{agg_col.old_name}").{agg_func}().alias("{agg_col.new_name}")'
                agg_exprs.append(expr)

        self._add_code(f"{var_name} = {input_df}.group_by({group_cols}).agg([")
        for expr in agg_exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle formula/expression nodes."""
        input_df = input_vars.get("main", "df")
        formula = settings.function.function
        col_name = settings.function.field.name
        can_convert_to_pl_code: bool = False
        pl_code: str | None = None
        try:
            pl_code = to_polars_code(formula)
            if pl_code:
                can_convert_to_pl_code = True
        except PolarsCodeGenError:
            can_convert_to_pl_code = False
        except Exception as e:
            logger.debug(f"Unhandled conversion of the formula to polars expression falling back to expression {e}")
            can_convert_to_pl_code = False

        # TODO(FlowFrame): to_polars_code() generates pl.col/pl.lit expressions that require
        # `import polars as pl`. When framework == "ff", either:
        # (a) add `import polars as pl` to FlowFrame converter imports, or
        # (b) post-process the expression to replace `pl.` with `{self.framework}.`, or
        # (c) make to_polars_code() accept a framework prefix parameter.
        if can_convert_to_pl_code:
            expr_str = f'({pl_code}).alias("{col_name}")'
            if settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
                output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
                if output_type[:3] != f"{self.framework}.":
                    output_type = f"{self.framework}." + output_type
                expr_str += f".cast({output_type})"
            self._add_code(f"{var_name} = {input_df}.with_columns([{expr_str}])")
            self._add_code("")
        else:
            self.imports.add(
                "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
            )
            self._add_code(f"{var_name} = {input_df}.with_columns([")
            self._add_code(f'simple_function_to_expr({repr(formula)}).alias("{col_name}")')
            if settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
                output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
                if output_type[:3] != f"{self.framework}.":
                    output_type = f"{self.framework}." + output_type
                self._add_code(f"    .cast({output_type})")
            self._add_code("])")
            self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input
        self._add_code(f"{var_name} = ({input_df}.collect()")
        self._add_code(f'    .with_columns({self.framework}.lit(1).alias("_temp_index_"))')
        self._add_code("    .pivot(")
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code('        index=["_temp_index_"],')
        self._add_code(f'        on="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("_temp_index_")')
        self._add_code(").lazy()")
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
            self._add_code(f"{var_name} = {input_df}.collect().pivot(")
            self._add_code(f"    values='{pivot_input.value_col}',")
            self._add_code(f"    index={pivot_input.index_columns},")
            self._add_code(f"    on='{pivot_input.pivot_column}',")

            self._add_code(f"    aggregate_function='{agg_func}'")
            self._add_code(").lazy()")
            self._add_code("")

    def _handle_unpivot(self, settings: input_schema.NodeUnpivot, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle unpivot nodes."""
        input_df = input_vars.get("main", "df")
        unpivot_input = settings.unpivot_input

        self._add_code(f"{var_name} = {input_df}.unpivot(")

        if unpivot_input.index_columns:
            self._add_code(f"    index={unpivot_input.index_columns},")

        if unpivot_input.value_columns:
            self._add_code(f"    on={unpivot_input.value_columns},")

        self._add_code("    variable_name='variable',")
        self._add_code("    value_name='value'")
        self._add_code(")")
        self._add_code("")

    def _handle_union(self, settings: input_schema.NodeUnion, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle union nodes."""
        dfs = []
        if "main" in input_vars:
            dfs.append(input_vars["main"])
        else:
            for key, df_var in input_vars.items():
                if key.startswith("main"):
                    dfs.append(df_var)

        if settings.union_input.mode == "relaxed":
            how = "diagonal_relaxed"
        else:
            how = "diagonal"

        self._add_code(f"{var_name} = {self.framework}.concat([")
        for df in dfs:
            self._add_code(f"    {df},")
        self._add_code(f"], how='{how}')")
        self._add_code("")

    def _handle_sort(self, settings: input_schema.NodeSort, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle sort nodes."""
        input_df = input_vars.get("main", "df")

        sort_cols = []
        descending = []

        for sort_input in settings.sort_input:
            sort_cols.append(f'"{sort_input.column}"')
            descending.append(sort_input.how == "desc")

        self._add_code(f"{var_name} = {input_df}.sort([{', '.join(sort_cols)}], descending={descending})")
        self._add_code("")

    def _handle_sample(self, settings: input_schema.NodeSample, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle sample nodes."""
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.head(n={settings.sample_size})")
        self._add_code("")

    def _build_window_expr_code(
        self,
        w: "transform_schema.WindowFunctionInput",
        partition_by: list[str],
        order_by: list["transform_schema.SortByInput"] | None = None,
    ) -> str:
        """Builds a Polars expression string for a single window-function op."""
        fw = self.framework
        partition_repr = repr(partition_by) if partition_by else None

        def over(expr: str) -> str:
            return f"{expr}.over({partition_repr})" if partition_by else expr

        func = w.function
        if func.startswith("rolling_"):
            behavior = w.edge_behavior or "require_full"
            kwargs = f"window_size={w.window_size}"
            if behavior in ("partial", "fill_zero"):
                kwargs += ", min_samples=1"
            elif w.min_periods is not None:
                kwargs += f", min_samples={w.min_periods}"
            base = f'{fw}.col("{w.column}").{func}({kwargs})'
            if behavior == "fill_zero":
                base = f"{base}.fill_null(0)"
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func.startswith("cum_"):
            base = f'{fw}.col("{w.column}").{func}()'
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func == "rank":
            method = w.rank_method or "ordinal"
            base = f'{fw}.col("{w.column}").rank(method="{method}")'
            return f'{over(base)}.alias("{w.new_column_name}")'
        if func == "tile":
            # Tile uses only Expr methods (cum_count, when/then/otherwise) and the
            # framework-level ``len()`` so it works in both pl and ff codegen.
            if not order_by:
                raise ValueError("tile requires at least one order_by column")
            order_col = order_by[0].column
            n = int(w.number_of_groups)
            pos = over(f'{fw}.col("{order_col}").cum_count()') + " - 1"  # 0..N-1 per group
            group_len = over(f"{fw}.len()")
            big = f"(({group_len}) + {n} - 1) // {n}"
            threshold = f"(({group_len}) % {n}) * ({big})"
            small = (
                f"{fw}.when((({group_len}) // {n}) < 1).then(1)"
                f".otherwise(({group_len}) // {n})"
            )
            expr = (
                f"{fw}.when(({pos}) < ({threshold}))"
                f".then(({pos}) // ({big}) + 1)"
                f".otherwise((({pos}) - ({threshold})) // ({small}) + (({group_len}) % {n}) + 1)"
                f".cast({fw}.Int64)"
            )
            return f'{expr}.alias("{w.new_column_name}")'
        raise ValueError(f"Unsupported window function: {func!r}")

    def _handle_window_functions(
        self, settings: input_schema.NodeWindowFunctions, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle window function nodes (rolling, cumulative, rank, tile)."""
        input_df = input_vars.get("main", "df")
        window_input = settings.window_input

        sorted_df = input_df
        if window_input.order_by:
            sort_cols = [f'"{s.column}"' for s in window_input.order_by]
            descending = [s.how == "desc" for s in window_input.order_by]
            self._add_code(f"{var_name} = {input_df}.sort([{', '.join(sort_cols)}], descending={descending})")
            sorted_df = var_name

        exprs = [
            self._build_window_expr_code(w, window_input.partition_by, window_input.order_by)
            for w in window_input.window_functions
        ]
        self._add_code(f"{var_name} = {sorted_df}.with_columns([")
        for expr in exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    @staticmethod
    def _transform_fuzzy_mappings_to_string(fuzzy_mappings: list[FuzzyMapping], prefix: str = "") -> str:
        # TODO(FlowFrame): FuzzyMapping fields containing Polars Expr objects
        # (e.g. threshold_expr) are serialized via repr, producing invalid code like
        # `pl.lit(<Expr ['len()'] at 0x...>)`. Need to convert Expr objects to their
        # code string representation.
        output_str = "["
        for i, fuzzy_mapping in enumerate(fuzzy_mappings):
            output_str += (
                f"{prefix}FuzzyMapping(left_col='{fuzzy_mapping.left_col}',"
                f" right_col='{fuzzy_mapping.right_col}', "
                f"threshold_score={fuzzy_mapping.threshold_score}, "
                f"fuzzy_type='{fuzzy_mapping.fuzzy_type}')"
            )
            if i < len(fuzzy_mappings) - 1:
                output_str += ",\n"
        output_str += "]"
        return output_str

    def _handle_fuzzy_match(
        self, settings: input_schema.NodeFuzzyMatch, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle fuzzy match nodes."""
        self.imports.add("from pl_fuzzy_frame_match import FuzzyMapping, fuzzy_match_dfs")
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

        fuzzy_join_mapping_settings = self._transform_fuzzy_mappings_to_string(fuzzy_match_handler.join_mapping)
        self._add_code(
            f"{var_name} = fuzzy_match_dfs(\n"
            f"       left_df={left_df}, right_df={right_df},\n"
            f"       fuzzy_maps={fuzzy_join_mapping_settings}\n"
            f"       ).lazy()"
        )

    def _handle_unique(self, settings: input_schema.NodeUnique, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle unique/distinct nodes."""
        input_df = input_vars.get("main", "df")

        if settings.unique_input.columns:
            self._add_code(
                f"{var_name} = {input_df}.unique("
                f"subset={settings.unique_input.columns}, keep='{settings.unique_input.strategy}')"
            )
        else:
            self._add_code(f"{var_name} = {input_df}.unique(keep='{settings.unique_input.strategy}')")
        self._add_code("")

    def _handle_text_to_rows(
        self, settings: input_schema.NodeTextToRows, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle text to rows (explode) nodes."""
        # TODO(FlowFrame): Verify that {self.framework}.col() expressions work correctly
        # when the input DataFrame may have been converted to pl.LazyFrame (e.g., after
        # pivot .collect().lazy() or right join .collect().lazy() chains).
        input_df = input_vars.get("main", "df")
        text_input = settings.text_to_rows_input

        split_expr = f'{self.framework}.col("{text_input.column_to_split}").str.split("{text_input.split_fixed_value}")'
        if text_input.output_column_name and text_input.output_column_name != text_input.column_to_split:
            split_expr = f'{split_expr}.alias("{text_input.output_column_name}")'
            explode_col = text_input.output_column_name
        else:
            explode_col = text_input.column_to_split

        self._add_code(f"{var_name} = {input_df}.with_columns({split_expr}).explode('{explode_col}')")
        self._add_code("")

    # .with_columns(
    #     (pl.cum_count(record_id_settings.output_column_name)
    #      .over(record_id_settings.group_by_columns) + record_id_settings.offset - 1)
    #     .alias(record_id_settings.output_column_name)
    # )
    def _handle_record_id(self, settings: input_schema.NodeRecordId, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle record ID nodes."""
        input_df = input_vars.get("main", "df")
        record_input = settings.record_id_input
        if record_input.group_by and record_input.group_by_columns:
            # cum_count is 1-indexed here, so the net shift is (offset - 1); only
            # emit it when it does not cancel out (default offset=1 -> nothing).
            delta = record_input.offset - 1
            offset_expr = f" + {delta}" if delta > 0 else f" - {abs(delta)}" if delta < 0 else ""
            self._add_code(f"{var_name} = ({input_df}")
            self._add_code(f"    .with_columns({self.framework}.lit(1).alias('{record_input.output_column_name}'))")
            self._add_code("    .with_columns([")
            self._add_code(
                f"    ({self.framework}.cum_count('{record_input.output_column_name}')"
                f".over({record_input.group_by_columns}){offset_expr})"
            )
            self._add_code(f"    .alias('{record_input.output_column_name}')")
            self._add_code("])")
            out_col = record_input.output_column_name
            self._add_code(f".select(['{out_col}'] + [col for col in {input_df}.columns if col != '{out_col}'])")
            self._add_code(")")
        else:
            self._add_code(
                f"{var_name} = {input_df}.with_row_count("
                f"name='{record_input.output_column_name}', offset={record_input.offset})"
            )
        self._add_code("")

    def _handle_cross_join(
        self, settings: input_schema.NodeCrossJoin, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle cross join nodes."""
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))

        self._add_code(f"{var_name} = {left_df}.join({right_df}, how='cross')")
        self._add_code("")

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
        self._add_code(f"def _polars_code_{var_name.replace('df_', '')}({params}):")

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

        self._add_code(f"{var_name} = _polars_code_{var_name.replace('df_', '')}({args})")
        self._add_code("")

    # Handlers for unsupported node types - these add nodes to the unsupported list

    def _handle_explore_data(
        self, settings: input_schema.NodeExploreData, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle explore_data nodes - interactive visualization only, so emit nothing.

        Remapping to the input variable makes the node transparent: downstream
        references and the return value resolve straight to the upstream frame, so
        no dead ``var = input`` passthrough appears in the generated code.
        """
        input_df = input_vars.get("main", "df")
        self.node_var_mapping[settings.node_id] = input_df

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

    def _check_process_method_signature(self, custom_node_class: type) -> tuple[bool, bool]:
        """
        Check the process method signature to determine if collect/lazy is needed.

        Returns:
            Tuple of (needs_collect, needs_lazy):
            - needs_collect: True if inputs need to be collected to DataFrame before passing to process()
            - needs_lazy: True if output needs to be converted to LazyFrame after process()
        """
        needs_collect = True
        needs_lazy = True

        process_method = getattr(custom_node_class, "process", None)
        if process_method is None:
            return needs_collect, needs_lazy

        try:
            type_hints = typing.get_type_hints(process_method)

            return_type = type_hints.get("return")
            if return_type is not None:
                return_type_str = str(return_type)
                if "LazyFrame" in return_type_str:
                    needs_lazy = False

            sig = inspect.signature(process_method)
            params = list(sig.parameters.values())
            for param in params[1:]:
                if param.annotation != inspect.Parameter.empty:
                    param_type_str = str(param.annotation)
                    if "LazyFrame" in param_type_str:
                        needs_collect = False
                        break
                if param.name in type_hints:
                    hint_str = str(type_hints[param.name])
                    if "LazyFrame" in hint_str:
                        needs_collect = False
                        break
        except Exception as e:
            # If we can't determine types, use defaults (collect + lazy)
            logger.debug(f"Could not determine process method signature: {e}")

        return needs_collect, needs_lazy

    def _read_custom_node_source_file(self, custom_node_class: type) -> str | None:
        """
        Read the entire source file where a custom node class is defined.
        This includes all class definitions in that file (settings schemas, etc.).

        Returns:
            The complete source code from the file, or None if not readable.
        """
        try:
            source_file = inspect.getfile(custom_node_class)
            with open(source_file) as f:
                return f.read()
        except (OSError, TypeError):
            return None

    def _handle_user_defined(self, node: FlowNode, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle user-defined custom nodes by including their class definition and calling process()."""
        custom_node_class = self._lookup_custom_node_class(node)
        if custom_node_class is None:
            return
        if not self._register_custom_node_source(node, custom_node_class):
            return
        self._emit_user_defined_call(node, custom_node_class, var_name, input_vars)

    def _lookup_custom_node_class(self, node: FlowNode) -> type | None:
        """Resolve a user-defined node's class from the registry, recording unsupported on miss."""
        node_type = node.node_type
        custom_node_class = CUSTOM_NODE_STORE.get(node_type)
        if custom_node_class is None:
            self.unsupported_nodes.append(
                (node.node_id, node_type, f"User-defined node type '{node_type}' not found in the custom node registry")
            )
            self._add_comment(f"# Node {node.node_id}: User-defined node '{node_type}' - Not found in registry")
        return custom_node_class

    def _register_custom_node_source(self, node: FlowNode, custom_node_class: type) -> bool:
        """Capture the custom node's class source for inlining into the generated script.

        Returns False (after recording the node as unsupported) when the
        source cannot be retrieved.
        """
        node_type = node.node_type
        class_name = custom_node_class.__name__
        if class_name not in self.custom_node_classes:
            file_source = self._read_custom_node_source_file(custom_node_class)
            if file_source:
                # Remove import lines from the file since we handle imports separately
                lines = file_source.split("\n")
                non_import_lines = []
                in_multiline_import = False
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith("import ") or stripped.startswith("from "):
                        if "(" in stripped and ")" not in stripped:
                            in_multiline_import = True
                        continue
                    if in_multiline_import:
                        if ")" in stripped:
                            in_multiline_import = False
                        continue
                    # Skip comments at the very start (like "# Auto-generated custom node")
                    if stripped.startswith("#") and not non_import_lines:
                        continue
                    non_import_lines.append(line)
                while non_import_lines and not non_import_lines[0].strip():
                    non_import_lines.pop(0)
                self.custom_node_classes[class_name] = "\n".join(non_import_lines)
            else:
                try:
                    self.custom_node_classes[class_name] = inspect.getsource(custom_node_class)
                except (OSError, TypeError) as e:
                    self.unsupported_nodes.append(
                        (node.node_id, node_type, f"Could not retrieve source code for user-defined node: {e}")
                    )
                    self._add_comment(
                        f"# Node {node.node_id}: User-defined node '{node_type}' - Source code unavailable"
                    )
                    return False

            self.imports.add(
                "from flowfile_core.flowfile.node_designer import ("
                "CustomNodeBase, Section, NodeSettings, SingleSelect, MultiSelect, "
                "IncomingColumns, ColumnSelector, NumericInput, TextInput, "
                "ColumnActionInput, SliderInput, ToggleSwitch)"
            )
        return True

    def _emit_user_defined_call(
        self, node: FlowNode, custom_node_class: type, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the instantiation, settings population, and process() call for a custom node."""
        settings = node.setting_input
        class_name = custom_node_class.__name__
        settings_dict = getattr(settings, "settings", {}) or {}

        needs_collect, needs_lazy = self._check_process_method_signature(custom_node_class)

        _node_name_field = custom_node_class.model_fields.get("node_name", type("", (), {"default": node.node_type}))
        self._add_code(f"# User-defined node: {_node_name_field.default}")
        self._add_code(f"_custom_node_{node.node_id} = {class_name}()")

        if settings_dict:
            self._add_code(f"_custom_node_{node.node_id}_settings = {repr(settings_dict)}")
            self._add_code(f"if _custom_node_{node.node_id}.settings_schema:")
            node_var = f"_custom_node_{node.node_id}"
            self._add_code(f"    {node_var}.settings_schema.populate_values({node_var}_settings)")

        if len(input_vars) == 0:
            input_args = ""
        elif len(input_vars) == 1:
            input_df = list(input_vars.values())[0]
            input_args = f"{input_df}.collect()" if needs_collect else input_df
        else:
            arg_list = []
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    if needs_collect:
                        arg_list.append(f"{input_vars[key]}.collect()")
                    else:
                        arg_list.append(input_vars[key])
            input_args = ", ".join(arg_list)

        if needs_lazy:
            self._add_code(f"{var_name} = _custom_node_{node.node_id}.process({input_args}).lazy()")
        else:
            self._add_code(f"{var_name} = _custom_node_{node.node_id}.process({input_args})")
        self._add_code("")

    # Helper methods

    def _add_code(self, line: str) -> None:
        """Add a line of code."""
        self.code_lines.append(line)

    def _add_comment(self, comment: str) -> None:
        """Add a comment line."""
        self.code_lines.append(comment)

    def _parse_filter_expression(self, expr: str) -> str:
        """Parse Flowfile filter expression to Polars expression."""
        import re

        # Pattern: [column_name]operator"value" or [column_name]operatorvalue
        pattern = r'\[([^\]]+)\]([><=!]+)"?([^"]*)"?'

        def replace_expr(match):
            col, op, val = match.groups()

            op_map = {"=": "==", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}

            polars_op = op_map.get(op, op)

            try:
                float(val)
                return f'{self.framework}.col("{col}") {polars_op} {val}'
            except ValueError:
                return f'{self.framework}.col("{col}") {polars_op} "{val}"'

        return re.sub(pattern, replace_expr, expr)

    def _create_basic_filter_expr(self, basic: transform_schema.BasicFilter) -> str:
        """Create Polars expression from basic filter.

        Generates proper Polars code for all supported filter operators.

        Args:
            basic: The BasicFilter configuration.

        Returns:
            A string containing valid Polars filter expression code.
        """
        from flowfile_core.schemas.transform_schema import FilterOperator

        col = f'{self.framework}.col("{basic.field}")'
        value = basic.value
        value2 = basic.value2

        # Determine if value is numeric (for proper quoting)
        is_numeric = value.replace(".", "", 1).replace("-", "", 1).isnumeric() if value else False

        try:
            operator = basic.get_operator()
        except (ValueError, AttributeError):
            operator = FilterOperator.from_symbol(str(basic.operator))

        if operator == FilterOperator.EQUALS:
            if is_numeric:
                return f"{col} == {value}"
            return f'{col} == "{value}"'

        elif operator == FilterOperator.NOT_EQUALS:
            if is_numeric:
                return f"{col} != {value}"
            return f'{col} != "{value}"'

        elif operator == FilterOperator.GREATER_THAN:
            if is_numeric:
                return f"{col} > {value}"
            return f'{col} > "{value}"'

        elif operator == FilterOperator.GREATER_THAN_OR_EQUALS:
            if is_numeric:
                return f"{col} >= {value}"
            return f'{col} >= "{value}"'

        elif operator == FilterOperator.LESS_THAN:
            if is_numeric:
                return f"{col} < {value}"
            return f'{col} < "{value}"'

        elif operator == FilterOperator.LESS_THAN_OR_EQUALS:
            if is_numeric:
                return f"{col} <= {value}"
            return f'{col} <= "{value}"'

        elif operator == FilterOperator.CONTAINS:
            return f'{col}.str.contains("{value}")'

        elif operator == FilterOperator.NOT_CONTAINS:
            return f'{col}.str.contains("{value}").not_()'

        elif operator == FilterOperator.STARTS_WITH:
            return f'{col}.str.starts_with("{value}")'

        elif operator == FilterOperator.ENDS_WITH:
            return f'{col}.str.ends_with("{value}")'

        elif operator == FilterOperator.IS_NULL:
            return f"{col}.is_null()"

        elif operator == FilterOperator.IS_NOT_NULL:
            return f"{col}.is_not_null()"

        elif operator == FilterOperator.IN:
            values = [v.strip() for v in value.split(",")]
            if all(v.replace(".", "", 1).replace("-", "", 1).isnumeric() for v in values):
                values_str = ", ".join(values)
            else:
                values_str = ", ".join(f'"{v}"' for v in values)
            return f"{col}.is_in([{values_str}])"

        elif operator == FilterOperator.NOT_IN:
            values = [v.strip() for v in value.split(",")]
            if all(v.replace(".", "", 1).replace("-", "", 1).isnumeric() for v in values):
                values_str = ", ".join(values)
            else:
                values_str = ", ".join(f'"{v}"' for v in values)
            return f"{col}.is_in([{values_str}]).not_()"

        elif operator == FilterOperator.BETWEEN:
            if value2 is None:
                return f"{col}  # BETWEEN requires two values"
            if is_numeric and value2.replace(".", "", 1).replace("-", "", 1).isnumeric():
                return f"({col} >= {value}) & ({col} <= {value2})"
            return f'({col} >= "{value}") & ({col} <= "{value2}")'

        return col

    def _get_polars_dtype(self, dtype_str: str) -> str:
        fw = self.framework
        dtype_map = {
            "String": f"{fw}.Utf8",
            "Integer": f"{fw}.Int64",
            "Double": f"{fw}.Float64",
            "Boolean": f"{fw}.Boolean",
            "Date": f"{fw}.Date",
            "Datetime": f"{fw}.Datetime",
            "Float32": f"{fw}.Float32",
            "Float64": f"{fw}.Float64",
            "Int32": f"{fw}.Int32",
            "Int64": f"{fw}.Int64",
            "Utf8": f"{fw}.Utf8",
        }
        return dtype_map.get(dtype_str, f"{fw}.Utf8")

    def _get_agg_function(self, agg: str) -> str:
        """Get Polars aggregation function name."""
        agg_map = {
            "avg": "mean",
            "average": "mean",
            "concat": "str.concat",
        }
        return agg_map.get(agg, agg)

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
        # Alias the bare var_name so `last_node_var` (set by dispatch) still resolves.
        self._add_code(f"{var_name} = {default_var}")
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
        self._add_code(f"{var_name} = {split_vars[0]}")
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
        self._add_code(f"{var_name} = {pass_var}")
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

        self._add_code(f"{var_name} = ({left_df}.join(")
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
        self._add_code(f"def _polars_code_{var_name.replace('df_', '')}({params}):")

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
        self._add_code(f"{var_name} = _polars_code_{var_name.replace('df_', '')}({args})")
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
