import inspect
import typing
import polars as pl
from pl_fuzzy_frame_match.models import FuzzyMapping

from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
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
        super().__init__(
            f"Cannot generate code for node '{node_type}' (node_id={node_id}): {reason}"
        )


class FlowGraphToPolarsConverter:
    """
    Converts a FlowGraph into executable Polars code.

    This class takes a FlowGraph instance and generates standalone Python code
    that uses only Polars, without any Flowfile dependencies.
    """

    flow_graph: FlowGraph
    node_var_mapping: dict[int, str]
    imports: set[str]
    code_lines: list[str]
    output_nodes: list[tuple[int, str]] = []
    last_node_var: str | None = None
    unsupported_nodes: list[tuple[int, str, str]]  # List of (node_id, node_type, reason)
    custom_node_classes: dict[str, str]  # Maps custom node class name to source code

    def __init__(self, flow_graph: FlowGraph):
        self.flow_graph = flow_graph
        self.node_var_mapping: dict[int, str] = {}  # Maps node_id to variable name
        self.imports: set[str] = {"import polars as pl"}
        self.code_lines: list[str] = []
        self.output_nodes = []
        self.last_node_var = None
        self.unsupported_nodes = []
        self.custom_node_classes = {}

    def convert(self) -> str:
        """
        Main method to convert the FlowGraph to Polars code.

        Returns:
            str: Complete Python code that can be executed standalone

        Raises:
            UnsupportedNodeError: If the graph contains nodes that cannot be converted
                to standalone code (e.g., database nodes, explore_data, external_source).
        """
        # Get execution order
        execution_order = determine_execution_order(
            all_nodes=[node for node in self.flow_graph.nodes if node.is_correct],
            flow_starts=self.flow_graph._flow_starts + self.flow_graph.get_implicit_starter_nodes(),
        )

        # Generate code for each node in order
        for node in execution_order:
            self._generate_node_code(node)

        # Check for unsupported nodes and raise an error with all of them listed
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

        # Combine everything
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
        # Create variable name for this node's output
        var_name = f"df_{node.node_id}"
        self.node_var_mapping[node.node_id] = var_name
        self.handle_output_node(node, var_name)
        if node.node_template.output > 0:
            self.last_node_var = var_name
        # Get input variable names
        input_vars = self._get_input_vars(node)

        # Check if this is a user-defined node
        if isinstance(settings, input_schema.UserDefinedNode) or getattr(settings, "is_user_defined", False):
            self._handle_user_defined(node, var_name, input_vars)
            return

        # Route to appropriate handler based on node type
        handler = getattr(self, f"_handle_{node_type}", None)
        if handler:
            handler(settings, var_name, input_vars)
        else:
            # Unknown node type - add to unsupported list
            self.unsupported_nodes.append((
                node.node_id,
                node_type,
                f"No code generator implemented for node type '{node_type}'"
            ))
            self._add_comment(f"# WARNING: Cannot generate code for node type '{node_type}' (node_id={node.node_id})")
            self._add_comment(f"# This node type is not supported for code export")

    def _get_input_vars(self, node: FlowNode) -> dict[str, str]:
        """Get input variable names for a node."""
        input_vars = {}

        if node.node_inputs.main_inputs:
            if len(node.node_inputs.main_inputs) == 1:
                input_vars["main"] = self.node_var_mapping.get(node.node_inputs.main_inputs[0].node_id, "df")
            else:
                for i, input_node in enumerate(node.node_inputs.main_inputs):
                    input_vars[f"main_{i}"] = self.node_var_mapping.get(input_node.node_id, f"df_{i}")

        if node.node_inputs.left_input:
            input_vars["left"] = self.node_var_mapping.get(node.node_inputs.left_input.node_id, "df_left")

        if node.node_inputs.right_input:
            input_vars["right"] = self.node_var_mapping.get(node.node_inputs.right_input.node_id, "df_right")

        return input_vars

    def _handle_csv_read(self, file_settings: input_schema.ReceivedTable, var_name: str):
        if file_settings.table_settings.encoding.lower() in ("utf-8", "utf8"):
            encoding = "utf8-lossy"
            self._add_code(f"{var_name} = pl.scan_csv(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
            self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
            self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
            self._add_code(f'    encoding="{encoding}",')
            self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
            self._add_code(")")
        else:
            self._add_code(f"{var_name} = pl.read_csv(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            self._add_code(f'    separator="{file_settings.table_settings.delimiter}",')
            self._add_code(f"    has_header={file_settings.table_settings.has_headers},")
            self._add_code(f"    ignore_errors={file_settings.table_settings.ignore_errors},")
            if file_settings.table_settings.encoding:
                self._add_code(f'    encoding="{file_settings.table_settings.encoding}",')
            self._add_code(f"    skip_rows={file_settings.table_settings.starting_from_line},")
            self._add_code(").lazy()")

    def _handle_cloud_storage_reader(
        self, settings: input_schema.NodeCloudStorageReader, var_name: str, input_vars: dict[str, str]
    ):
        cloud_read_settings = settings.cloud_storage_settings
        self.imports.add("import flowfile as ff")
        if cloud_read_settings.file_format == "csv":
            self._add_code(f"{var_name} = ff.scan_csv_from_cloud_storage(")
            self._add_code(f'    "{cloud_read_settings.resource_path}",')
            self._add_code(f'    connection_name="{cloud_read_settings.connection_name}",')
            self._add_code(f'    scan_mode="{cloud_read_settings.scan_mode}",')
            self._add_code(f'    delimiter="{cloud_read_settings.csv_delimiter}",')
            self._add_code(f"    has_header={cloud_read_settings.csv_has_header},")
            self._add_code(f'    encoding="{cloud_read_settings.csv_encoding}",')

        elif cloud_read_settings.file_format == "parquet":
            self._add_code(f"{var_name} = ff.scan_parquet_from_cloud_storage(")
            self._add_code(f'    "{cloud_read_settings.resource_path}",')
            self._add_code(f'    connection_name="{cloud_read_settings.connection_name}",')
            self._add_code(f'    scan_mode="{cloud_read_settings.scan_mode}",')

        elif cloud_read_settings.file_format == "json":
            self._add_code(f"{var_name} = ff.scan_json_from_cloud_storage(")
            self._add_code(f'    "{cloud_read_settings.resource_path}",')
            self._add_code(f'    connection_name="{cloud_read_settings.connection_name}",')
            self._add_code(f'    scan_mode="{cloud_read_settings.scan_mode}",')

        elif cloud_read_settings.file_format == "delta":
            self._add_code(f"{var_name} = ff.scan_delta(")
            self._add_code(f'    "{cloud_read_settings.resource_path}",')
            self._add_code(f'    connection_name="{cloud_read_settings.connection_name}",')
            self._add_code(f'    scan_mode="{cloud_read_settings.scan_mode}",')
            self._add_code(f"    version_id={cloud_read_settings.delta_version},")
        else:
            return
        self._add_code(").data")

    def _handle_read(self, settings: input_schema.NodeRead, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle file reading nodes."""
        file_settings = settings.received_file

        if file_settings.file_type == "csv":
            self._handle_csv_read(file_settings, var_name)

        elif file_settings.file_type == "parquet":
            self._add_code(f'{var_name} = pl.scan_parquet("{file_settings.abs_file_path}")')

        elif file_settings.file_type in ("xlsx", "excel"):
            self._add_code(f"{var_name} = pl.read_excel(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            if file_settings.table_settings.sheet_name:
                self._add_code(f'    sheet_name="{file_settings.table_settings.sheet_name}",')
            self._add_code(").lazy()")

        self._add_code("")

    @staticmethod
    def _generate_pl_schema_with_typing(flowfile_schema: list[FlowfileColumn]) -> str:
        polars_schema_str = (
            "pl.Schema(["
            + ", ".join(
                f'("{flowfile_column.column_name}", pl.{flowfile_column.data_type})'
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
        self._add_code(f"{var_name} = pl.LazyFrame({data}, schema={schema}, strict=False)")
        self._add_code("")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle filter nodes."""
        input_df = input_vars.get("main", "df")

        if settings.filter_input.is_advanced():
            # Parse the advanced filter expression
            self.imports.add(
                "from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr"
            )
            self._add_code(f"{var_name} = {input_df}.filter(")
            self._add_code(f'simple_function_to_expr("{settings.filter_input.advanced_filter}")')
            self._add_code(")")
        else:
            # Handle basic filter
            basic = settings.filter_input.basic_filter
            if basic is not None:
                filter_expr = self._create_basic_filter_expr(basic)
                self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
            else:
                self._add_code(f"{var_name} = {input_df}  # No filter applied")
        self._add_code("")

    def _handle_record_count(self, settings: input_schema.NodeRecordCount, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        self._add_code(f"{var_name} = {input_df}.select(pl.len().alias('number_of_records'))")

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: dict[str, str]):
        input_df = input_vars.get("main", "df")
        from_col_name = settings.graph_solver_input.col_from
        to_col_name = settings.graph_solver_input.col_to
        output_col_name = settings.graph_solver_input.output_column_name
        self._add_code(
            f'{var_name} = {input_df}.with_columns(graph_solver(pl.col("{from_col_name}"), '
            f'pl.col("{to_col_name}"))'
            f'.alias("{output_col_name}"))'
        )
        self._add_code("")
        self.imports.add("from polars_grouper import graph_solver")

    def _handle_select(self, settings: input_schema.NodeSelect, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle select/rename nodes."""
        input_df = input_vars.get("main", "df")
        # Get columns to keep and renames
        select_exprs = []
        for select_input in settings.select_input:
            if select_input.keep and select_input.is_available:
                if select_input.old_name != select_input.new_name:
                    expr = f'pl.col("{select_input.old_name}").alias("{select_input.new_name}")'
                else:
                    expr = f'pl.col("{select_input.old_name}")'

                if (select_input.data_type_change or select_input.is_altered) and select_input.data_type:
                    polars_dtype = self._get_polars_dtype(select_input.data_type)
                    expr = f"{expr}.cast({polars_dtype})"

                select_exprs.append(expr)

        if select_exprs:
            self._add_code(f"{var_name} = {input_df}.select([")
            for expr in select_exprs:
                self._add_code(f"    {expr},")
            self._add_code("])")
        else:
            self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

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
        # Ensure left and right DataFrames are distinct
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
        # Get join keys
        left_on, right_on = self._get_join_keys(join_input_manager)

        # Apply pre-join transformations
        left_df, right_df = self._apply_pre_join_transformations(join_input_manager, left_df, right_df)
        # Handle join-specific key transformations
        left_on, right_on, reverse_action, after_join_drop_cols = self._handle_join_key_transformations(
            join_input_manager, left_df, right_df, left_on, right_on
        )
        # Execute the join
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
        # Calculate renames and drops
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

        # Apply transformations
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
        left_join_keys_to_keep = [jk.new_name for jk in settings.left_select.join_key_selects if jk.keep]
        join_key_duplication_command = [
            f'pl.col("{rjk.old_name}").alias("__DROP__{rjk.new_name}__DROP__")'
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
            f'pl.col("{ljk.new_name}").alias("__jk_{ljk.new_name}")'
            for ljk in settings.left_select.join_key_selects
            if ljk.keep
        ]

        # Update left_on keys
        for position, left_on_key in enumerate(left_on):
            left_on_select = settings.left_select.get_select_input_on_new_name(left_on_key)
            if left_on_select and left_on_select.keep:
                left_on[position] = f"__jk_{left_on_select.new_name}"

        if join_key_duplication_command:
            self._add_code(f"{left_df} = {left_df}.with_columns([{', '.join(join_key_duplication_command)}])")

        # Calculate columns to drop after join
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

        # Update right_on keys
        for position, right_on_key in enumerate(right_on):
            right_on_select = settings.right_select.get_select_input_on_new_name(right_on_key)
            if right_on_select and right_on_select.keep and right_on_select.new_name in left_join_keys:
                right_on[position] = f"__jk_{right_on_select.new_name}"

        if join_key_rename_command:
            self._add_code(f"{right_df} = {right_df}.rename({join_key_rename_command})")

        reverse_action = {f"__jk_{rjk.new_name}": rjk.new_name for rjk in join_keys_to_keep_and_rename}

        # Calculate columns to drop after join
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

        # Handle right join special case
        if settings.join_input.how == "right":
            self._add_code(".collect()")  # Right join needs to be collected first cause of issue with rename

        # Apply post-join transformations
        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")

        if reverse_action:
            self._add_code(f".rename({reverse_action})")

        # Convert back to lazy for right joins
        if settings.join_input.how == "right":
            self._add_code(".lazy()")

        self._add_code(")")

    def _handle_group_by(self, settings: input_schema.NodeGroupBy, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle group by nodes."""
        input_df = input_vars.get("main", "df")

        # Separate groupby columns from aggregation columns
        group_cols = []
        agg_exprs = []

        for agg_col in settings.groupby_input.agg_cols:
            if agg_col.agg == "groupby":
                group_cols.append(agg_col.old_name)
            else:
                agg_func = self._get_agg_function(agg_col.agg)
                expr = f'pl.col("{agg_col.old_name}").{agg_func}().alias("{agg_col.new_name}")'
                agg_exprs.append(expr)

        self._add_code(f"{var_name} = {input_df}.group_by({group_cols}).agg([")
        for expr in agg_exprs:
            self._add_code(f"    {expr},")
        self._add_code("])")
        self._add_code("")

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle formula/expression nodes."""
        input_df = input_vars.get("main", "df")
        self.imports.add("from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr")

        # Convert SQL-like formula to Polars expression
        formula = settings.function.function
        col_name = settings.function.field.name
        self._add_code(f"{var_name} = {input_df}.with_columns([")
        self._add_code(f'simple_function_to_expr({repr(formula)}).alias("{col_name}")')
        if settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
            if output_type[:3] != "pl.":
                output_type = "pl." + output_type
            self._add_code(f"    .cast({output_type})")

        self._add_code("])")
        self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input

        self._add_code(f"{var_name} = ({input_df}.collect()")
        self._add_code('    .with_columns(pl.lit(1).alias("__temp_index__"))')
        self._add_code("    .pivot(")
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code('        index=["__temp_index__"],')
        self._add_code(f'        columns="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("__temp_index__")')
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
            # Generate pivot code
            self._add_code(f"{var_name} = {input_df}.collect().pivot(")
            self._add_code(f"    values='{pivot_input.value_col}',")
            self._add_code(f"    index={pivot_input.index_columns},")
            self._add_code(f"    columns='{pivot_input.pivot_column}',")

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
        # Get all input LazyFrame
        dfs = []
        if "main" in input_vars:
            dfs.append(input_vars["main"])
        else:
            # Multiple main inputs
            for key, df_var in input_vars.items():
                if key.startswith("main"):
                    dfs.append(df_var)

        if settings.union_input.mode == "relaxed":
            how = "diagonal_relaxed"
        else:
            how = "diagonal"

        self._add_code(f"{var_name} = pl.concat([")
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

    @staticmethod
    def _transform_fuzzy_mappings_to_string(fuzzy_mappings: list[FuzzyMapping]) -> str:
        output_str = "["
        for i, fuzzy_mapping in enumerate(fuzzy_mappings):
            output_str += (
                f"FuzzyMapping(left_col='{fuzzy_mapping.left_col}',"
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
            self._add_code(
                f"{left_df} = {left_df}.drop({[c.old_name for c in fuzzy_match_handler.left_select.non_jk_drop_columns]})"
            )
        if fuzzy_match_handler.right_select.has_drop_cols():
            self._add_code(
                f"{right_df} = {right_df}.drop({[c.old_name for c in fuzzy_match_handler.right_select.non_jk_drop_columns]})"
            )

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
                f"{var_name} = {input_df}.unique(subset={settings.unique_input.columns}, keep='{settings.unique_input.strategy}')"
            )
        else:
            self._add_code(f"{var_name} = {input_df}.unique(keep='{settings.unique_input.strategy}')")
        self._add_code("")

    def _handle_text_to_rows(
        self, settings: input_schema.NodeTextToRows, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle text to rows (explode) nodes."""
        input_df = input_vars.get("main", "df")
        text_input = settings.text_to_rows_input

        # First split the column
        split_expr = f'pl.col("{text_input.column_to_split}").str.split("{text_input.split_fixed_value}")'
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
            # Row number within groups
            self._add_code(f"{var_name} = ({input_df}")
            self._add_code(f"    .with_columns(pl.lit(1).alias('{record_input.output_column_name}'))")
            self._add_code("    .with_columns([")
            self._add_code(
                f"    (pl.cum_count('{record_input.output_column_name}').over({record_input.group_by_columns}) + {record_input.offset} - 1)"
            )
            self._add_code(f"    .alias('{record_input.output_column_name}')")
            self._add_code("])")
            self._add_code(
                f".select(['{record_input.output_column_name}'] + [col for col in {input_df}.columns if col != '{record_input.output_column_name}'])"
            )
            self._add_code(")")
        else:
            # Simple row number
            self._add_code(
                f"{var_name} = {input_df}.with_row_count(name='{record_input.output_column_name}', offset={record_input.offset})"
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

    def _handle_cloud_storage_writer(
        self, settings: input_schema.NodeCloudStorageWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle cloud storage writer nodes."""
        input_df = input_vars.get("main", "df")
        # def write_csv_to_cloud_storage(self, path: str, connection_name: typing.Optional[str] = None, delimiter: str = ';', encoding: typing.Literal['utf8', 'utf8-lossy'] = 'utf8', description: Optional[str] = None) -> 'FlowFrame': ...

        output_settings = settings.cloud_storage_settings
        self.imports.add("import flowfile as ff")
        self._add_code(f"(ff.FlowFrame({input_df})")
        if output_settings.file_format == "csv":
            self._add_code("    .write_csv_to_cloud_storage(")
            self._add_code(f'        path="{output_settings.resource_path}",')
            self._add_code(f'        connection_name="{output_settings.connection_name}",')
            self._add_code(f'        delimiter="{output_settings.csv_delimiter}",')
            self._add_code(f'        encoding="{output_settings.csv_encoding}",')
            self._add_code(f'        description="{settings.description}"')
        elif output_settings.file_format == "parquet":
            self._add_code("    .write_parquet_to_cloud_storage(")
            self._add_code(f'        path="{output_settings.resource_path}",')
            self._add_code(f'        connection_name="{output_settings.connection_name}",')
            self._add_code(f'        description="{settings.description}"')
        elif output_settings.file_format == "json":
            self._add_code("    .write_json_to_cloud_storage(")
            self._add_code(f'        path="{output_settings.resource_path}",')
            self._add_code(f'        connection_name="{output_settings.connection_name}",')
            self._add_code(f'        description="{settings.description}"')
        elif output_settings.file_format == "delta":
            self._add_code("    .write_delta(")
            self._add_code(f'        path="{output_settings.resource_path}",')
            self._add_code(f'        write_mode="{output_settings.write_mode}",')
            self._add_code(f'        connection_name="{output_settings.connection_name}",')
            self._add_code(f'        description="{settings.description}"')
        self._add_code("    )")
        self._add_code(")")

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle output nodes."""
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
            self._add_code(f"{input_df}.collect().write_excel(")
            self._add_code(f'    "{output_settings.abs_file_path}",')
            self._add_code(f'    worksheet="{output_settings.table_settings.sheet_name}"')
            self._add_code(")")

        self._add_code("")

    def _handle_polars_code(
        self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle custom Polars code nodes."""
        code = settings.polars_code_input.polars_code.strip()
        # Determine function parameters based on number of inputs
        if len(input_vars) == 0:
            params = ""
            args = ""
        elif len(input_vars) == 1:
            params = "input_df: pl.LazyFrame"
            input_df = list(input_vars.values())[0]
            args = input_df
        else:
            # Multiple inputs
            param_list = []
            arg_list = []
            i = 1
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    param_list.append(f"input_df_{i}: pl.LazyFrame")
                    arg_list.append(input_vars[key])
                    i += 1
            params = ", ".join(param_list)
            args = ", ".join(arg_list)

        # Check if the code is just an expression (no assignment)
        is_expression = "output_df" not in code

        # Wrap the code in a function
        self._add_code("# Custom Polars code")
        self._add_code(f"def _polars_code_{var_name.replace('df_', '')}({params}):")

        # Handle the code based on its structure
        if is_expression:
            # It's just an expression, return it directly
            self._add_code(f"    return {code}")
        else:
            # It contains assignments
            for line in code.split("\n"):
                if line.strip():
                    self._add_code(f"    {line}")

            # If no explicit return, try to detect what to return
            if "return" not in code:
                # Try to find the last assignment
                lines = [l.strip() for l in code.split("\n") if l.strip() and "=" in l]
                if lines:
                    last_assignment = lines[-1]
                    if "=" in last_assignment:
                        output_var = last_assignment.split("=")[0].strip()
                        self._add_code(f"    return {output_var}")

        self._add_code("")

        # Call the function
        self._add_code(f"{var_name} = _polars_code_{var_name.replace('df_', '')}({args})")
        self._add_code("")

    # Handlers for unsupported node types - these add nodes to the unsupported list

    def _handle_explore_data(
        self, settings: input_schema.NodeExploreData, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle explore_data nodes - these are skipped as they are interactive visualization only."""
        # explore_data is just for visualization in the UI, it doesn't transform data
        # So we skip it in code generation but don't fail - just add a comment
        input_df = input_vars.get("main", "df")
        self._add_comment(f"# Node {settings.node_id}: Explore Data (skipped - interactive visualization only)")
        self._add_code(f"{var_name} = {input_df}  # Pass through unchanged")
        self._add_code("")

    def _handle_database_reader(
        self, settings: input_schema.NodeDatabaseReader, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle database_reader nodes by generating code to read from database using a named connection."""
        db_settings = settings.database_settings

        # Only reference mode is supported for code generation
        if db_settings.connection_mode != "reference":
            self.unsupported_nodes.append((
                settings.node_id,
                "database_reader",
                "Database Reader nodes with inline connections cannot be exported. "
                "Please use a named connection (reference mode) instead."
            ))
            self._add_comment(f"# Node {settings.node_id}: Database Reader - Inline connections not supported")
            return

        if not db_settings.database_connection_name:
            self.unsupported_nodes.append((
                settings.node_id,
                "database_reader",
                "Database Reader node is missing a connection name"
            ))
            return

        self.imports.add("import flowfile as ff")

        connection_name = db_settings.database_connection_name
        self._add_code(f"# Read from database using connection: {connection_name}")

        if db_settings.query_mode == "query" and db_settings.query:
            # Query mode - use triple quotes to preserve query formatting
            self._add_code(f'{var_name} = ff.read_database(')
            self._add_code(f'    "{connection_name}",')
            self._add_code(f'    query="""')
            # Add each line of the query with proper indentation
            for line in db_settings.query.split("\n"):
                self._add_code(f"        {line}")
            self._add_code('    """,')
            self._add_code(")")
        else:
            # Table mode
            self._add_code(f'{var_name} = ff.read_database(')
            self._add_code(f'    "{connection_name}",')
            if db_settings.table_name:
                self._add_code(f'    table_name="{db_settings.table_name}",')
            if db_settings.schema_name:
                self._add_code(f'    schema_name="{db_settings.schema_name}",')
            self._add_code(")")

        self._add_code("")

    def _handle_database_writer(
        self, settings: input_schema.NodeDatabaseWriter, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle database_writer nodes by generating code to write to database using a named connection."""
        db_settings = settings.database_write_settings

        # Only reference mode is supported for code generation
        if db_settings.connection_mode != "reference":
            self.unsupported_nodes.append((
                settings.node_id,
                "database_writer",
                "Database Writer nodes with inline connections cannot be exported. "
                "Please use a named connection (reference mode) instead."
            ))
            self._add_comment(f"# Node {settings.node_id}: Database Writer - Inline connections not supported")
            return

        if not db_settings.database_connection_name:
            self.unsupported_nodes.append((
                settings.node_id,
                "database_writer",
                "Database Writer node is missing a connection name"
            ))
            return

        self.imports.add("import flowfile as ff")

        connection_name = db_settings.database_connection_name
        input_df = input_vars.get("main", "df")

        self._add_code(f"# Write to database using connection: {connection_name}")
        self._add_code(f"ff.write_database(")
        self._add_code(f"    {input_df}.collect(),")
        self._add_code(f'    "{connection_name}",')
        self._add_code(f'    "{db_settings.table_name}",')
        if db_settings.schema_name:
            self._add_code(f'    schema_name="{db_settings.schema_name}",')
        if db_settings.if_exists:
            self._add_code(f'    if_exists="{db_settings.if_exists}",')
        self._add_code(")")
        self._add_code(f"{var_name} = {input_df}  # Pass through the input DataFrame")
        self._add_code("")

    def _handle_external_source(
        self, settings: input_schema.NodeExternalSource, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle external_source nodes - these are not supported for code generation."""
        self.unsupported_nodes.append((
            settings.node_id,
            "external_source",
            "External Source nodes use dynamic data sources that cannot be included in generated code"
        ))
        self._add_comment(f"# Node {settings.node_id}: External Source - Not supported for code export")
        self._add_comment("# (External data sources require runtime configuration)")

    def _check_process_method_signature(self, custom_node_class: type) -> tuple[bool, bool]:
        """
        Check the process method signature to determine if collect/lazy is needed.

        Returns:
            Tuple of (needs_collect, needs_lazy):
            - needs_collect: True if inputs need to be collected to DataFrame before passing to process()
            - needs_lazy: True if output needs to be converted to LazyFrame after process()
        """
        needs_collect = True  # Default: assume needs DataFrame input
        needs_lazy = True  # Default: assume returns DataFrame

        process_method = getattr(custom_node_class, 'process', None)
        if process_method is None:
            return needs_collect, needs_lazy

        try:
            # Try to get type hints from the process method
            type_hints = typing.get_type_hints(process_method)

            # Check return type
            return_type = type_hints.get('return')
            if return_type is not None:
                return_type_str = str(return_type)
                if 'LazyFrame' in return_type_str:
                    needs_lazy = False

            # Check input parameter types (look for *inputs parameter or first param after self)
            sig = inspect.signature(process_method)
            params = list(sig.parameters.values())
            for param in params[1:]:  # Skip 'self'
                if param.annotation != inspect.Parameter.empty:
                    param_type_str = str(param.annotation)
                    if 'LazyFrame' in param_type_str:
                        needs_collect = False
                        break
                # Also check the type_hints dict for this param
                if param.name in type_hints:
                    hint_str = str(type_hints[param.name])
                    if 'LazyFrame' in hint_str:
                        needs_collect = False
                        break
        except Exception as e:
            # If we can't determine types, use defaults (collect + lazy)
            logger.debug(f"Could not determine process method signature: {e}")

        return needs_collect, needs_lazy

    def _generate_component_code(self, component: object) -> str:
        """Generate Python code to recreate a UI component."""
        component_type = type(component).__name__

        # Get the component's attributes
        if hasattr(component, 'model_dump'):
            # Pydantic model
            data = component.model_dump(exclude_none=True, exclude={'component_type', 'value'})
        else:
            return f"{component_type}()"

        # Build the constructor arguments
        args = []
        for key, val in data.items():
            if key in ('data_types_filter',):  # Skip computed fields
                continue
            if isinstance(val, str):
                args.append(f'{key}="{val}"')
            elif isinstance(val, bool):
                args.append(f'{key}={val}')
            elif isinstance(val, (int, float)):
                args.append(f'{key}={val}')
            elif isinstance(val, list):
                args.append(f'{key}={repr(val)}')
            elif val is None:
                continue
            else:
                args.append(f'{key}={repr(val)}')

        return f"{component_type}({', '.join(args)})"

    def _generate_section_code(self, section: object, section_name: str) -> list[str]:
        """Generate Python code to recreate a Section with its components."""
        lines = []

        # Get section attributes
        title = getattr(section, 'title', None)
        description = getattr(section, 'description', None)
        hidden = getattr(section, 'hidden', False)

        # Start section definition
        section_args = []
        if title:
            section_args.append(f'title="{title}"')
        if description:
            section_args.append(f'description="{description}"')
        if hidden:
            section_args.append(f'hidden={hidden}')

        # Get components from the section
        components = {}
        extra = getattr(section, '__pydantic_extra__', {}) or {}
        for key, val in extra.items():
            if hasattr(val, 'component_type'):
                components[key] = val

        # Also check model fields
        if hasattr(section, 'model_fields'):
            for field_name in section.model_fields:
                if field_name not in ('title', 'description', 'hidden'):
                    val = getattr(section, field_name, None)
                    if val is not None and hasattr(val, 'component_type'):
                        components[field_name] = val

        # Add component arguments
        for comp_name, comp in components.items():
            comp_code = self._generate_component_code(comp)
            section_args.append(f'{comp_name}={comp_code}')

        lines.append(f'{section_name} = Section({", ".join(section_args)})')
        return lines

    def _generate_settings_schema_code(self, settings_schema: object, class_name: str) -> str | None:
        """
        Generate Python code to recreate a settings schema class from its runtime structure.

        Returns:
            Generated Python code for the settings class, or None if not possible.
        """
        if settings_schema is None:
            return None

        # Get the actual class name
        actual_class_name = type(settings_schema).__name__
        if actual_class_name == 'NodeSettings':
            return None

        lines = []
        lines.append(f"class {class_name}(NodeSettings):")

        # Get sections from extra fields and model fields
        sections = {}

        # Check __pydantic_extra__ for dynamically added sections
        extra = getattr(settings_schema, '__pydantic_extra__', {}) or {}
        for key, val in extra.items():
            if hasattr(val, 'get_components'):  # It's a Section
                sections[key] = val

        # Also check model fields
        if hasattr(settings_schema, 'model_fields'):
            for field_name in settings_schema.model_fields:
                val = getattr(settings_schema, field_name, None)
                if val is not None and hasattr(val, 'get_components'):  # It's a Section
                    sections[field_name] = val

        if not sections:
            lines.append("    pass")
        else:
            # Generate code for each section
            for section_name, section in sections.items():
                section_lines = self._generate_section_code(section, section_name)
                for line in section_lines:
                    lines.append(f"    {line}")

        return "\n".join(lines)

    def _read_custom_node_source_file(self, custom_node_class: type) -> str | None:
        """
        Read the entire source file where a custom node class is defined.
        This includes all class definitions in that file (settings schemas, etc.).

        Returns:
            The complete source code from the file, or None if not readable.
        """
        try:
            source_file = inspect.getfile(custom_node_class)
            with open(source_file, 'r') as f:
                return f.read()
        except (OSError, TypeError):
            return None

    def _extract_class_from_source(self, source_code: str, class_name: str) -> str | None:
        """
        Extract a specific class definition from source code using AST.

        Args:
            source_code: The full source code to search in
            class_name: The name of the class to extract

        Returns:
            The class definition source, or None if not found.
        """
        import ast

        try:
            tree = ast.parse(source_code)
        except SyntaxError:
            return None

        # Find all class definitions and their line numbers
        class_locations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_locations.append((node.name, node.lineno, node.end_lineno))

        # Find the target class
        target_class = None
        for name, start_line, end_line in class_locations:
            if name == class_name:
                target_class = (start_line, end_line)
                break

        if target_class is None:
            return None

        # Extract the lines for this class
        lines = source_code.split('\n')
        start_line, end_line = target_class
        # AST line numbers are 1-indexed
        class_lines = lines[start_line - 1:end_line]
        return '\n'.join(class_lines)

    def _extract_settings_schema_class(self, custom_node_class: type) -> str | None:
        """
        Try to extract or generate the settings schema class source code for a custom node.

        Attempts multiple strategies:
        1. Read from the source file where the custom node is defined
        2. Use inspect.getsource() directly on the settings class
        3. Generate code from the runtime structure

        Returns:
            Source code of the settings schema class, or None if not extractable.
        """
        # Get the settings_schema field from the custom node class
        settings_schema = getattr(custom_node_class, 'settings_schema', None)
        if settings_schema is None:
            return None

        # Get the class of the settings schema instance
        settings_class = type(settings_schema)

        # Skip if it's the base NodeSettings class
        if settings_class.__name__ == 'NodeSettings':
            return None

        settings_class_name = settings_class.__name__

        # Strategy 1: Try to read from the same file as the custom node class
        full_source = self._read_custom_node_source_file(custom_node_class)
        if full_source:
            extracted = self._extract_class_from_source(full_source, settings_class_name)
            if extracted:
                return extracted

        # Strategy 2: Try to get the source code directly
        try:
            return inspect.getsource(settings_class)
        except (OSError, TypeError):
            pass

        # Strategy 3: Generate code from the runtime structure
        return self._generate_settings_schema_code(settings_schema, settings_class_name)

    def _handle_user_defined(
        self, node: FlowNode, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Handle user-defined custom nodes by including their class definition and calling process()."""
        node_type = node.node_type
        settings = node.setting_input

        # Get the custom node class from the registry
        custom_node_class = CUSTOM_NODE_STORE.get(node_type)
        if custom_node_class is None:
            self.unsupported_nodes.append((
                node.node_id,
                node_type,
                f"User-defined node type '{node_type}' not found in the custom node registry"
            ))
            self._add_comment(f"# Node {node.node_id}: User-defined node '{node_type}' - Not found in registry")
            return

        # Get the source code of the custom node class
        try:
            source_code = inspect.getsource(custom_node_class)
        except (OSError, TypeError) as e:
            self.unsupported_nodes.append((
                node.node_id,
                node_type,
                f"Could not retrieve source code for user-defined node: {e}"
            ))
            self._add_comment(f"# Node {node.node_id}: User-defined node '{node_type}' - Source code unavailable")
            return

        # Store the class source if we haven't already
        class_name = custom_node_class.__name__
        if class_name not in self.custom_node_classes:
            self.custom_node_classes[class_name] = source_code
            # Add necessary imports for the custom node base classes and UI components
            self.imports.add("from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, NodeSettings, Section")
            self.imports.add("from flowfile_core.flowfile.node_designer.ui_components import FlowfileInComponent, ColumnSelector, NumericInput, TextInput, DropdownSelector, TextArea, Toggle, IncomingColumns")
            self.imports.add("from pydantic import BaseModel")
            self.imports.add("from typing import Any")

            # Try to extract the settings schema class source code
            settings_schema_source = self._extract_settings_schema_class(custom_node_class)
            if settings_schema_source:
                # Store the settings schema class (use a special prefix to ensure it comes before the node class)
                settings_class = type(getattr(custom_node_class, 'settings_schema', None))
                if settings_class and settings_class.__name__ != 'NodeSettings':
                    self.custom_node_classes[f"__{settings_class.__name__}"] = settings_schema_source

        # Get settings values to initialize the node
        settings_dict = getattr(settings, "settings", {}) or {}

        # Check process method signature to determine if collect/lazy is needed
        needs_collect, needs_lazy = self._check_process_method_signature(custom_node_class)

        # Generate the code to instantiate and run the custom node
        self._add_code(f"# User-defined node: {custom_node_class.model_fields.get('node_name', type('', (), {'default': node_type})).default}")
        self._add_code(f"_custom_node_{node.node_id} = {class_name}()")

        # If there are settings, apply them
        if settings_dict:
            self._add_code(f"_custom_node_{node.node_id}_settings = {repr(settings_dict)}")
            self._add_code(f"if _custom_node_{node.node_id}.settings_schema:")
            self._add_code(f"    _custom_node_{node.node_id}.settings_schema.populate_values(_custom_node_{node.node_id}_settings)")

        # Prepare input arguments based on whether we need to collect
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

        # Call the process method, adding .lazy() only if needed
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
        # This is a simplified parser - you'd need more sophisticated parsing
        # Handle patterns like [column]>value or [column]="value"

        import re

        # Pattern: [column_name]operator"value" or [column_name]operatorvalue
        pattern = r'\[([^\]]+)\]([><=!]+)"?([^"]*)"?'

        def replace_expr(match):
            col, op, val = match.groups()

            # Map operators
            op_map = {"=": "==", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}

            polars_op = op_map.get(op, op)

            # Check if value is numeric
            try:
                float(val)
                return f'pl.col("{col}") {polars_op} {val}'
            except ValueError:
                return f'pl.col("{col}") {polars_op} "{val}"'

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

        col = f'pl.col("{basic.field}")'
        value = basic.value
        value2 = basic.value2

        # Determine if value is numeric (for proper quoting)
        is_numeric = value.replace(".", "", 1).replace("-", "", 1).isnumeric() if value else False

        # Get the operator
        try:
            operator = basic.get_operator()
        except (ValueError, AttributeError):
            operator = FilterOperator.from_symbol(str(basic.operator))

        # Generate expression based on operator
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

        # Fallback
        return col

    def _get_polars_dtype(self, dtype_str: str) -> str:
        """Convert Flowfile dtype string to Polars dtype."""
        dtype_map = {
            "String": "pl.Utf8",
            "Integer": "pl.Int64",
            "Double": "pl.Float64",
            "Boolean": "pl.Boolean",
            "Date": "pl.Date",
            "Datetime": "pl.Datetime",
            "Float32": "pl.Float32",
            "Float64": "pl.Float64",
            "Int32": "pl.Int32",
            "Int64": "pl.Int64",
            "Utf8": "pl.Utf8",
        }
        return dtype_map.get(dtype_str, "pl.Utf8")

    def _get_agg_function(self, agg: str) -> str:
        """Get Polars aggregation function name."""
        agg_map = {
            "avg": "mean",
            "average": "mean",
            "concat": "str.concat",
        }
        return agg_map.get(agg, agg)

    def _sql_to_polars_expr(self, sql_expr: str) -> str:
        """Convert SQL-like expression to Polars expression."""
        # This is a very simplified converter
        # In practice, you'd want a proper SQL parser

        # Replace column references
        import re

        # Pattern for column names (simplified)
        col_pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b"

        def replace_col(match):
            col_name = match.group(1)
            # Skip SQL keywords
            keywords = {"CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN", "AS"}
            if col_name.upper() in keywords:
                return col_name
            return f'pl.col("{col_name}")'

        result = re.sub(col_pattern, replace_col, sql_expr)

        # Handle CASE WHEN
        if "CASE" in result:
            # This would need proper parsing
            result = "pl.when(...).then(...).otherwise(...)"

        return result

    def add_return_code(self, lines: list[str]) -> None:
        if self.output_nodes:
            # Return marked output nodes
            if len(self.output_nodes) == 1:
                # Single output
                _, var_name = self.output_nodes[0]
                lines.append(f"    return {var_name}")
            else:
                # Multiple outputs - return as dictionary
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

        # Add imports
        lines.extend(sorted(self.imports))
        lines.append("")
        lines.append("")

        # Add custom node class definitions if any
        if self.custom_node_classes:
            lines.append("# Custom Node Class Definitions")
            lines.append("# These classes are user-defined nodes that were included in the flow")
            lines.append("")
            # Sort classes so settings schema classes (prefixed with "__") come first
            sorted_classes = sorted(
                self.custom_node_classes.items(),
                key=lambda x: (0 if x[0].startswith("__") else 1, x[0])
            )
            for class_name, source_code in sorted_classes:
                # Remove the "__" prefix from settings schema class names in the output
                # (the prefix was just for sorting purposes)
                # Add the source code for each custom node class
                for source_line in source_code.split("\n"):
                    lines.append(source_line)
                lines.append("")
            lines.append("")

        # Add main function
        lines.append("def run_etl_pipeline():")
        lines.append('    """')
        lines.append(f"    ETL Pipeline: {self.flow_graph.__name__}")
        lines.append("    Generated from Flowfile")
        lines.append('    """')
        lines.append("    ")

        # Add the generated code
        for line in self.code_lines:
            if line:
                lines.append(f"    {line}")
            else:
                lines.append("")
        # Add main block
        lines.append("")
        self.add_return_code(lines)
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    pipeline_output = run_etl_pipeline()")

        return "\n".join(lines)


# Example usage function
def export_flow_to_polars(flow_graph: FlowGraph) -> str:
    """
    Export a FlowGraph to standalone Polars code.

    Args:
        flow_graph: The FlowGraph instance to convert

    Returns:
        str: Python code that can be executed standalone
    """
    converter = FlowGraphToPolarsConverter(flow_graph)
    return converter.convert()
