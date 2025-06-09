from typing import List, Dict, Optional, Set, Tuple, Any
from collections import defaultdict
import polars as pl

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.execution_orderer import determine_execution_order
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.configs import logger


class FlowGraphToPolarsConverter:
    """
    Converts a FlowGraph into executable Polars code.

    This class takes a FlowGraph instance and generates standalone Python code
    that uses only Polars, without any Flowfile dependencies.
    """
    flow_graph: FlowGraph
    node_var_mapping: Dict[int, str]
    imports: Set[str]
    code_lines: List[str]
    output_nodes: List[Tuple[int, str]] = []
    last_node_var: Optional[str] = None

    def __init__(self, flow_graph: FlowGraph):
        self.flow_graph = flow_graph
        self.node_var_mapping: Dict[int, str] = {}  # Maps node_id to variable name
        self.imports: Set[str] = {"import polars as pl"}
        self.code_lines: List[str] = []
        self.output_nodes = []
        self.last_node_var = None

    def convert(self) -> str:
        """
        Main method to convert the FlowGraph to Polars code.

        Returns:
            str: Complete Python code that can be executed standalone
        """
        # Get execution order
        execution_order = determine_execution_order(
            all_nodes=[node for node in self.flow_graph.nodes if node.is_correct],
            flow_starts=self.flow_graph._flow_starts + self.flow_graph.get_implicit_starter_nodes()
        )

        # Generate code for each node in order
        for node in execution_order:
            self._generate_node_code(node)

        # Combine everything
        return self._build_final_code()

    def handle_output_node(self, node: FlowNode, var_name: str) -> None:
        settings = node.setting_input
        if hasattr(settings, 'is_flow_output') and settings.is_flow_output:
            self.output_nodes.append((node.node_id, var_name))

    def _generate_node_code(self, node: FlowNode) -> None:
        """Generate Polars code for a specific node."""
        node_type = node.node_type
        settings = node.setting_input
        # Skip placeholder nodes
        if isinstance(settings, input_schema.NodePromise):
            self._add_comment(f"# Skipping uninitialized node: {node.node_id}")
            return
        # Create variable name for this node's output
        var_name = f"df_{node.node_id}"
        self.node_var_mapping[node.node_id] = var_name
        self.handle_output_node(node, var_name)
        if node.node_template.output>0:
            self.last_node_var = var_name
        # Get input variable names
        input_vars = self._get_input_vars(node)
        # Route to appropriate handler based on node type
        handler = getattr(self, f"_handle_{node_type}", None)
        if handler:
            handler(settings, var_name, input_vars)
        else:
            self._add_comment(f"# TODO: Implement handler for node type: {node_type}")
            raise Exception(f"No handler implemented for node type: {node_type}")

    def _get_input_vars(self, node: FlowNode) -> Dict[str, str]:
        """Get input variable names for a node."""
        input_vars = {}

        if node.node_inputs.main_inputs:
            if len(node.node_inputs.main_inputs) == 1:
                input_vars['main'] = self.node_var_mapping.get(
                    node.node_inputs.main_inputs[0].node_id, 'df'
                )
            else:
                for i, input_node in enumerate(node.node_inputs.main_inputs):
                    input_vars[f'main_{i}'] = self.node_var_mapping.get(
                        input_node.node_id, f'df_{i}'
                    )

        if node.node_inputs.left_input:
            input_vars['left'] = self.node_var_mapping.get(
                node.node_inputs.left_input.node_id, 'df_left'
            )

        if node.node_inputs.right_input:
            input_vars['right'] = self.node_var_mapping.get(
                node.node_inputs.right_input.node_id, 'df_right'
            )

        return input_vars

    def _handle_csv_read(self, file_settings: input_schema.ReceivedTable, var_name: str):
        if file_settings.encoding.lower() in ('utf-8', 'utf8'):
            encoding = "utf8-lossy"
            self._add_code(f"{var_name} = pl.scan_csv(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            self._add_code(f'    separator="{file_settings.delimiter}",')
            self._add_code(f'    has_header={file_settings.has_headers},')
            self._add_code(f'    ignore_errors={file_settings.ignore_errors},')
            self._add_code(f'    encoding="{encoding}",')
            self._add_code(f'    skip_rows={file_settings.starting_from_line},')
            self._add_code(")")
        else:
            self._add_code(f"{var_name} = pl.read_csv(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            self._add_code(f'    separator="{file_settings.delimiter}",')
            self._add_code(f'    has_header={file_settings.has_headers},')
            self._add_code(f'    ignore_errors={file_settings.ignore_errors},')
            if file_settings.encoding:
                self._add_code(f'    encoding="{file_settings.encoding}",')
            self._add_code(f'    skip_rows={file_settings.starting_from_line},')
            self._add_code(").lazy()")

    def _handle_read(self, settings: input_schema.NodeRead, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle file reading nodes."""
        file_settings = settings.received_file

        if file_settings.file_type == 'csv':
            self._handle_csv_read(file_settings, var_name)

        elif file_settings.file_type == 'parquet':
            self._add_code(f'{var_name} = pl.scan_parquet("{file_settings.abs_file_path}")')

        elif file_settings.file_type in ('xlsx', 'excel'):
            self._add_code(f"{var_name} = pl.read_excel(")
            self._add_code(f'    "{file_settings.abs_file_path}",')
            if file_settings.sheet_name:
                self._add_code(f'    sheet_name="{file_settings.sheet_name}",')
            self._add_code(").lazy()")

        self._add_code("")

    @staticmethod
    def _generate_pl_schema_with_typing(flowfile_schema: List[FlowfileColumn]) -> str:
        polars_schema_str = "pl.Schema([" + ", ".join(f'("{flowfile_column.column_name}", pl.{flowfile_column.data_type})'
                                   for flowfile_column in flowfile_schema) + "])"
        return polars_schema_str

    def get_manual_schema_input(self, flowfile_schema: List[FlowfileColumn]) -> str:
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

    def _handle_manual_input(self, settings: input_schema.NodeManualInput, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle manual data input nodes."""
        if settings.raw_data_format:
            data = settings.raw_data_format.data
            flowfile_schema = list(FlowfileColumn.create_from_minimal_field_info(c) for c in settings.raw_data_format.columns)
            schema = self.get_manual_schema_input(flowfile_schema)
            self._add_code(f"{var_name} = pl.LazyFrame({data}, schema={schema}, strict=False)")
        else:
            self._add_code(f"{var_name} = pl.LazyFrame({settings.raw_data})")
        self._add_code("")

    def _handle_filter(self, settings: input_schema.NodeFilter, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle filter nodes."""
        input_df = input_vars.get('main', 'df')

        if settings.filter_input.filter_type == 'advanced':
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
            filter_expr = self._create_basic_filter_expr(basic)
            self._add_code(f"{var_name} = {input_df}.filter({filter_expr})")
        self._add_code("")

    def _handle_record_count(self, settings: input_schema.NodeRecordCount, var_name: str, input_vars: Dict[str, str]):
        input_df = input_vars.get('main', 'df')
        self._add_code(f"{var_name} = {input_df}.select(pl.len().alias('number_of_records'))")

    def _handle_graph_solver(self, settings: input_schema.NodeGraphSolver, var_name: str, input_vars: Dict[str, str]):
        input_df = input_vars.get('main', 'df')
        from_col_name = settings.graph_solver_input.col_from
        to_col_name = settings.graph_solver_input.col_to
        output_col_name = settings.graph_solver_input.output_column_name
        self._add_code(f'{var_name} = {input_df}.with_columns(graph_solver(pl.col("{from_col_name}"), '
                       f'pl.col("{to_col_name}"))'
                       f'.alias("{output_col_name}"))')
        self._add_code("")
        self.imports.add("from polars_grouper import graph_solver")

    def _handle_select(self, settings: input_schema.NodeSelect, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle select/rename nodes."""
        input_df = input_vars.get('main', 'df')
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
                    expr = f'{expr}.cast({polars_dtype})'

                select_exprs.append(expr)

        if select_exprs:
            self._add_code(f"{var_name} = {input_df}.select([")
            for expr in select_exprs:
                self._add_code(f"    {expr},")
            self._add_code("])")
        else:
            self._add_code(f"{var_name} = {input_df}")
        self._add_code("")

    def _handle_join(self, settings: input_schema.NodeJoin, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle join nodes."""
        left_df = input_vars.get('main', input_vars.get('main_0', 'df_left'))
        right_df = input_vars.get('right', input_vars.get('main_1', 'df_right'))

        # Extract join keys
        left_on = [jm.left_col for jm in settings.join_input.join_mapping]
        right_on = [jm.right_col for jm in settings.join_input.join_mapping]

        self._add_code(f"{var_name} = {left_df}.join(")
        self._add_code(f"    {right_df},")
        self._add_code(f"    left_on={left_on},")
        self._add_code(f"    right_on={right_on},")
        self._add_code(f'    how="{settings.join_input.how}"')
        self._add_code(")")
        self._add_code("")

    def _handle_group_by(self, settings: input_schema.NodeGroupBy, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle group by nodes."""
        input_df = input_vars.get('main', 'df')

        # Separate groupby columns from aggregation columns
        group_cols = []
        agg_exprs = []

        for agg_col in settings.groupby_input.agg_cols:
            if agg_col.agg == 'groupby':
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

    def _handle_formula(self, settings: input_schema.NodeFormula, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle formula/expression nodes."""
        input_df = input_vars.get('main', 'df')
        self.imports.add("from polars_expr_transformer.process.polars_expr_transformer import simple_function_to_expr")

        # Convert SQL-like formula to Polars expression
        formula = settings.function.function
        col_name = settings.function.field.name
        self._add_code(f"{var_name} = {input_df}.with_columns([")
        self._add_code(f'simple_function_to_expr({repr(formula)}).alias("{col_name}")')
        if settings.function.field.data_type not in (None, "Auto"):
            output_type = convert_pl_type_to_string(cast_str_to_polars_type(settings.function.field.data_type))
            if output_type[:3] != "pl.":
                output_type = "pl." + output_type
            self._add_code(f'    .cast({output_type})')

        self._add_code("])")
        self._add_code("")

    def _handle_pivot_no_index(self, settings: input_schema.NodePivot, var_name: str, input_df: str, agg_func: str):
        pivot_input = settings.pivot_input

        self._add_code(f'{var_name} = ({input_df}.collect()')
        self._add_code('    .with_columns(pl.lit(1).alias("__temp_index__"))')
        self._add_code('    .pivot(')
        self._add_code(f'        values="{pivot_input.value_col}",')
        self._add_code(f'        index=["__temp_index__"],')
        self._add_code(f'        columns="{pivot_input.pivot_column}",')
        self._add_code(f'        aggregate_function="{agg_func}"')
        self._add_code("    )")
        self._add_code('    .drop("__temp_index__")')
        self._add_code(").lazy()")
        self._add_code("")

    def _handle_pivot(self, settings: input_schema.NodePivot, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle pivot nodes."""
        input_df = input_vars.get('main', 'df')
        pivot_input = settings.pivot_input
        if len(pivot_input.aggregations) > 1:
            logger.error("Multiple aggregations are not convertable to polars code. "
                         "Taking the first value")
        if len(pivot_input.aggregations) > 0:
            agg_func = pivot_input.aggregations[0]
        else:
            agg_func = 'first'
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

    def _handle_unpivot(self, settings: input_schema.NodeUnpivot, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle unpivot nodes."""
        input_df = input_vars.get('main', 'df')
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

    def _handle_union(self, settings: input_schema.NodeUnion, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle union nodes."""
        # Get all input LazyFrame
        dfs = []
        if 'main' in input_vars:
            dfs.append(input_vars['main'])
        else:
            # Multiple main inputs
            for key, df_var in input_vars.items():
                if key.startswith('main'):
                    dfs.append(df_var)

        if settings.union_input.mode == 'relaxed':
            how = 'diagonal_relaxed'
        else:
            how = 'diagonal'

        self._add_code(f"{var_name} = pl.concat([")
        for df in dfs:
            self._add_code(f"    {df},")
        self._add_code(f"], how='{how}')")
        self._add_code("")

    def _handle_sort(self, settings: input_schema.NodeSort, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle sort nodes."""
        input_df = input_vars.get('main', 'df')

        sort_cols = []
        descending = []

        for sort_input in settings.sort_input:
            sort_cols.append(f'"{sort_input.column}"')
            descending.append(sort_input.how == 'desc')

        self._add_code(f"{var_name} = {input_df}.sort([{', '.join(sort_cols)}], descending={descending})")
        self._add_code("")

    def _handle_sample(self, settings: input_schema.NodeSample, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle sample nodes."""
        input_df = input_vars.get('main', 'df')
        self._add_code(f"{var_name} = {input_df}.head(n={settings.sample_size})")
        self._add_code("")

    def _handle_unique(self, settings: input_schema.NodeUnique, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle unique/distinct nodes."""
        input_df = input_vars.get('main', 'df')

        if settings.unique_input.columns:
            self._add_code(f"{var_name} = {input_df}.unique(subset={settings.unique_input.columns}, keep='{settings.unique_input.strategy}')")
        else:
            self._add_code(f"{var_name} = {input_df}.unique(keep='{settings.unique_input.strategy}')")
        self._add_code("")

    def _handle_text_to_rows(self, settings: input_schema.NodeTextToRows, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle text to rows (explode) nodes."""
        input_df = input_vars.get('main', 'df')
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
    def _handle_record_id(self, settings: input_schema.NodeRecordId, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle record ID nodes."""
        input_df = input_vars.get('main', 'df')
        record_input = settings.record_id_input
        if record_input.group_by and record_input.group_by_columns:

            # Row number within groups
            self._add_code(f"{var_name} = ({input_df}")
            self._add_code(f"    .with_columns(pl.lit(1).alias('{record_input.output_column_name}'))")
            self._add_code(f"    .with_columns([")
            self._add_code(f"    (pl.cum_count('{record_input.output_column_name}').over({record_input.group_by_columns}) + {record_input.offset} - 1)")
            self._add_code(f"    .alias('{record_input.output_column_name}')")
            self._add_code("])")
            self._add_code(f".select(['{record_input.output_column_name}'] + [col for col in {input_df}.columns if col != '{record_input.output_column_name}'])")
            self._add_code(")")
        else:
            # Simple row number
            self._add_code(f"{var_name} = {input_df}.with_row_count(name='{record_input.output_column_name}', offset={record_input.offset})")
        self._add_code("")

    def _handle_cross_join(self, settings: input_schema.NodeCrossJoin, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle cross join nodes."""
        left_df = input_vars.get('main', input_vars.get('main_0', 'df_left'))
        right_df = input_vars.get('right', input_vars.get('main_1', 'df_right'))

        self._add_code(f"{var_name} = {left_df}.join({right_df}, how='cross')")
        self._add_code("")

    def _handle_output(self, settings: input_schema.NodeOutput, var_name: str, input_vars: Dict[str, str]) -> None:
        """Handle output nodes."""
        input_df = input_vars.get('main', 'df')
        output_settings = settings.output_settings

        if output_settings.file_type == 'csv':
            self._add_code(f'{input_df}.sink_csv(')
            self._add_code(f'    "{output_settings.abs_file_path}",')
            self._add_code(f'    separator="{output_settings.output_csv_table.delimiter}"')
            self._add_code(')')

        elif output_settings.file_type == 'parquet':
            self._add_code(f'{input_df}.sink_parquet("{output_settings.abs_file_path}")')

        elif output_settings.file_type == 'excel':
            self._add_code(f'{input_df}.collect().write_excel(')
            self._add_code(f'    "{output_settings.abs_file_path}",')
            self._add_code(f'    worksheet="{output_settings.output_excel_table.sheet_name}"')
            self._add_code(')')

        self._add_code("")

    def _handle_polars_code(self, settings: input_schema.NodePolarsCode, var_name: str, input_vars: Dict[str, str]) -> None:
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
                if key.startswith('main'):
                    param_list.append(f"input_df_{i}: pl.LazyFrame")
                    arg_list.append(input_vars[key])
                    i += 1
            params = ", ".join(param_list)
            args = ", ".join(arg_list)

        # Check if the code is just an expression (no assignment)
        is_expression = "output_df" not in code

        # Wrap the code in a function
        self._add_code(f"# Custom Polars code")
        self._add_code(f"def _polars_code_{var_name.replace('df_', '')}({params}):")

        # Handle the code based on its structure
        if is_expression:
            # It's just an expression, return it directly
            self._add_code(f"    return {code}")
        else:
            # It contains assignments
            for line in code.split('\n'):
                if line.strip():
                    self._add_code(f"    {line}")

            # If no explicit return, try to detect what to return
            if 'return' not in code:
                # Try to find the last assignment
                lines = [l.strip() for l in code.split('\n') if l.strip() and '=' in l]
                if lines:
                    last_assignment = lines[-1]
                    if '=' in last_assignment:
                        output_var = last_assignment.split('=')[0].strip()
                        self._add_code(f"    return {output_var}")

        self._add_code("")

        # Call the function
        self._add_code(f"{var_name} = _polars_code_{var_name.replace('df_', '')}({args})")
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
            op_map = {
                '=': '==',
                '!=': '!=',
                '>': '>',
                '<': '<',
                '>=': '>=',
                '<=': '<='
            }

            polars_op = op_map.get(op, op)

            # Check if value is numeric
            try:
                float(val)
                return f'pl.col("{col}") {polars_op} {val}'
            except ValueError:
                return f'pl.col("{col}") {polars_op} "{val}"'

        return re.sub(pattern, replace_expr, expr)

    def _create_basic_filter_expr(self, basic: transform_schema.BasicFilter) -> str:
        """Create Polars expression from basic filter."""
        col = f'pl.col("{basic.field}")'

        if basic.filter_type == 'equals':
            return f'{col} == "{basic.filter_value}"'
        elif basic.filter_type == 'not_equals':
            return f'{col} != "{basic.filter_value}"'
        elif basic.filter_type == 'greater':
            return f'{col} > {basic.filter_value}'
        elif basic.filter_type == 'less':
            return f'{col} < {basic.filter_value}'
        elif basic.filter_type == 'in':
            values = basic.filter_value.split(',')
            return f"pl.col('{col}').is_in({values})"
        return col

    def _get_polars_dtype(self, dtype_str: str) -> str:
        """Convert Flowfile dtype string to Polars dtype."""
        dtype_map = {
            'String': 'pl.Utf8',
            'Integer': 'pl.Int64',
            'Double': 'pl.Float64',
            'Boolean': 'pl.Boolean',
            'Date': 'pl.Date',
            'Datetime': 'pl.Datetime',
            'Float32': 'pl.Float32',
            'Float64': 'pl.Float64',
            'Int32': 'pl.Int32',
            'Int64': 'pl.Int64',
            'Utf8': 'pl.Utf8',
        }
        return dtype_map.get(dtype_str, 'pl.Utf8')

    def _get_agg_function(self, agg: str) -> str:
        """Get Polars aggregation function name."""
        agg_map = {
            'avg': 'mean',
            'average': 'mean',
            'concat': 'str.concat',
        }
        return agg_map.get(agg, agg)

    def _sql_to_polars_expr(self, sql_expr: str) -> str:
        """Convert SQL-like expression to Polars expression."""
        # This is a very simplified converter
        # In practice, you'd want a proper SQL parser

        # Replace column references
        import re

        # Pattern for column names (simplified)
        col_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'

        def replace_col(match):
            col_name = match.group(1)
            # Skip SQL keywords
            keywords = {'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'AS'}
            if col_name.upper() in keywords:
                return col_name
            return f'pl.col("{col_name}")'

        result = re.sub(col_pattern, replace_col, sql_expr)

        # Handle CASE WHEN
        if 'CASE' in result:
            # This would need proper parsing
            result = "pl.when(...).then(...).otherwise(...)"

        return result

    def add_return_code(self, lines: List[str]) -> None:
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

        # Add main function
        lines.append("def run_etl_pipeline():")
        lines.append('    """')
        lines.append(f'    ETL Pipeline: {self.flow_graph.__name__}')
        lines.append('    Generated from Flowfile')
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
