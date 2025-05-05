import uuid
import os
from polars.datatypes import *
from typing import Any, Iterable, List

import polars as pl

# Assume these imports are correct from your original context
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection
from flowfile_core.flowfile.flowfile_frame.expr import Expr, Column, lit, col
from flowfile_core.flowfile.flowfile_frame.selectors import Selector
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.node_step.node_step import NodeStep
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.flowfile.flowfile_frame.utils import _parse_inputs_as_iterable
from flowfile_core.flowfile.flowfile_frame.group_frame import GroupByFrame

# --- Helper Functions ---

node_id_counter = 0


def _generate_id() -> int:
    """Generate a simple unique ID for nodes."""
    return int(uuid.uuid4().int % 100000)


def generate_node_id() -> int:
    global node_id_counter
    node_id_counter += 1
    return node_id_counter


class FlowFrame:
    """Main class that wraps FlowfileTable and maintains the ETL graph."""
    flow_graph: EtlGraph
    data: pl.LazyFrame

    def __init__(self,
                 data: pl.LazyFrame,
                 flow_graph=None,
                 node_id=None,
                 parent_node_id=None, ):
        """Initialize with data and graph references."""
        if not isinstance(data, pl.LazyFrame):
            raise ValueError('Data should be of type polars lazy frame')
        self.node_id = node_id or generate_node_id()
        self.parent_node_id = parent_node_id

        # Initialize graph
        if flow_graph is None:
            flow_id = _generate_id()
            flow_settings = schemas.FlowSettings(
                flow_id=flow_id,
                name=f"Flow_{flow_id}",
                path=f"flow_{flow_id}"
            )
            self.flow_graph = EtlGraph(flow_id=flow_id, flow_settings=flow_settings)
        else:
            self.flow_graph = flow_graph

        # Set up data
        if isinstance(data, FlowfileTable):
            self.data = data.data_frame
        elif isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            self.data = FlowfileTable(raw_data=data).data_frame
        else:
            # Assume list of dicts
            self.data = FlowfileTable(raw_data=data).data_frame

    def __repr__(self):
        return str(self.data)

    @property
    def columns(self):
        return self.data.columns

    def _add_connection(self, from_id, to_id):
        """Helper method to add a connection between nodes"""
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=from_id, to_id=to_id
        )
        add_connection(self.flow_graph, connection)

    def _create_child_frame(self, new_node_id):
        """Helper method to create a new FlowFrame that's a child of this one"""
        self._add_connection(self.node_id, new_node_id)
        return FlowFrame(
            data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
        )

    def sort(self, by: List[Expr | str] | Expr | str,
             *more_by, descending: bool | List[bool] = False, nulls_last: bool = False,
             multithreaded: bool = True, maintain_order: bool = False,
             description: str = None):
        by = list(_parse_inputs_as_iterable((by,)))
        new_node_id = generate_node_id()
        sort_expressions = by
        if more_by:
            sort_expressions.extend(more_by)

        # Check if descending is a list/sequence
        if isinstance(descending, (list, tuple)):
            # Ensure descending list has the same length as sort_expressions
            if len(descending) != len(sort_expressions):
                raise ValueError(
                    f"Length of descending ({len(descending)}) must match number of sort columns ({len(sort_expressions)})"
                )
            descending_values = descending
        else:
            # If it's a single boolean, repeat it for all expressions
            descending_values = [descending] * len(sort_expressions)

        # Process nulls_last in the same way
        if isinstance(nulls_last, (list, tuple)):
            if len(nulls_last) != len(sort_expressions):
                raise ValueError(
                    f"Length of nulls_last ({len(nulls_last)}) must match number of sort columns ({len(sort_expressions)})"
                )
            nulls_last_values = nulls_last
        else:
            nulls_last_values = [nulls_last] * len(sort_expressions)

        # Create SortByInput objects
        sort_inputs = []
        for i, expr in enumerate(sort_expressions):
            # Convert expr to column name
            if isinstance(expr, (Column, Expr)):
                column_name = expr.name
            elif isinstance(expr, str):
                column_name = expr
            else:
                column_name = str(expr)

            # Create SortByInput with appropriate settings
            sort_inputs.append(
                transform_schema.SortByInput(
                    column=column_name,
                    how="desc" if descending_values[i] else "asc",
                    # Note: nulls_last is not currently passed to SortByInput
                    # This would need to be added to your SortByInput schema
                )
            )

        sort_settings = input_schema.NodeSort(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            sort_input=sort_inputs,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description
        )
        self.flow_graph.add_sort(sort_settings)
        return self._create_child_frame(new_node_id)

    def _add_polars_code(self, new_node_id: int, code: str, description: str = None):
        polars_code_settings = input_schema.NodePolarsCode(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            is_setup=True,
            depending_on_id=self.node_id,
            description=description,
        )
        self.flow_graph.add_polars_code(polars_code_settings)

    def select(self, *columns, description: str = None):
        """
        Select columns from the frame.

        Args:
            *columns: Column names or expressions
            description: Description of the step, this will be shown in the flowfile file

        Returns:
            A new FlowFrame with selected columns
        """
        # Create new node ID
        columns = _parse_inputs_as_iterable(columns)
        new_node_id = generate_node_id()
        existing_columns = self.columns

        # Handle simple column names
        if all(isinstance(col_, (str, Column)) for col_ in columns):
            # Create select inputs
            select_inputs = [
                transform_schema.SelectInput(old_name=col_) if isinstance(col_, str) else col_.to_select_input()
                for col_ in columns
            ]
            dropped_columns = [transform_schema.SelectInput(c, keep=False) for c in existing_columns if
                               c not in [s.old_name for s in select_inputs]]
            select_inputs.extend(dropped_columns)
            select_settings = input_schema.NodeSelect(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                select_input=select_inputs,
                keep_missing=False,
                pos_x=200,
                pos_y=100,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description
            )

            # Add to graph
            self.flow_graph.add_select(select_settings)
            return self._create_child_frame(new_node_id)

        else:
            readable_exprs = []
            is_readable: bool = True
            for col_ in columns:
                if isinstance(col_, Expr):
                    readable_exprs.append(col_)
                elif isinstance(col_, Selector):
                    readable_exprs.append(col_)
                elif isinstance(col_, pl.expr.Expr):
                    print('warning this cannot be converted to flowfile frontend. Make sure you use the flowfile expr')
                    is_readable = False
                elif isinstance(col_, str) and col_ in self.columns:
                    col_expr = Column(col_)
                    readable_exprs.append(col_expr)
                else:
                    lit_expr = lit(col_)
                    readable_exprs.append(lit_expr)
            if is_readable:
                code = f"input_df.select([{', '.join(str(e) for e in readable_exprs)}])"
            else:
                raise ValueError('Not supported')

            self._add_polars_code(new_node_id, code, description)
            return self._create_child_frame(new_node_id)

    def filter(self, predicate: Expr | Any = None, *, flowfile_formula: str = None, description: str = None):
        """
        Filter rows based on a predicate.

        Args:
            predicate: Filter condition
            flowfile_formula: Native support in frontend
            description: Description of the step that is performed
        Returns:
            A new FlowFrame with filtered rows
        """
        new_node_id = generate_node_id()
        # Create new node ID
        if predicate:
            # we use for now the fallback on polars code.
            if isinstance(predicate, Expr):
                predicate_expr = predicate
            else:
                predicate_expr = lit(predicate)
            code = f"input_df.filter({str(predicate_expr)})"
            self._add_polars_code(new_node_id, code, description)

        elif flowfile_formula:
            # Create node settings
            filter_settings = input_schema.NodeFilter(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                filter_input=transform_schema.FilterInput(
                    advanced_filter=flowfile_formula,
                    filter_type="advanced"
                ),
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description
            )

            self.flow_graph.add_filter(filter_settings)

        return self._create_child_frame(new_node_id)

    def sink_csv(self,
                 file: str,
                 *args,
                 separator: str = ",",
                 encoding: str = "utf-8",
                 description: str = None):
        """
        Write the data to a CSV file.

        Args:
            path: Path or filename for the CSV file
            separator: Field delimiter to use, defaults to ','
            encoding: File encoding, defaults to 'utf-8'
            description: Description of this operation for the ETL graph

        Returns:
            Self for method chaining
        """
        return self.write_csv(file, *args, separator=separator, encoding=encoding, description=description)

    def write_parquet(
            self,
            path: str|os.PathLike,
            *,
            description: str = None,
            convert_to_absolute_path: bool = True,
            **kwargs: Any,
    ) -> "FlowFrame":
        """
        Write the data to a Parquet file. Creates a standard Output node if only
        'path' and standard options are provided. Falls back to a Polars Code node
        if other keyword arguments are used.

        Args:
            path: Path (string or pathlib.Path) or filename for the Parquet file.
                  Note: Writable file-like objects are not supported when using advanced options
                  that trigger the Polars Code node fallback.
            description: Description of this operation for the ETL graph.
            convert_to_absolute_path: If the path needs to be set to a fixed location.
            **kwargs: Additional keyword arguments for polars.DataFrame.sink_parquet/write_parquet.
                      If any kwargs other than 'description' or 'convert_to_absolute_path' are provided,
                      a Polars Code node will be created instead of a standard Output node.
                      Complex objects like IO streams or credential provider functions are NOT
                      supported via this method's Polars Code fallback.

        Returns:
            Self for method chaining (new FlowFrame pointing to the output node).
        """
        new_node_id = generate_node_id()

        is_path_input = isinstance(path, (str, os.PathLike))
        if isinstance(path, os.PathLike):
            file_str = str(path)
        elif isinstance(path, str):
            file_str = path
        else:
            file_str = path
            is_path_input = False

        file_name = file_str.split(os.sep)[-1]
        use_polars_code = bool(kwargs.items()) or not is_path_input

        output_parquet_table = input_schema.OutputParquetTable(
            file_type="parquet"
        )
        output_settings = input_schema.OutputSettings(
            file_type='parquet',
            name=file_name,
            directory=file_str if is_path_input else str(file_str),
            output_parquet_table=output_parquet_table,
            output_csv_table=input_schema.OutputCsvTable(),
            output_excel_table=input_schema.OutputExcelTable()
        )

        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                print(f"Warning: Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description
            )
            self.flow_graph.add_output(node_output)
        else:
            if not is_path_input:
                raise TypeError(
                    f"Input 'path' must be a string or Path-like object when using advanced "
                    f"write_parquet options (kwargs={kwargs.items()}), got {type(path)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )

            # Use the potentially converted absolute path string
            path_arg_repr = repr(output_settings.directory)
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in kwargs.items())
            args_str = f"path={path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"

            # Use sink_parquet for LazyFrames
            code = f"input_df.sink_parquet({args_str})"
            print(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def write_csv(
            self,
            file: str | os.PathLike,
            *,
            separator: str = ",",
            encoding: str = "utf-8",
            description: str = None,
            convert_to_absolute_path: bool = True,
            **kwargs: Any,
    ) -> "FlowFrame":
        new_node_id = generate_node_id()

        is_path_input = isinstance(file, (str, os.PathLike))
        if isinstance(file, os.PathLike):
            file_str = str(file)
        elif isinstance(file, str):
            file_str = file
        else:
            file_str = file
            is_path_input = False

        file_name = file_str.split(os.sep)[-1] if is_path_input else "output.csv"

        use_polars_code = bool(kwargs) or not is_path_input

        output_settings = input_schema.OutputSettings(
            file_type='csv',
            name=file_name,
            directory=file_str if is_path_input else str(file_str),
            output_csv_table=input_schema.OutputCsvTable(
                file_type="csv", delimiter=separator, encoding=encoding),
            output_excel_table=input_schema.OutputExcelTable(),
            output_parquet_table=input_schema.OutputParquetTable()
        )

        if is_path_input:
            try:
                output_settings.set_absolute_filepath()
                if convert_to_absolute_path:
                    output_settings.directory = output_settings.abs_file_path
            except Exception as e:
                print(f"Warning: Could not determine absolute path for {file_str}: {e}")

        if not use_polars_code:
            node_output = input_schema.NodeOutput(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                output_settings=output_settings,
                depending_on_id=self.node_id,
                description=description
            )
            self.flow_graph.add_output(node_output)
        else:
            if not is_path_input:
                raise TypeError(
                    f"Input 'file' must be a string or Path-like object when using advanced "
                    f"write_csv options (kwargs={kwargs}), got {type(file)}."
                    " File-like objects are not supported with the Polars Code fallback."
                )

            path_arg_repr = repr(output_settings.directory)

            all_kwargs_for_code = {
                'separator': separator,
                'encoding': encoding,
                **kwargs  # Add the extra kwargs
            }
            kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in all_kwargs_for_code.items())

            args_str = f"file={path_arg_repr}"
            if kwargs_repr:
                args_str += f", {kwargs_repr}"

            code = f"input_df.collect().write_csv({args_str})"
            print(f"Generated Polars Code: {code}")
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def group_by(self, *by, description: str = None, maintain_order=False, **named_by) -> GroupByFrame:
        """
        Start a group by operation.

        Parameters:
            *by: Column names or expressions to group by
            description: add optional description to this step for the frontend
            maintain_order: Keep groups in the order they appear in the data
            **named_by: Additional columns to group by with custom names

        Returns:
            GroupByFrame object for aggregations
        """
        # Process positional arguments
        new_node_id = generate_node_id()
        by_cols = []
        for col_expr in by:
            if isinstance(col_expr, str):
                by_cols.append(col_expr)
            elif isinstance(col_expr, Expr):
                by_cols.append(col_expr)
            elif isinstance(col_expr, Selector):
                by_cols.append(col_expr)
            elif isinstance(col_expr, (list, tuple)):
                by_cols.extend(col_expr)

        for new_name, col_expr in named_by.items():
            if isinstance(col_expr, str):
                by_cols.append(col(col_expr).alias(new_name))
            elif isinstance(col_expr, Expr):
                by_cols.append(col_expr.alias(new_name))

        # Create a GroupByFrame
        return GroupByFrame(
            node_id=new_node_id,
            parent_frame=self, by_cols=by_cols, maintain_order=maintain_order, description=description
        )

    def to_graph(self):
        """Get the underlying ETL graph."""
        return self.flow_graph

    def save_graph(self, file_path: str, auto_arrange: bool = True):
        """Save the graph """
        if auto_arrange:
            self.flow_graph.apply_layout()
        self.flow_graph.save_flow(file_path)

    def collect(self):
        """Collect lazy data into memory."""
        if hasattr(self.data, "collect"):
            return self.data.collect()
        return self.data

    def with_columns(self, exprs: Expr | List[Expr] = None, *,
                     flowfile_formulas: List[str] = None,
                     output_column_names: List[str] = None,
                     description: str = None) -> "FlowFrame":
        if exprs is not None:
            new_node_id = generate_node_id()
            exprs_iterable = _parse_inputs_as_iterable((exprs,))
            all_expressions = []
            for expression in exprs_iterable:
                if not isinstance(expression, (Expr, Column)):
                    all_expressions.append(lit(expression))
                else:
                    all_expressions.append(expression)
            code = f"input_df.with_columns({','.join(str(e) for e in all_expressions)})"
            self._add_polars_code(new_node_id, code, description)
            return self._create_child_frame(new_node_id)

        elif flowfile_formulas is not None and output_column_names:
            if not len(output_column_names) == len(flowfile_formulas):
                raise ValueError("Length of both the formulas and the output columns names must be identical")

            if len(flowfile_formulas) == 1:
                return self._with_flowfile_formula(flowfile_formulas[0], output_column_names[0], description)
            ff = self
            for i, (flowfile_formula, output_column_name) in enumerate(zip(flowfile_formulas, output_column_names)):
                ff = ff._with_flowfile_formula(flowfile_formula, output_column_name, f"{i}: {description}")
            return ff
        else:
            raise ValueError("Either exprs or flowfile_formulas with output_column_names must be provided")

    def _with_flowfile_formula(self, flowfile_formula: str, output_column_name, description: str = None) -> "FlowFrame":
        new_node_id = generate_node_id()
        function_settings = (
            input_schema.NodeFormula(flow_id=self.flow_graph.flow_id, node_id=new_node_id, depending_on_id=self.node_id,
                                     function=transform_schema.FunctionInput(
                                         function=flowfile_formula,
                                         field=transform_schema.FieldInput(name=output_column_name)),
                                     description=description))
        self.flow_graph.add_formula(function_settings)
        return self._create_child_frame(new_node_id)

    @property
    def schema(self):
        return self.data.collect_schema()

    def head(self, n: int, description: str = None):
        new_node_id = generate_node_id()
        settings = input_schema.NodeSample(flow_id=self.flow_graph.flow_id,
                                           node_id=new_node_id,
                                           depending_on_id=self.node_id,
                                           sample_size=n,
                                           description=description
                                           )
        self.flow_graph.add_sample(settings)
        return self._create_child_frame(new_node_id)

    def limit(self, n: int, description: str = None):
        return self.head(n, description)

    def cache(self) -> "FlowFrame":
        setting_input = self.get_node_settings().setting_input
        setting_input.cache_results = True
        return self

    def get_node_settings(self) -> NodeStep:
        return self.flow_graph.get_node(self.node_id)


def sum(expr):
    """Sum aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.sum()


def mean(expr):
    """Mean aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.mean()


def min(expr):
    """Min aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.min()


def max(expr):
    """Max aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.max()


def count(expr):
    """Count aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.count()


def read_csv(file_path, *, flow_graph: EtlGraph = None, separator: str = ';',
             convert_to_absolute_path: bool = True,
             description: str = None, **options):
    """
    Read a CSV file into a FlowFrame.

    Args:
        file_path: Path to CSV file
        flow_graph: if you want to add it to an existing graph
        separator: Single byte character to use as separator in the file.
        convert_to_absolute_path: If the path needs to be set to a fixed location
        description: if you want to add a readable name in the frontend (advised)
        **options: Options for polars.read_csv

    Returns:
        A FlowFrame with the CSV data
    """
    # Create new node ID
    node_id = generate_node_id()
    if flow_graph is None:
        flow_id = _generate_id()
        flow_settings = schemas.FlowSettings(
            flow_id=flow_id,
            name=f"Flow_{flow_id}",
            path=f"flow_{flow_id}"
        )
        flow_graph = EtlGraph(flow_id=flow_id, flow_settings=flow_settings)
    else:
        flow_id = flow_graph.flow_id
    has_headers = options.get('has_header', True)
    encoding = options.get('encoding', 'utf-8')

    received_table = input_schema.ReceivedTable(
        file_type='csv',
        path=file_path,
        name=file_path.split('/')[-1],
        delimiter=separator,
        has_headers=has_headers,
        encoding=encoding
    )

    if convert_to_absolute_path:
        received_table.path = received_table.abs_file_path

    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True
    )

    flow_graph.add_read(read_node)

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def read_parquet(file_path, *, flow_graph: EtlGraph = None, description: str = None,
                 convert_to_absolute_path: bool = True, **options) -> FlowFrame:
    """
    Read a Parquet file into a FlowFrame.

    Args:
        file_path: Path to Parquet file
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
        convert_to_absolute_path: If the path needs to be set to a fixed location
        **options: Options for polars.read_parquet

    Returns:
        A FlowFrame with the Parquet data
    """
    node_id = generate_node_id()

    if flow_graph is None:
        flow_id = _generate_id()
        flow_settings = schemas.FlowSettings(
            flow_id=flow_id,
            name=f"Flow_{flow_id}",
            path=f"flow_{flow_id}"
        )
        flow_graph = EtlGraph(flow_id=flow_id, flow_settings=flow_settings)
    else:
        flow_id = flow_graph.flow_id

    received_table = input_schema.ReceivedTable(
        file_type='parquet',
        path=file_path,
        name=file_path.split('/')[-1]
    )
    if convert_to_absolute_path:
        received_table.path = received_table.abs_file_path

    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    flow_graph.add_read(read_node)

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def from_dict(data, *, flow_graph: EtlGraph = None, description: str = None) -> FlowFrame:
    """
    Create a FlowFrame from a dictionary or list of dictionaries.

    Args:
        data: Dictionary of lists or list of dictionaries
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
    Returns:
        A FlowFrame with the data
    """
    # Create new node ID
    node_id = generate_node_id()

    if not flow_graph:
        flow_id = _generate_id()
        flow_settings = schemas.FlowSettings(
            flow_id=flow_id,
            name=f"Flow_{flow_id}",
            path=f"flow_{flow_id}"
        )
        flow_graph = EtlGraph(flow_id=flow_id, flow_settings=flow_settings)
    else:
        flow_id = flow_graph.flow_id

    input_node = input_schema.NodeManualInput(
        flow_id=flow_id,
        node_id=node_id,
        raw_data=FlowfileTable(data).to_pylist(),
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    # Add to graph
    flow_graph.add_manual_input(input_node)

    # Return new frame
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )
