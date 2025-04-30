import uuid
from polars.datatypes import *
from typing import Any, Iterable, List

import polars as pl

# Assume these imports are correct from your original context
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection
from flowfile_core.flowfile.flowfile_frame.expr import Expr, Column, lit, col
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.schemas import input_schema, schemas, transform_schema

node_id_counter = 0


def _generate_id() -> int:
    """Generate a simple unique ID for nodes."""
    return int(uuid.uuid4().int % 100000)


def generate_node_id() -> int:
    global node_id_counter
    node_id_counter += 1
    return node_id_counter


def _is_iterable(obj: Any) -> bool:
    # Avoid treating strings as iterables in this context
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def _parse_inputs_as_iterable(
        inputs: tuple[Any, ...] | tuple[Iterable[Any]],
) -> Iterable[Any]:
    if not inputs:
        return []

    # Treat elements of a single iterable as separate inputs
    if len(inputs) == 1 and _is_iterable(inputs[0]):
        return inputs[0]

    return inputs


class GroupByFrame:
    """Represents a grouped DataFrame for aggregation operations."""

    def __init__(self, parent_frame, by_cols, maintain_order=False):
        self.parent = parent_frame
        self.by_cols = by_cols
        self.maintain_order = maintain_order

    def agg(self, *agg_exprs, **named_agg_exprs):
        """
        Apply aggregations to grouped data.

        Args:
            *agg_exprs: Expressions to aggregate
            **named_agg_exprs: Named expressions to aggregate

        Returns:
            A FlowFrame with the aggregated data
        """
        # Create a new node ID
        new_node_id = generate_node_id()
        agg_expressions = _parse_inputs_as_iterable(agg_exprs)
        # Prepare the agg_cols list
        agg_cols = []

        # Add groupby columns
        for col_expr in self.by_cols:
            if isinstance(col_expr, str):
                # Simple column name
                agg_cols.append(
                    transform_schema.AggColl(old_name=col_expr, agg="groupby")
                )
            elif isinstance(col_expr, Expr):
                # Expression with possible alias
                col_name = col_expr.name
                agg_cols.append(
                    transform_schema.AggColl(old_name=col_name, agg="groupby")
                )

        # Process positional aggregation expressions
        for expr in agg_expressions:
            if isinstance(expr, Expr):
                # Check if the expression has an aggregation function
                if hasattr(expr, "agg_func") and expr.agg_func:
                    agg_cols.append(
                        transform_schema.AggColl(
                            old_name=expr._initial_column_name or expr.name,
                            agg=expr.agg_func,
                            new_name=f"{expr.name}"
                            if expr.name
                            else None,
                        )
                    )
                else:
                    agg_cols.append(
                        transform_schema.AggColl(old_name=expr.name, agg="first")
                    )
            elif isinstance(expr, str):
                # String column name - assume we want to collect all values
                agg_cols.append(transform_schema.AggColl(old_name=expr, agg="first"))

        # Process named aggregation expressions
        for name, expr in named_agg_exprs.items():
            if isinstance(expr, Expr):
                agg_cols.append(
                    transform_schema.AggColl(
                        old_name=expr.name,
                        agg=expr.agg_func
                        if hasattr(expr, "agg_func") and expr.agg_func
                        else "first",
                        new_name=name,
                    )
                )
            elif isinstance(expr, str):
                agg_cols.append(
                    transform_schema.AggColl(old_name=expr, agg="first", new_name=name)
                )
            elif isinstance(expr, tuple) and len(expr) == 2:
                # (column, agg_func) format
                col_name = expr[0].name if isinstance(expr[0], Expr) else expr[0]
                agg_func = expr[1]
                agg_cols.append(
                    transform_schema.AggColl(
                        old_name=col_name, agg=agg_func, new_name=name
                    )
                )

        # Create node settings
        group_by_settings = input_schema.NodeGroupBy(
            flow_id=self.parent.flow_graph.flow_id,
            node_id=new_node_id,
            groupby_input=transform_schema.GroupByInput(agg_cols=agg_cols),
            pos_x=200,
            pos_y=200,
            is_setup=True,
            depending_on_id=self.parent.node_id,
        )

        # Add to graph
        self.parent.flow_graph.add_group_by(group_by_settings)

        # Create connection
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=self.parent.node_id, to_id=new_node_id
        )
        add_connection(self.parent.flow_graph, connection)

        # Return new frame
        return FlowFrame(
            data=self.parent.flow_graph.get_node(new_node_id)
            .get_resulting_data()
            .data_frame,
            flow_graph=self.parent.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.parent.node_id,
        )


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
        return self.data.collect_schema().names()

    def sort(self, by: List[Expr | str] | Expr | str,
             *more_by, descending: bool | List[bool] = False, nulls_last: bool = False,
             multithreaded: bool = True, maintain_order: bool = False):
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
        )
        self.flow_graph.add_sort(sort_settings)
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=self.node_id, to_id=new_node_id
        )
        add_connection(self.flow_graph, connection)

        # Return new frame
        return FlowFrame(
            data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
        )

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

            # Create connection
            connection = input_schema.NodeConnection.create_from_simple_input(
                from_id=self.node_id,
                to_id=new_node_id
            )
            add_connection(self.flow_graph, connection)

            # Return new frame
            return FlowFrame(
                data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id
            )

        else:
            # Handle expressions by creating a formula node
            # Convert to polars expressions
            readable_exprs = []
            is_readable: bool = True
            for col_ in columns:
                if isinstance(col_, Expr):
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
            # Add to graph

            # Create connection
            connection = input_schema.NodeConnection.create_from_simple_input(
                from_id=self.node_id,
                to_id=new_node_id
            )
            add_connection(self.flow_graph, connection)

            # Return new frame
            return FlowFrame(
                data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id
            )

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

        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=self.node_id,
            to_id=new_node_id
        )
        add_connection(self.flow_graph, connection)

        # Return new frame
        return FlowFrame(
            data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id
        )

    def group_by(self, *by, maintain_order=False, **named_by):
        """
        Start a group by operation.

        Parameters:
            *by: Column names or expressions to group by
            maintain_order: Keep groups in the order they appear in the data
            **named_by: Additional columns to group by with custom names

        Returns:
            GroupByFrame object for aggregations
        """
        # Process positional arguments
        by_cols = []
        for col_expr in by:
            if isinstance(col_expr, str):
                by_cols.append(col_expr)
            elif isinstance(col_expr, Expr):
                by_cols.append(col_expr)
            elif isinstance(col_expr, (list, tuple)):
                by_cols.extend(col_expr)

        # Process named arguments (column renames)
        for new_name, col_expr in named_by.items():
            if isinstance(col_expr, str):
                # For a string, create an expression with an alias
                by_cols.append(col(col_expr).alias(new_name))
            elif isinstance(col_expr, Expr):
                # For an expression, add an alias
                by_cols.append(col_expr.alias(new_name))

        # Create a GroupByFrame
        return GroupByFrame(
            parent_frame=self, by_cols=by_cols, maintain_order=maintain_order
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
            connection = input_schema.NodeConnection.create_from_simple_input(
                from_id=self.node_id,
                to_id=new_node_id
            )
            add_connection(self.flow_graph, connection)
            return FlowFrame(
                self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                node_id=new_node_id,
                parent_node_id=self.node_id,
            )

        elif flowfile_formulas is not None and output_column_names:
            if not len(output_column_names) == len(flowfile_formulas):
                raise ValueError("Lenght of both the formulas and the output columns names must be identical")

            if len(flowfile_formulas) == 1:
                return self._with_flowfile_formula(flowfile_formulas[0], output_column_names[0], description)
            ff = self
            for i, flowfile_formula, output_column_name in enumerate(zip(flowfile_formulas, output_column_names)):
                ff = ff._with_flowfile_formula(flowfile_formula, output_column_name, f"{i}: description")
            return ff
        else:
            raise

    def _with_flowfile_formula(self, flowfile_formula: str, output_column_name, description: str = None) -> "FlowFrame":
        new_node_id = generate_node_id()
        function_settings = (
            input_schema.NodeFormula(flow_id=self.flow_graph.flow_id, node_id=new_node_id, depending_on_id=self.node_id,
                                     function=transform_schema.FunctionInput(
                                         function=flowfile_formula,
                                         field=transform_schema.FieldInput(name=output_column_name)),
                                     description=description))
        self.flow_graph.add_formula(function_settings)
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=self.node_id,
            to_id=new_node_id
        )
        add_connection(self.flow_graph, connection)
        return FlowFrame(self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                         node_id=new_node_id, parent_node_id=self.node_id,
                         flow_graph=self.flow_graph)

    @property
    def schema(self):
        return self.data.collect_schema()



def read_csv(file_path, *, flow_graph: EtlGraph = None, description: str = None, **options):
    """
    Read a CSV file into a FlowFrame.

    Args:
        file_path: Path to CSV file
        flow_graph: if you want to add it to an existing graph
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
    # Extract options
    delimiter = options.get('separator', ',')
    has_headers = options.get('has_header', True)
    encoding = options.get('encoding', 'utf-8')

    # Create received table
    received_table = input_schema.ReceivedTable(
        file_type='csv',
        path=file_path,
        name=file_path.split('/')[-1],
        delimiter=delimiter,
        has_headers=has_headers,
        encoding=encoding
    )

    # Create read node
    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True
    )

    # Add to graph
    flow_graph.add_read(read_node)

    # Return new frame
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def read_parquet(file_path, *, flow_graph: EtlGraph = None, description: str = None, **options) -> FlowFrame:
    """
    Read a Parquet file into a FlowFrame.

    Args:
        file_path: Path to Parquet file
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
        **options: Options for polars.read_parquet

    Returns:
        A FlowFrame with the Parquet data
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

    # Create received table
    received_table = input_schema.ReceivedTable(
        file_type='parquet',
        path=file_path,
        name=file_path.split('/')[-1]
    )

    # Create read node
    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    # Add to graph
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
        description: if you want to add a readable name in the frontend (adviced)
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


# Aggregation functions
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
