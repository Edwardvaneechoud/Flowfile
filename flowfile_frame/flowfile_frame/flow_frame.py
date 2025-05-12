import uuid
import os
from typing import Any, Iterable, List, Literal, Optional, Tuple, Union
from pathlib import Path

import re
import polars as pl
from polars._typing import FrameInitTypes, SchemaDefinition, SchemaDict, Orientation

# Assume these imports are correct from your original context
from flowfile_core.flowfile.FlowfileFlow import FlowGraph, add_connection
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema, transform_schema

from flowfile_frame.expr import Expr, Column, lit, col
from flowfile_frame.selectors import Selector
from flowfile_frame.group_frame import GroupByFrame
from flowfile_frame.utils import _parse_inputs_as_iterable, create_flow_graph
from flowfile_frame.join import _normalize_columns_to_list, _create_join_mappings

node_id_counter = 0


def _to_string_val(v) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    else:
        return v


def generate_node_id() -> int:
    global node_id_counter
    node_id_counter += 1
    return node_id_counter


class FlowFrame:
    """Main class that wraps FlowDataEngine and maintains the ETL graph."""
    flow_graph: FlowGraph
    data: pl.LazyFrame

    @staticmethod
    def create_from_any_type(
        data: FrameInitTypes = None,
        schema: SchemaDefinition | None = None,
        *,
        schema_overrides: SchemaDict | None = None,
        strict: bool = True,
        orient: Orientation | None = None,
        infer_schema_length: int | None = 100,
        nan_to_null: bool = False,
        flow_graph=None,
        node_id=None,
        parent_node_id=None,
    ):
        """
        Simple naive implementation of creating the frame from any type. It converts the data to a polars frame,
        next it implements it from a manual_input

        Parameters
        ----------
        data : FrameInitTypes
            Data to initialize the frame with
        schema : SchemaDefinition, optional
            Schema definition for the data
        schema_overrides : pl.SchemaDict, optional
            Schema overrides for specific columns
        strict : bool, default True
            Whether to enforce the schema strictly
        orient : pl.Orientation, optional
            Orientation of the data
        infer_schema_length : int, default 100
            Number of rows to use for schema inference
        nan_to_null : bool, default False
            Whether to convert NaN values to null
        flow_graph : FlowGraph, optional
            Existing ETL graph to add nodes to
        node_id : int, optional
            ID for the new node
        parent_node_id : int, optional
            ID of the parent node

        Returns
        -------
        FlowFrame
            A new FlowFrame with the data loaded as a manual input node
        """
        # Extract flow-specific parameters
        node_id = node_id or generate_node_id()
        description = "Data imported from Python object"

        # Create a new flow graph if none is provided
        if flow_graph is None:
            flow_graph = create_flow_graph()

        flow_id = flow_graph.flow_id

        # Convert data to a polars DataFrame/LazyFrame
        try:
            # Use polars to convert from various types
            pl_df = pl.DataFrame(
                data,
                schema=schema,
                schema_overrides=schema_overrides,
                strict=strict,
                orient=orient,
                infer_schema_length=infer_schema_length,
                nan_to_null=nan_to_null,
            )
            pl_data = pl_df.lazy()
        except Exception as e:
            raise ValueError(f"Could not convert data to a polars DataFrame: {e}")

        # Create a FlowDataEngine to get data in the right format for manual input
        flow_table = FlowDataEngine(raw_data=pl_data)

        # Create a manual input node
        input_node = input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=node_id,
            raw_data=flow_table.to_pylist(),  # Convert to list of dicts
            pos_x=100,
            pos_y=100,
            is_setup=True,
            description=description,
        )

        # Add to graph
        flow_graph.add_manual_input(input_node)

        # Return new frame
        return FlowFrame(
            data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
            flow_graph=flow_graph,
            node_id=node_id,
            parent_node_id=parent_node_id,
        )

    def __new__(
        cls,
        data: pl.LazyFrame | FrameInitTypes = None,
        schema: SchemaDefinition | None = None,
        *,
        schema_overrides: SchemaDict | None = None,
        strict: bool = True,
        orient: Orientation | None = None,
        infer_schema_length: int | None = 100,
        nan_to_null: bool = False,
        flow_graph=None,
        node_id=None,
        parent_node_id=None,
    ):
        """Create a new FlowFrame instance."""

        # If data is not a LazyFrame, use the factory method
        if data is not None and not isinstance(data, pl.LazyFrame):
            return cls.create_from_any_type(
                data=data,
                schema=schema,
                schema_overrides=schema_overrides,
                strict=strict,
                orient=orient,
                infer_schema_length=infer_schema_length,
                nan_to_null=nan_to_null,
                flow_graph=flow_graph,
                node_id=node_id,
                parent_node_id=parent_node_id,
            )

        # Otherwise create the instance normally
        instance = super().__new__(cls)
        return instance

    def __init__(
        self,
        data: pl.LazyFrame | FrameInitTypes = None,
        schema: SchemaDefinition | None = None,
        *,
        schema_overrides: SchemaDict | None = None,
        strict: bool = True,
        orient: Orientation | None = None,
        infer_schema_length: int | None = 100,
        nan_to_null: bool = False,
        flow_graph=None,
        node_id=None,
        parent_node_id=None,
    ):
        """Initialize the FlowFrame with data and graph references."""

        if data is None:
            data = pl.LazyFrame()
        if not isinstance(data, pl.LazyFrame):
            return

        self.node_id = node_id or generate_node_id()
        self.parent_node_id = parent_node_id

        # Initialize graph
        if flow_graph is None:
            flow_graph = create_flow_graph()
        self.flow_graph = flow_graph
        # Set up data
        if isinstance(data, FlowDataEngine):
            self.data = data.data_frame
        else:
            self.data = data

    def __repr__(self):
        return str(self.data)

    def _add_connection(self, from_id, to_id, input_type: input_schema.InputType = "main"):
        """Helper method to add a connection between nodes"""
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=from_id, to_id=to_id, input_type=input_type
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

    def sort(
        self,
        by: List[Expr | str] | Expr | str,
        *more_by,
        descending: bool | List[bool] = False,
        nulls_last: bool = False,
        multithreaded: bool = True,
        maintain_order: bool = False,
        description: str = None,
    ):
        """
        Sort the dataframe by the given columns.

        Parameters:
        -----------
        by : Expr, str, or list of Expr/str
            Column(s) to sort by. Accepts expression input. Strings are parsed as column names.
        *more_by : Expr or str
            Additional columns to sort by, specified as positional arguments.
        descending : bool or list of bool, default False
            Sort in descending order. When sorting by multiple columns, can be specified per column.
        nulls_last : bool or list of bool, default False
            Place null values last; can specify a single boolean or a sequence for per-column control.
        multithreaded : bool, default True
            Sort using multiple threads.
        maintain_order : bool, default False
            Whether the order should be maintained if elements are equal.
        description : str, optional
            Description of this operation for the ETL graph.

        Returns:
        --------
        FlowFrame
            A new FlowFrame with sorted data.
        """
        by = list(_parse_inputs_as_iterable((by,)))
        new_node_id = generate_node_id()
        sort_expressions = by
        if more_by:
            sort_expressions.extend(more_by)

        # Determine if we need to use polars code fallback
        needs_polars_code = False

        # Check for any expressions that are not simple columns
        for expr in sort_expressions:
            if not isinstance(expr, (str, Column)) or (
                isinstance(expr, Column) and expr._select_input.is_altered
            ):
                needs_polars_code = True
                break

        # Also need polars code if we're using maintain_order or multithreaded params
        if maintain_order or not multithreaded:
            needs_polars_code = True

        # Standardize descending parameter
        if isinstance(descending, (list, tuple)):
            # Ensure descending list has the same length as sort_expressions
            if len(descending) != len(sort_expressions):
                raise ValueError(
                    f"Length of descending ({len(descending)}) must match number of sort columns ({len(sort_expressions)})"
                )
            descending_values = descending
        else:
            descending_values = [descending] * len(sort_expressions)

        # Standardize nulls_last parameter
        if isinstance(nulls_last, (list, tuple)):
            if len(nulls_last) != len(sort_expressions):
                raise ValueError(
                    f"Length of nulls_last ({len(nulls_last)}) must match number of sort columns ({len(sort_expressions)})"
                )
            nulls_last_values = nulls_last
            # Any non-default nulls_last needs polars code
            if any(val is not False for val in nulls_last_values):
                needs_polars_code = True
        else:
            nulls_last_values = [nulls_last] * len(sort_expressions)
            # Non-default nulls_last needs polars code
            if nulls_last:
                needs_polars_code = True

        if needs_polars_code:
            # Generate polars code for complex cases
            code = self._generate_sort_polars_code(
                sort_expressions,
                descending_values,
                nulls_last_values,
                multithreaded,
                maintain_order,
            )
            self._add_polars_code(new_node_id, code, description)
        else:
            # Use native implementation for simple cases
            sort_inputs = []
            for i, expr in enumerate(sort_expressions):
                # Convert expr to column name
                if isinstance(expr, Column):
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
                or f"Sort by {', '.join(str(e) for e in sort_expressions)}",
            )
            self.flow_graph.add_sort(sort_settings)

        return self._create_child_frame(new_node_id)

    def _generate_sort_polars_code(
        self,
        sort_expressions: list,
        descending_values: list,
        nulls_last_values: list,
        multithreaded: bool,
        maintain_order: bool,
    ) -> str:
        """Generate Polars code for sort operations that need fallback."""
        # Format expressions for code
        expr_strs = []
        for expr in sort_expressions:
            if isinstance(expr, (Expr, Column)):
                expr_strs.append(str(expr))
            elif isinstance(expr, str):
                expr_strs.append(f"'{expr}'")
            else:
                expr_strs.append(str(expr))

        # Format parameters
        if len(sort_expressions) == 1:
            by_arg = expr_strs[0]
        else:
            by_arg = f"[{', '.join(expr_strs)}]"

        # Build kwargs
        kwargs = {}

        # Only add descending if it's non-default
        if any(d for d in descending_values):
            if len(descending_values) == 1:
                kwargs["descending"] = descending_values[0]
            else:
                kwargs["descending"] = descending_values

        # Only add nulls_last if it's non-default
        if any(nl for nl in nulls_last_values):
            if len(nulls_last_values) == 1:
                kwargs["nulls_last"] = nulls_last_values[0]
            else:
                kwargs["nulls_last"] = nulls_last_values

        # Add other parameters if they're non-default
        if not multithreaded:
            kwargs["multithreaded"] = multithreaded

        if maintain_order:
            kwargs["maintain_order"] = maintain_order

        # Build kwargs string
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())

        # Build final code
        if kwargs_str:
            return f"input_df.sort({by_arg}, {kwargs_str})"
        else:
            return f"input_df.sort({by_arg})"

    def _add_polars_code(self, new_node_id: int, code: str, description: str = None,
                         depending_on_ids: List[str] | None = None):
        polars_code_settings = input_schema.NodePolarsCode(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            is_setup=True,
            depending_on_ids=depending_on_ids if depending_on_ids is not None else [self.node_id],
            description=description,
        )
        self.flow_graph.add_polars_code(polars_code_settings)

    def join(
        self,
        other,
        on: List[str | Column] | str | Column = None,
        how: str = "inner",
        left_on: List[str | Column] | str | Column = None,
        right_on: List[str | Column] | str | Column = None,
        suffix: str = "_right",
        validate: str = None,
        nulls_equal: bool = False,
        coalesce: bool = None,
        maintain_order: Literal[None, "left", "right", "left_right", "right_left"] = None,
        description: str = None,
    ):
        """
        Add a join operation to the Logical Plan.

        Parameters
        ----------
        other : FlowFrame
            Other DataFrame.
        on : str or list of str, optional
            Name(s) of the join columns in both DataFrames.
        how : {'inner', 'left', 'outer', 'semi', 'anti', 'cross'}, default 'inner'
            Join strategy.
        left_on : str or list of str, optional
            Name(s) of the left join column(s).
        right_on : str or list of str, optional
            Name(s) of the right join column(s).
        suffix : str, default "_right"
            Suffix to add to columns with a duplicate name.
        validate : {"1:1", "1:m", "m:1", "m:m"}, optional
            Validate join relationship.
        nulls_equal:
            Join on null values. By default null values will never produce matches.
        coalesce:
            None: -> join specific.
            True: -> Always coalesce join columns.
            False: -> Never coalesce join columns.
        maintain_order:
            Which DataFrame row order to preserve, if any. Do not rely on any observed ordering without explicitly setting this parameter, as your code may break in a future release. Not specifying any ordering can improve performance Supported for inner, left, right and full joins
            None: No specific ordering is desired. The ordering might differ across Polars versions or even between different runs.
            left: Preserves the order of the left DataFrame.
            right: Preserves the order of the right DataFrame.
            left_right: First preserves the order of the left DataFrame, then the right.
            right_left: First preserves the order of the right DataFrame, then the left.
        description : str, optional
            Description of the join operation for the ETL graph.

        Returns
        -------
        FlowFrame
            New FlowFrame with join operation applied.
        """
        new_node_id = generate_node_id()
        print('new node id', new_node_id)
        use_polars_code = not(maintain_order is None and
                              coalesce is None and
                              nulls_equal is False and
                              validate is None and
                              suffix == '_right')
        join_mappings = None
        if on is not None:
            left_columns = right_columns = _normalize_columns_to_list(on)
        elif left_on is not None and right_on is not None:
            left_columns = _normalize_columns_to_list(left_on)
            right_columns = _normalize_columns_to_list(right_on)
        elif how == 'cross' and left_on is None and right_on is None and on is None:
            left_columns = None
            right_columns = None
        else:
            raise ValueError("Must specify either 'on' or both 'left_on' and 'right_on'")

        # Ensure left and right column lists have same length
        if how != 'cross' and len(left_columns) != len(right_columns):
            raise ValueError(
                f"Length mismatch: left columns ({len(left_columns)}) != right columns ({len(right_columns)})"
            )
        if not use_polars_code:
            join_mappings, use_polars_code = _create_join_mappings(
                left_columns, right_columns
            )

        if use_polars_code or suffix != '_right':
            _on = "["+', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in _normalize_columns_to_list(on)) + "]" if on else None
            _left = "["+', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in left_columns) + "]" if left_on else None
            _right = "["+', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in right_columns) + "]" if right_on else None
            code_kwargs = {"other": "input_df_2", "how": _to_string_val(how), "on": _on, "left_on": _left,
                           "right_on": _right, "suffix": _to_string_val(suffix), "validate": _to_string_val(validate),
                           "nulls_equal": nulls_equal, "coalesce": coalesce,
                           "maintain_order": _to_string_val(maintain_order)}
            kwargs_str = ", ".join(f"{k}={v}" for k, v in code_kwargs.items() if v is not None)
            code = f"input_df_1.join({kwargs_str})"
            self._add_polars_code(new_node_id, code, description, depending_on_ids=[self.node_id, other.node_id])
            self._add_connection(self.node_id, new_node_id, "main")
            other._add_connection(other.node_id, new_node_id, "main")
            result_frame = FlowFrame(
                data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id,
            )

        elif join_mappings:
            left_select = transform_schema.SelectInputs.create_from_pl_df(self.data)
            right_select = transform_schema.SelectInputs.create_from_pl_df(other.data)

            join_input = transform_schema.JoinInput(
                join_mapping=join_mappings,
                left_select=left_select.renames,
                right_select=right_select.renames,
                how=how,
            )
            join_input.auto_rename()
            # Create node settings
            join_settings = input_schema.NodeJoin(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                join_input=join_input,
                auto_generate_selection=True,
                verify_integrity=True,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_ids=[self.node_id, other.node_id],
                description=description or f"Join with {how} strategy",
            )
            self.flow_graph.add_join(join_settings)
            self._add_connection(self.node_id, new_node_id, "main")
            other._add_connection(other.node_id, new_node_id, "right")
            result_frame = FlowFrame(
                data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
                flow_graph=self.flow_graph,
                node_id=new_node_id,
                parent_node_id=self.node_id,
            )
        else:
            raise ValueError("Could not execute join")

        return result_frame

    def _add_number_of_records(self, new_node_id: int, description: str = None) -> "FlowFrame":
        node_number_of_records = input_schema.NodeRecordCount(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            pos_x=200,
            pos_y=100,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description
        )
        self.flow_graph.add_record_count(node_number_of_records)
        return self._create_child_frame(new_node_id)

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

        if (len(columns) == 1 and isinstance(columns[0], Expr)
                and str(columns[0]) == "pl.Expr(len()).alias('number_of_records')"):
            return self._add_number_of_records(new_node_id, description)

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
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
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
        if "~" in file_str:
            file_str = os.path.expanduser(file_str)
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
        self.data.cache()
        return self

    def get_node_settings(self) -> FlowNode:
        return self.flow_graph.get_node(self.node_id)

    def pivot(self,
              on: str | list[str],
              *,
              index: str | list[str] | None = None,
              values: str | list[str] | None = None,
              aggregate_function: str | None = "first",
              maintain_order: bool = True,
              sort_columns: bool = False,
              separator: str = '_',
              description: str = None) -> "FlowFrame":
        """
        Pivot a DataFrame from long to wide format.

        Parameters
        ----------
        on: str | list[str]
            Column values to use as column names in the pivoted DataFrame
        index: str | list[str] | None
            Column(s) to use as index/row identifiers in the pivoted DataFrame
        values: str | list[str] | None
            Column(s) that contain the values of the pivoted DataFrame
        aggregate_function: str | None
            Function to aggregate values if there are duplicate entries.
            Options: 'first', 'last', 'min', 'max', 'sum', 'mean', 'median', 'count'
        maintain_order: bool
            Whether to maintain the order of the columns/rows as they appear in the source
        sort_columns: bool
            Whether to sort the output columns
        separator: str
            Separator to use when joining column levels in the pivoted DataFrame
        description: str
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with pivoted data
        """
        new_node_id = generate_node_id()

        # Handle input standardization
        on_value = on[0] if isinstance(on, list) and len(on) == 1 else on

        # Create index_columns list
        if index is None:
            index_columns = []
        elif isinstance(index, str):
            index_columns = [index]
        else:
            index_columns = list(index)

        # Set values column
        if values is None:
            raise ValueError("Values parameter must be specified for pivot operation")

        value_col = values if isinstance(values, str) else values[0]

        # Set valid aggregations
        valid_aggs = ['first', 'last', 'min', 'max', 'sum', 'mean', 'median', 'count']
        if aggregate_function not in valid_aggs:
            raise ValueError(f"Invalid aggregate_function: {aggregate_function}. "
                             f"Must be one of: {', '.join(valid_aggs)}")

        # Check if we can use the native implementation
        can_use_native = (
                isinstance(on_value, str) and
                isinstance(value_col, str) and
                aggregate_function in valid_aggs
        )

        if can_use_native:
            # Create pivot input for native implementation
            pivot_input = transform_schema.PivotInput(
                index_columns=index_columns,
                pivot_column=on_value,
                value_col=value_col,
                aggregations=[aggregate_function]
            )

            # Create node settings
            pivot_settings = input_schema.NodePivot(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                pivot_input=pivot_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Pivot {value_col} by {on_value}"
            )

            # Add to graph using native implementation
            self.flow_graph.add_pivot(pivot_settings)
        else:
            # Fall back to polars code for complex cases
            # Generate proper polars code
            on_repr = repr(on)
            index_repr = repr(index)
            values_repr = repr(values)

            code = f"""
    # Perform pivot operation
    result = input_df.pivot(
        on={on_repr}, 
        index={index_repr},
        values={values_repr},
        aggregate_function='{aggregate_function}',
        maintain_order={maintain_order},
        sort_columns={sort_columns},
        separator="{separator}"
    )
    result
    """
            # Generate description if not provided
            if description is None:
                on_str = on if isinstance(on, str) else ", ".join(on if isinstance(on, list) else [on])
                values_str = values if isinstance(values, str) else ", ".join(
                    values if isinstance(values, list) else [values])
                description = f"Pivot {values_str} by {on_str}"

            # Add polars code node
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def unpivot(self,
                on: list[str | Selector] | str | None | Selector = None,
                *,
                index: list[str] | str | None = None,
                variable_name: str = "variable",
                value_name: str = "value",
                description: str = None) -> "FlowFrame":
        """
        Unpivot a DataFrame from wide to long format.

        Parameters
        ----------
        on : list[str | Selector] | str | None | Selector
            Column(s) to unpivot (become values in the value column)
            If None, all columns not in index will be used
        index : list[str] | str | None
            Column(s) to use as identifier variables (stay as columns)
        variable_name : str, optional
            Name to give to the variable column, by default "variable"
        value_name : str, optional
            Name to give to the value column, by default "value"
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with unpivoted data
        """
        new_node_id = generate_node_id()

        # Standardize inputs
        if index is None:
            index_columns = []
        elif isinstance(index, str):
            index_columns = [index]
        else:
            index_columns = list(index)
        can_use_native = True
        if on is None:
            value_columns = []
        elif isinstance(on, (str, Selector)):
            if isinstance(on, Selector):
                can_use_native = False
            value_columns = [on]
        elif isinstance(on, Iterable):
            value_columns = list(on)
            if isinstance(value_columns[0], Iterable):
                can_use_native = False
        else:
            value_columns = [on]

        if can_use_native:
            can_use_native = (variable_name == "variable" and value_name == "value")
        if can_use_native:
            unpivot_input = transform_schema.UnpivotInput(
                index_columns=index_columns,
                value_columns=value_columns,
                data_type_selector=None,
                data_type_selector_mode='column'
            )

            # Create node settings
            unpivot_settings = input_schema.NodeUnpivot(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                unpivot_input=unpivot_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or "Unpivot data from wide to long format"
            )

            # Add to graph using native implementation
            self.flow_graph.add_unpivot(unpivot_settings)
        else:
            # Fall back to polars code for complex cases

            # Generate proper polars code
            on_repr = repr(on)
            index_repr = repr(index)

            # Using unpivot() method to match polars API
            code = f"""
    # Perform unpivot operation
    output_df = input_df.unpivot(
        on={on_repr}, 
        index={index_repr},
        variable_name="{variable_name}",
        value_name="{value_name}"
    )
    output_df
    """
            # Generate description if not provided
            if description is None:
                index_str = ", ".join(index_columns) if index_columns else "none"
                value_str = ", ".join(value_columns) if value_columns else "all non-index columns"
                description = f"Unpivot data with index: {index_str} and value cols: {value_str}"

            # Add polars code node
            self._add_polars_code(new_node_id, code, description)

        return self._create_child_frame(new_node_id)

    def concat(
        self,
        other: "FlowFrame" | List["FlowFrame"],
        how: str = "vertical",
        rechunk: bool = False,
        parallel: bool = True,
        description: str = None,
    ) -> "FlowFrame":
        """
        Combine multiple FlowFrames into a single FlowFrame.

        This is equivalent to Polars' concat operation with various joining strategies.

        Parameters
        ----------
        other : FlowFrame or List[FlowFrame]
            One or more FlowFrames to concatenate with this one
        how : str, default 'vertical'
            How to combine the FlowFrames:
            - 'vertical': Stack frames on top of each other (equivalent to 'union all')
            - 'vertical_relaxed': Same as vertical but coerces columns to common supertypes
            - 'diagonal': Union of column schemas, filling missing values with null
            - 'diagonal_relaxed': Same as diagonal but coerces columns to common supertypes
            - 'horizontal': Stack horizontally (column-wise concat)
            - 'align', 'align_full', 'align_left', 'align_right': Auto-determine key columns
        rechunk : bool, default False
            Whether to ensure contiguous memory in result
        parallel : bool, default True
            Whether to use parallel processing for the operation
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with the concatenated data
        """
        new_node_id = generate_node_id()

        # Convert single FlowFrame to list
        if isinstance(other, FlowFrame):
            others = [other]
        else:
            others = other

        use_native = how == "diagonal_relaxed" and parallel and not rechunk

        if use_native:
            # Create union input for the transform schema
            union_input = transform_schema.UnionInput(
                mode="relaxed"  # This maps to diagonal_relaxed in polars
            )

            # Create node settings
            union_settings = input_schema.NodeUnion(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                union_input=union_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_ids=[self.node_id] + [frame.node_id for frame in others],
                description=description or "Concatenate dataframes",
            )

            # Add to graph
            self.flow_graph.add_union(union_settings)

            # Add connections
            self._add_connection(self.node_id, new_node_id, "main")
            for other_frame in others:
                other_frame._add_connection(other_frame.node_id, new_node_id, "main")
        else:
            # Fall back to Polars code for other cases
            # Create a list of input dataframes for the code
            input_vars = ["input_df_1"]
            for i in range(len(others)):
                input_vars.append(f"input_df_{i+2}")

            frames_list = f"[{', '.join(input_vars)}]"

            code = f"""
            # Perform concat operation
            output_df = pl.concat(
                {frames_list},
                how='{how}',
                rechunk={rechunk},
                parallel={parallel}
            )
            """


            # Add polars code node with dependencies on all input frames
            depending_on_ids = [self.node_id] + [frame.node_id for frame in others]
            self._add_polars_code(
                new_node_id, code, description, depending_on_ids=depending_on_ids
            )

            # Add connections to ensure all frames are available
            self._add_connection(self.node_id, new_node_id, "main")
            for other_frame in others:
                other_frame._add_connection(other_frame.node_id, new_node_id, "main")

        # Create and return the new frame
        return FlowFrame(
            data=self.flow_graph.get_node(new_node_id).get_resulting_data().data_frame,
            flow_graph=self.flow_graph,
            node_id=new_node_id,
            parent_node_id=self.node_id,
        )

    def _detect_cum_count_record_id(
        self, expr: Any, new_node_id: int, description: Optional[str] = None
    ) -> Tuple[bool, Optional["FlowFrame"]]:
        """
        Detect if the expression is a cum_count operation and use record_id if possible.

        Parameters
        ----------
        expr : Any
            Expression to analyze
        new_node_id : int
            Node ID to use if creating a record_id node
        description : str, optional
            Description to use for the new node

        Returns
        -------
        Tuple[bool, Optional[FlowFrame]]
            A tuple containing:
            - bool: Whether a cum_count expression was detected and optimized
            - Optional[FlowFrame]: The new FlowFrame if detection was successful, otherwise None
        """
        # Check if this is a cum_count operation
        if (not isinstance(expr, Expr) or not expr._repr_str
                or "cum_count" not in expr._repr_str or not hasattr(expr, "name")):
            return False, None

        # Extract the output name
        output_name = expr.name

        if ".over(" not in expr._repr_str:
            # Simple cumulative count can be implemented as a record ID with offset=1
            record_id_input = transform_schema.RecordIdInput(
                output_column_name=output_name,
                offset=1,
                group_by=False,
                group_by_columns=[],
            )

            # Create node settings
            record_id_settings = input_schema.NodeRecordId(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                record_id_input=record_id_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Add cumulative count as '{output_name}'",
            )

            # Add to graph using native implementation
            self.flow_graph.add_record_id(record_id_settings)
            return True, self._create_child_frame(new_node_id)

        # Check for windowed/partitioned cum_count
        elif ".over(" in expr._repr_str:
            # Try to extract partition columns from different patterns
            partition_columns = []

            # Case 1: Simple string column - .over('column')
            simple_match = re.search(r'\.over\([\'"]([^\'"]+)[\'"]\)', expr._repr_str)
            if simple_match:
                partition_columns = [simple_match.group(1)]

            # Case 2: List of column strings - .over(['col1', 'col2'])
            list_match = re.search(r"\.over\(\[(.*?)\]", expr._repr_str)
            if list_match:
                items = list_match.group(1).split(",")
                for item in items:
                    # Extract string column names from quoted strings
                    col_match = re.search(r'[\'"]([^\'"]+)[\'"]', item.strip())
                    if col_match:
                        partition_columns.append(col_match.group(1))

            # Case 3: pl.col expressions - .over(pl.col('category'), pl.col('abc'))
            col_matches = re.finditer(r'pl\.col\([\'"]([^\'"]+)[\'"]\)', expr._repr_str)
            for match in col_matches:
                partition_columns.append(match.group(1))

            # If we found partition columns, create a grouped record ID
            if partition_columns:
                # Use grouped record ID implementation
                record_id_input = transform_schema.RecordIdInput(
                    output_column_name=output_name,
                    offset=1,
                    group_by=True,
                    group_by_columns=partition_columns,
                )

                # Create node settings
                record_id_settings = input_schema.NodeRecordId(
                    flow_id=self.flow_graph.flow_id,
                    node_id=new_node_id,
                    record_id_input=record_id_input,
                    pos_x=200,
                    pos_y=150,
                    is_setup=True,
                    depending_on_id=self.node_id,
                    description=description
                    or f"Add grouped cumulative count as '{output_name}' by {', '.join(partition_columns)}",
                )

                # Add to graph using native implementation
                self.flow_graph.add_record_id(record_id_settings)
                return True, self._create_child_frame(new_node_id)

        # Not a cum_count we can optimize
        return False, None

    def with_columns(
        self,
        exprs: Expr | List[Expr | None] = None,
        *,
        flowfile_formulas: Optional[List[str]] = None,
        output_column_names: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> "FlowFrame":
        """
        Add multiple columns to the DataFrame.

        Parameters
        ----------
        exprs : Expr or List[Expr], optional
            Expressions to evaluate as new columns
        flowfile_formulas : List[str], optional
            Alternative approach using flowfile formula syntax
        output_column_names : List[str], optional
            Column names for the flowfile formulas
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with the columns added

        Raises
        ------
        ValueError
            If neither exprs nor flowfile_formulas with output_column_names are provided,
            or if the lengths of flowfile_formulas and output_column_names don't match
        """
        if exprs is not None:
            new_node_id = generate_node_id()
            exprs_iterable = _parse_inputs_as_iterable((exprs,))

            if len(exprs_iterable) == 1:
                detected, result = self._detect_cum_count_record_id(
                    exprs_iterable[0], new_node_id, description
                )
                if detected:
                    return result
            all_expressions = []
            for expression in exprs_iterable:
                if not isinstance(expression, (Expr, Column)):
                    all_expressions.append(lit(expression))
                else:
                    all_expressions.append(expression)

            code = (
                f"input_df.with_columns({', '.join(str(e) for e in all_expressions)})"
            )
            self._add_polars_code(new_node_id, code, description)
            return self._create_child_frame(new_node_id)

        elif flowfile_formulas is not None and output_column_names is not None:
            if len(output_column_names) != len(flowfile_formulas):
                raise ValueError(
                    "Length of both the formulas and the output columns names must be identical"
                )

            if len(flowfile_formulas) == 1:
                return self._with_flowfile_formula(flowfile_formulas[0], output_column_names[0], description)
            ff = self
            for i, (flowfile_formula, output_column_name) in enumerate(zip(flowfile_formulas, output_column_names)):
                ff = ff._with_flowfile_formula(flowfile_formula, output_column_name, f"{i}: {description}")
            return ff
        else:
            raise ValueError(
                "Either exprs or flowfile_formulas with output_column_names must be provided"
            )

    def with_row_index(
        self, name: str = "index", offset: int = 0, description: str = None
    ) -> "FlowFrame":
        """
        Add a row index as the first column in the DataFrame.

        Parameters
        ----------
        name : str, default "index"
            Name of the index column.
        offset : int, default 0
            Start the index at this offset. Cannot be negative.
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with the row index column added
        """
        new_node_id = generate_node_id()

        # Check if we can use the native record_id implementation
        if name == "record_id" or (offset == 1 and name != "index"):
            # Create RecordIdInput - no grouping needed
            record_id_input = transform_schema.RecordIdInput(
                output_column_name=name,
                offset=offset,
                group_by=False,
                group_by_columns=[],
            )

            # Create node settings
            record_id_settings = input_schema.NodeRecordId(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                record_id_input=record_id_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Add row index column '{name}'",
            )

            # Add to graph
            self.flow_graph.add_record_id(record_id_settings)
        else:
            # Use the polars code approach for other cases
            code = f"input_df.with_row_index(name='{name}', offset={offset})"
            self._add_polars_code(
                new_node_id, code, description or f"Add row index column '{name}'"
            )

        return self._create_child_frame(new_node_id)

    def explode(
        self,
        columns: str | Column | Iterable[str | Column],
        *more_columns: str | Column,
        description: str = None,
    ) -> "FlowFrame":
        """
        Explode the dataframe to long format by exploding the given columns.

        The underlying columns being exploded must be of the List or Array data type.

        Parameters
        ----------
        columns : str, Column, or Sequence[str, Column]
            Column names, expressions, or a sequence of them to explode
        *more_columns : str or Column
            Additional columns to explode, specified as positional arguments
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with exploded rows
        """
        new_node_id = generate_node_id()

        all_columns = []

        if isinstance(columns, (list, tuple)):
            all_columns.extend(
                [col.name if isinstance(col, Column) else col for col in columns]
            )
        else:
            all_columns.append(columns.name if isinstance(columns, Column) else columns)

        if more_columns:
            for col in more_columns:
                all_columns.append(col.name if isinstance(col, Column) else col)

        if len(all_columns) == 1:
            columns_str = f"'{all_columns[0]}'"
        else:
            columns_str = "[" + ", ".join([f"'{col}'" for col in all_columns]) + "]"

        code = f"""
        # Explode columns into multiple rows
        output_df = input_df.explode({columns_str})
        """

        cols_desc = ", ".join(all_columns)
        desc = description or f"Explode column(s): {cols_desc}"

        # Add polars code node
        self._add_polars_code(new_node_id, code, desc)

        return self._create_child_frame(new_node_id)

    def text_to_rows(
        self,
        column: str | Column,
        output_column: str = None,
        delimiter: str = None,
        split_by_column: str = None,
        description: str = None,
    ) -> "FlowFrame":
        """
        Split text in a column into multiple rows.

        This is equivalent to the explode operation after string splitting in Polars.

        Parameters
        ----------
        column : str or Column
            Column containing text to split
        output_column : str, optional
            Column name for the split values (defaults to input column name)
        delimiter : str, default ','
            String delimiter to split text on when using a fixed value
        split_by_column : str, optional
            Alternative: column name containing the delimiter for each row
            If provided, this overrides the delimiter parameter
        description : str, optional
            Description of this operation for the ETL graph

        Returns
        -------
        FlowFrame
            A new FlowFrame with text split into multiple rows
        """
        new_node_id = generate_node_id()

        if isinstance(column, Column):
            column_name = column.name
        else:
            column_name = column

        output_column = output_column or column_name

        text_to_rows_input = transform_schema.TextToRowsInput(
            column_to_split=column_name,
            output_column_name=output_column,
            split_by_fixed_value=split_by_column is None,
            split_fixed_value=delimiter,
            split_by_column=split_by_column,
        )

        # Create node settings
        text_to_rows_settings = input_schema.NodeTextToRows(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            text_to_rows_input=text_to_rows_input,
            pos_x=200,
            pos_y=150,
            is_setup=True,
            depending_on_id=self.node_id,
            description=description or f"Split text in '{column_name}' to rows",
        )

        # Add to graph
        self.flow_graph.add_text_to_rows(text_to_rows_settings)

        return self._create_child_frame(new_node_id)

    def unique(
        self,
        subset: Union[str, "Expr", List[ Union[ str,  "Expr"]]] = None,
        *,
        keep: Literal["first", "last", "any", "none"] = "any",
        maintain_order: bool = False,
        description: str = None,
    ) -> "FlowFrame":
        """
        Drop duplicate rows from this dataframe.

        Parameters
        ----------
        subset : str, Expr, list of str or Expr, optional
            Column name(s) or selector(s), to consider when identifying duplicate rows.
            If set to None (default), use all columns.
        keep : {'first', 'last', 'any', 'none'}, default 'any'
            Which of the duplicate rows to keep.
            * 'any': Does not give any guarantee of which row is kept.
              This allows more optimizations.
            * 'none': Don't keep duplicate rows.
            * 'first': Keep first unique row.
            * 'last': Keep last unique row.
        maintain_order : bool, default False
            Keep the same order as the original DataFrame. This is more expensive
            to compute. Settings this to True blocks the possibility to run on
            the streaming engine.
        description : str, optional
            Description of this operation for the ETL graph.

        Returns
        -------
        FlowFrame
            DataFrame with unique rows.
        """
        new_node_id = generate_node_id()

        processed_subset = None
        can_use_native = True
        if subset is not None:
            # Convert to list if single item
            if not isinstance(subset, (list, tuple)):
                subset = [subset]

            # Extract column names
            processed_subset = []
            for col_expr in subset:
                if isinstance(col_expr, str):
                    processed_subset.append(col_expr)
                elif isinstance(col_expr, Column):
                    if col_expr._select_input.is_altered:
                        can_use_native = False
                        break
                    processed_subset.append(col_expr.name)
                else:
                    can_use_native = False
                    break

        # Determine if we can use the native implementation
        can_use_native = (
            can_use_native
            and keep in ["any", "first", "last", "none"]
            and not maintain_order
        )

        if can_use_native:
            # Use the native NodeUnique implementation
            unique_input = transform_schema.UniqueInput(
                columns=processed_subset, strategy=keep
            )

            # Create node settings
            unique_settings = input_schema.NodeUnique(
                flow_id=self.flow_graph.flow_id,
                node_id=new_node_id,
                unique_input=unique_input,
                pos_x=200,
                pos_y=150,
                is_setup=True,
                depending_on_id=self.node_id,
                description=description or f"Get unique rows (strategy: {keep})",
            )

            # Add to graph using native implementation
            self.flow_graph.add_unique(unique_settings)
        else:
            # Generate polars code for more complex cases
            if subset is None:
                subset_str = "None"
            elif isinstance(subset, (list, tuple)):
                # Format each item in the subset list
                items = []
                for item in subset:
                    if isinstance(item, str):
                        items.append(f'"{item}"')
                    else:
                        # For expressions, use their string representation
                        items.append(str(item))
                subset_str = f"[{', '.join(items)}]"
            else:
                # Single item that's not a string
                subset_str = str(subset)

            code = f"""
            # Remove duplicate rows
            output_df = input_df.unique(
                subset={subset_str},
                keep='{keep}',
                maintain_order={maintain_order}
            )
            """

            # Create descriptive text based on parameters
            subset_desc = "all columns" if subset is None else f"columns: {subset_str}"
            desc = description or f"Get unique rows using {subset_desc}, keeping {keep}"

            # Add polars code node
            self._add_polars_code(new_node_id, code, desc)

        return self._create_child_frame(new_node_id)

    @property
    def columns(self) -> List[str]:
        """Get the column names."""
        return self.data.collect_schema().names()

    @property
    def dtypes(self) -> List[pl.DataType]:
        """Get the column data types."""
        return self.data.dtypes

    @property
    def schema(self) -> pl.schema.Schema:
        """Get an ordered mapping of column names to their data type."""
        return self.data.schema

    @property
    def width(self) -> int:
        """Get the number of columns."""
        return self.data.width


def _add_delegated_methods():
    """Add delegated methods from polars LazyFrame."""
    delegate_methods = [
        "collect_async",
        "profile",
        "describe",
        "explain",
        "show_graph",
        "serialize",
        "fetch",
        "get_meta",
        "columns",
        "dtypes",
        "schema",
        "estimated_size",
        "n_chunks",
        "is_empty",
        "chunk_lengths",
        "optimization_toggle",
        "set_polars_options",
        "collect_schema"
    ]

    already_implemented = set(dir(FlowFrame))

    for method_name in delegate_methods:
        if method_name not in already_implemented and hasattr(
            pl.LazyFrame, method_name
        ):
            # Create a simple delegate method
            def make_delegate(name):
                def delegate_method(self, *args, **kwargs):
                    return getattr(self.data, name)(*args, **kwargs)

                # Set docstring and name
                delegate_method.__doc__ = (
                    f"See pl.LazyFrame.{name} for full documentation."
                )
                delegate_method.__name__ = name
                return delegate_method

            # Add the method to the class
            setattr(FlowFrame, method_name, make_delegate(method_name))


_add_delegated_methods()


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


def read_csv(file_path, *, flow_graph: FlowGraph = None, separator: str = ';',
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
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id

    has_headers = options.get('has_header', True)
    encoding = options.get('encoding', 'utf-8')

    if '~' in file_path:
        file_path = os.path.expanduser(file_path)

    received_table = input_schema.ReceivedTable(
        file_type='csv',
        path=file_path,
        name=Path(file_path).name,
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


def read_parquet(file_path, *, flow_graph: FlowGraph = None, description: str = None,
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
    if '~' in file_path:
        file_path = os.path.expanduser(file_path)
    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id

    received_table = input_schema.ReceivedTable(
        file_type='parquet',
        path=file_path,
        name=Path(file_path).name,
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


def from_dict(data, *, flow_graph: FlowGraph = None, description: str = None) -> FlowFrame:
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
        flow_graph = create_flow_graph()
    flow_id = flow_graph.flow_id

    input_node = input_schema.NodeManualInput(
        flow_id=flow_id,
        node_id=node_id,
        raw_data=FlowDataEngine(data).to_pylist(),
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


def concat(frames: List['FlowFrame'],
                  how: str = 'vertical',
                  rechunk: bool = False,
                  parallel: bool = True,
                  description: str = None) -> 'FlowFrame':
    """
    Concatenate multiple FlowFrames into one.

    Parameters
    ----------
    frames : List[FlowFrame]
        List of FlowFrames to concatenate
    how : str, default 'vertical'
        How to combine the FlowFrames (see concat method documentation)
    rechunk : bool, default False
        Whether to ensure contiguous memory in result
    parallel : bool, default True
        Whether to use parallel processing for the operation
    description : str, optional
        Description of this operation

    Returns
    -------
    FlowFrame
        A new FlowFrame with the concatenated data
    """
    if not frames:
        raise ValueError("No frames provided to concat_frames")

    if len(frames) == 1:
        return frames[0]

    # Use first frame's concat method with remaining frames
    first_frame = frames[0]
    remaining_frames = frames[1:]

    return first_frame.concat(remaining_frames, how=how,
                              rechunk=rechunk, parallel=parallel,
                              description=description)
