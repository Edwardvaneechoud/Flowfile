from flowfile_frame.expr import col, Expr, Column, lit
from flowfile_frame.selectors import Selector
from flowfile_frame.utils import _parse_inputs_as_iterable
from flowfile_core.schemas import transform_schema, input_schema
from typing import TYPE_CHECKING, Any
from flowfile_frame.utils import _check_if_convertible_to_code, ensure_inputs_as_iterable, get_pl_expr_from_expr

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame
else:
    FlowFrame = None


class GroupByFrame:
    """Represents a grouped DataFrame for aggregation operations."""

    def __init__(self, node_id: int, parent_frame, by_cols, maintain_order=False, description: str = None):
        """Initialize a GroupByFrame instance.

        Args:
            node_id: Unique identifier for this node
            parent_frame: The parent FlowFrame this group operation is applied to
            by_cols: Columns to group by
            maintain_order: Whether to maintain original order
            description: Optional description for this operation
        """
        self.parent = parent_frame
        self.by_cols = _parse_inputs_as_iterable(by_cols)
        self.expr_by_cols = [self._create_expr_col(c) for c in self.by_cols]
        self.maintain_order = maintain_order
        self.description = description
        self.node_id = node_id

    @staticmethod
    def _create_expr_col(col_: Any) -> Expr:
        """Convert various column specifications to Expr objects."""
        if isinstance(col_, str):
            return col(col_)
        elif isinstance(col_, (Column, Expr)):
            return col_
        else:
            return lit(col_)

    def readable_group(self):
        """Generate a readable string representation of grouping columns."""
        parts = []
        for c in self.by_cols:
            if isinstance(c, Expr):
                parts.append(str(c))
            elif isinstance(c, str):
                parts.append(f'''"{c}"''')
            else:
                parts.append(f'''"{str(c)}"''')
        return ", ".join(parts)

    def len(self) -> 'FlowFrame':
        """Count number of rows per group. Output column is named 'len'."""
        return self._generate_direct_polars_code("len")

    def count(self) -> 'FlowFrame':
        """Count number of rows per group. Output column is named 'count'."""
        return self._generate_direct_polars_code("count")

    def agg(self, *agg_exprs, **named_agg_exprs) -> FlowFrame:
        """Apply explicit aggregations to grouped data using expressions.

        Args:
            *agg_exprs: Aggregation expressions to apply
            **named_agg_exprs: Named aggregation expressions

        Returns:
            FlowFrame: New frame with aggregated results
        """
        agg_expressions = _parse_inputs_as_iterable(agg_exprs)
        convertable_to_code = _check_if_convertible_to_code(agg_expressions)
        can_be_converted: bool = not self.maintain_order
        agg_cols: list[transform_schema.AggColl] = []
        if can_be_converted:
            can_be_converted = self._process_group_columns(agg_cols)
        if can_be_converted:
            can_be_converted = self._process_agg_expressions(agg_cols, agg_expressions)
        if can_be_converted:
            can_be_converted = self._process_named_agg_expressions(agg_cols, named_agg_exprs)
        node_desc = self.description or f"Aggregate after grouping by {self.readable_group()}"
        return self._create_agg_node(self.node_id, can_be_converted, agg_cols, agg_expressions, named_agg_exprs,
                                     convertable_to_code=convertable_to_code, description=node_desc)

    def _process_group_columns(self, agg_cols: list[transform_schema.AggColl]) -> bool:
        """Process grouping columns for aggregation schema."""
        for col_expr in self.by_cols:
            if isinstance(col_expr, str):
                agg_cols.append(transform_schema.AggColl(old_name=col_expr, agg="groupby"))
            elif isinstance(col_expr, Expr):
                agg_cols.append(transform_schema.AggColl(old_name=col_expr.column_name, agg="groupby"))
            elif isinstance(col_expr, Selector):
                return False
            else:
                return False
        return True

    @staticmethod
    def _process_agg_expressions(agg_cols: list[transform_schema.AggColl], agg_expressions) -> bool:
        """Process aggregation expressions for schema conversion."""
        for expr in agg_expressions:
            if isinstance(expr, Expr):
                if expr.is_complex:
                    return False
                agg_func = getattr(expr, "agg_func", None)
                old_name = getattr(expr, "_initial_column_name", expr.column_name) or expr.column_name
                if agg_func:
                    agg_cols.append(
                        transform_schema.AggColl(old_name=old_name, agg=agg_func, new_name=expr.column_name or old_name)
                    )
                else:
                    agg_cols.append(transform_schema.AggColl(old_name=expr.column_name, agg="first"))
            elif isinstance(expr, str):
                agg_cols.append(transform_schema.AggColl(old_name=expr, agg="first"))
            elif isinstance(expr, Selector):
                return False
            else:
                return False
        return True

    @staticmethod
    def _process_named_agg_expressions(agg_cols: list[transform_schema.AggColl], named_agg_exprs: dict) -> bool:
        """Process named aggregation expressions for schema conversion."""
        for name, expr in named_agg_exprs.items():
            if expr.is_complex:
                return False
            if isinstance(expr, Expr):
                agg_func = getattr(expr, "agg_func", "first")
                old_name = getattr(expr, "_initial_column_name", expr.column_name) or expr.column_name
                agg_cols.append(transform_schema.AggColl(old_name=old_name, agg=agg_func, new_name=name))
            elif isinstance(expr, str):
                agg_cols.append(transform_schema.AggColl(old_name=expr, agg="first", new_name=name))
            elif isinstance(expr, tuple) and len(expr) == 2:
                col_spec, agg_func_str = expr
                if isinstance(col_spec, Expr):
                    old_name = getattr(col_spec, "_initial_column_name", col_spec.column_name) or col_spec.column_name
                elif isinstance(col_spec, str):
                    old_name = col_spec
                else:
                    return False
                if not isinstance(agg_func_str, str):
                    return False
                agg_cols.append(transform_schema.AggColl(old_name=old_name, agg=agg_func_str, new_name=name))
            else:
                return False
        return True

    def _create_agg_node(self, node_id_to_use: int, can_be_converted: bool, agg_cols: list, agg_expressions,
                         named_agg_exprs, convertable_to_code: bool, description: str):
        """Create node for explicit aggregations via self.agg()."""
        if can_be_converted:
            group_by_settings = input_schema.NodeGroupBy(
                flow_id=self.parent.flow_graph.flow_id,
                node_id=node_id_to_use,
                groupby_input=transform_schema.GroupByInput(agg_cols=agg_cols),
                pos_x=200, pos_y=200, is_setup=True,
                depending_on_id=self.parent.node_id,
                description=description
            )
            self.parent.flow_graph.add_group_by(group_by_settings)
        else:
            code = self._generate_polars_agg_code(agg_expressions, named_agg_exprs)
            pl_agg_expressions = list(map(get_pl_expr_from_expr, ensure_inputs_as_iterable(agg_expressions)))
            pl_group_expr = list(map(get_pl_expr_from_expr, ensure_inputs_as_iterable(self.expr_by_cols)))
            pl_kwargs_expr = {k: self._create_expr_col(c).expr for k, c in named_agg_exprs.items()}
            self.parent._add_polars_code(new_node_id=node_id_to_use, code=code, description=description,
                                         method_name='group_by', convertable_to_code=convertable_to_code,
                                         polars_expr=pl_agg_expressions, group_expr=pl_group_expr,
                                         kwargs_expr=pl_kwargs_expr,
                                         group_kwargs={'maintain_order': self.maintain_order})
        return self.parent._create_child_frame(node_id_to_use)

    def _generate_direct_polars_code(self, method_name: str, *args, **kwargs) -> "FlowFrame":
        """Generate Polars code for simple GroupBy methods like sum(), mean(), len(), count().

        Args:
            method_name: Name of the aggregation method
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method

        Returns:
            FlowFrame: New child frame with the operation applied
        """
        readable_group_str = self.readable_group()
        execution = "(" + ",".join(args) + ",".join([f"{k}={v}" for k, v in kwargs.items()]) + ")"

        code = f"input_df.group_by([{readable_group_str}], maintain_order={self.maintain_order}).{method_name}{execution}"
        node_description = self.description or f"{method_name.capitalize()} after grouping by {readable_group_str}"
        self.parent._add_polars_code(new_node_id=self.node_id, code=code, description=node_description)
        return self.parent._create_child_frame(self.node_id)

    def _generate_polars_agg_code(self, agg_expressions, named_agg_exprs) -> str:
        """Generate Polars code specifically for explicit .agg() calls."""
        readable_group_str = self.readable_group()
        agg_strs = [str(expr) for expr in agg_expressions]
        named_agg_strs = [f"{name}={str(expr)}" for name, expr in named_agg_exprs.items()]
        all_agg_strs = agg_strs + named_agg_strs
        agg_combined = ", ".join(all_agg_strs)
        return f"input_df.group_by([{readable_group_str}], maintain_order={self.maintain_order}).agg({agg_combined})"

    def sum(self):
        """Calculate sum for all non-grouping columns."""
        return self._generate_direct_polars_code("sum")

    def mean(self):
        """Calculate mean for all non-grouping columns."""
        return self._generate_direct_polars_code("mean")

    def median(self):
        """Calculate median for all non-grouping columns."""
        return self._generate_direct_polars_code("median")

    def min(self):
        """Calculate minimum for all non-grouping columns."""
        return self._generate_direct_polars_code("min")

    def max(self):
        """Calculate maximum for all non-grouping columns."""
        return self._generate_direct_polars_code("max")

    def tail(self, n: int = 10):
        """Get last n rows for all non-grouping columns.

        Args:
            n: Number of rows to return (default: 10)
        """
        return self._generate_direct_polars_code("tail", n=n)

    def head(self, n: int = 10):
        """Get the first n rows for all non-grouping columns.

        Args:
            n: Number of rows to return (default: 10)
        """
        return self._generate_direct_polars_code("head", n=n)

    def first(self):
        """Get first value for all non-grouping columns."""
        return self._generate_direct_polars_code("first")

    def last(self):
        """Get last value for all non-grouping columns."""
        return self._generate_direct_polars_code("last")