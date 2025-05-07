from typing import Any, Optional, Union, TYPE_CHECKING, List, Literal

import polars as pl
from polars.expr.string import ExprStringNameSpace

from flowfile_core.schemas import transform_schema

# --- TYPE CHECKING IMPORTS ---
if TYPE_CHECKING:
    # Import Selector only for type hints
    from flowfile_core.flowfile.flowfile_frame.selectors import Selector


# --- Helper Functions ---

def _repr_args(*args, **kwargs):
    """Helper to represent arguments for __repr__."""
    arg_reprs = [repr(a) for a in args]
    kwarg_reprs = []
    for k, v in kwargs.items():
        if isinstance(v, pl.DataType):
            kwarg_reprs.append(f"{k}={v!s}")
        elif isinstance(v, type) and issubclass(v, pl.DataType):
            kwarg_reprs.append(f"{k}=pl.{v.__name__}")
        else:
            kwarg_reprs.append(f"{k}={repr(v)}")
    return ", ".join(arg_reprs + kwarg_reprs)


def _get_expr_and_repr(value: Any) -> tuple[Optional[pl.Expr], str]:
    """Helper to get polars expr and repr string for operands."""
    if isinstance(value, Expr):
        # Ensure we return None if the inner expression is None
        inner_expr = value.expr if value.expr is not None else None
        return inner_expr, value._repr_str
    elif isinstance(value, pl.Expr):
        base_str = str(value)
        if base_str.startswith("col("):
            return value, f"pl.{base_str}"
        if base_str.startswith("lit("):
            return value, f"pl.{base_str}"
        return value, f"pl.Expr({base_str})"
    else:
        # Assume literal
        return pl.lit(value), repr(value)


# --- Namespaces ---

class StringMethods:
    expr: Optional[ExprStringNameSpace]

    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.str if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, method_name: str, result_expr: Optional[pl.Expr], *args, **kwargs) -> 'Expr':
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.str.{method_name}({args_repr})"
        # String ops clear selector link but keep agg_func/initial_name
        # Note: String ops themselves aren't aggregations, so they don't set agg_func, just inherit
        new_expr = Expr(result_expr, self.parent.name, repr_str=new_repr,
                        initial_column_name=self.parent._initial_column_name,
                        selector=None,
                        agg_func=self.parent.agg_func)
        return new_expr

    # ... (String methods remain unchanged from your provided code) ...
    def contains(self, pattern, *, literal=False):
        res_expr = self.expr.contains(pattern, literal=literal) if self.expr is not None else None
        return self._create_next_expr("contains", res_expr, pattern, literal=literal)

    def starts_with(self, prefix):
        res_expr = self.expr.starts_with(prefix) if self.expr is not None else None
        return self._create_next_expr("starts_with", res_expr, prefix)

    def ends_with(self, suffix):
        res_expr = self.expr.ends_with(suffix) if self.expr is not None else None
        return self._create_next_expr("ends_with", res_expr, suffix)

    def replace(self, pattern, replacement, *, literal=False):
        res_expr = self.expr.replace(pattern, replacement, literal=literal) if self.expr is not None else None
        return self._create_next_expr("replace", res_expr, pattern, replacement, literal=literal)

    def to_uppercase(self):
        res_expr = self.expr.to_uppercase() if self.expr is not None else None
        return self._create_next_expr("to_uppercase", res_expr)

    def to_lowercase(self):
        res_expr = self.expr.to_lowercase() if self.expr is not None else None
        return self._create_next_expr("to_lowercase", res_expr)

    def len_chars(self):
        res_expr = self.expr.len_chars() if self.expr is not None else None
        return self._create_next_expr("len_chars", res_expr)

    def len_bytes(self):
        res_expr = self.expr.len_bytes() if self.expr is not None else None
        return self._create_next_expr("len_bytes", res_expr)

    def to_titlecase(self):
        res_expr = self.expr.to_titlecase() if self.expr is not None else None
        return self._create_next_expr("to_titlecase", res_expr)

    def __getattr__(self, name):
        if self.expr is None or not hasattr(self.expr, name):
            if self.expr is None:
                raise AttributeError(
                    f"'StringMethods' cannot call '{name}' because underlying expression is not set "
                    f"(e.g., created from selector). Apply aggregation first."
                )
            raise AttributeError(f"'StringMethods' underlying expression has no attribute '{name}'")
        pl_attr = getattr(self.expr, name)
        if callable(pl_attr):
            def wrapper(*args, **kwargs):
                result = pl_attr(*args, **kwargs)
                # Assume generic getattr methods don't change aggregation status
                return self._create_next_expr(name, result, *args, **kwargs)
            return wrapper
        else:
            return pl_attr


class DateTimeMethods:
    expr: Optional[Any] # Polars DateTimeNameSpace

    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.dt if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, method_name: str, result_expr: Optional[pl.Expr], *args, **kwargs) -> 'Expr':
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.dt.{method_name}({args_repr})"
        # Datetime ops clear selector link but keep agg_func/initial_name
        # Note: Datetime ops themselves aren't aggregations, so they don't set agg_func, just inherit
        new_expr = Expr(result_expr, self.parent.name, repr_str=new_repr,
                        initial_column_name=self.parent._initial_column_name,
                        selector=None,
                        agg_func=self.parent.agg_func)
        return new_expr

    # ... (DateTime methods remain unchanged from your provided code) ...
    def year(self):
        res_expr = self.expr.year() if self.expr is not None else None
        return self._create_next_expr("year", res_expr)

    def month(self):
        res_expr = self.expr.month() if self.expr is not None else None
        return self._create_next_expr("month", res_expr)

    def day(self):
        res_expr = self.expr.day() if self.expr is not None else None
        return self._create_next_expr("day", res_expr)

    def hour(self):
        res_expr = self.expr.hour() if self.expr is not None else None
        return self._create_next_expr("hour", res_expr)

    def minute(self):
        res_expr = self.expr.minute() if self.expr is not None else None
        return self._create_next_expr("minute", res_expr)

    def second(self):
        res_expr = self.expr.second() if self.expr is not None else None
        return self._create_next_expr("second", res_expr)

    def __getattr__(self, name):
        if self.expr is None or not hasattr(self.expr, name):
            if self.expr is None:
                raise AttributeError(
                    f"'DateTimeMethods' cannot call '{name}' because underlying expression is not set "
                    f"(e.g., created from selector). Apply aggregation first."
                )
            raise AttributeError(f"'DateTimeMethods' underlying expression has no attribute '{name}'")
        pl_attr = getattr(self.expr, name)
        if callable(pl_attr):
            def wrapper(*args, **kwargs):
                result = pl_attr(*args, **kwargs)
                # Assume generic getattr methods don't change aggregation status
                return self._create_next_expr(name, result, *args, **kwargs)
            return wrapper
        else:
            return pl_attr


class Expr:
    _initial_column_name: Optional[str]
    selector: Optional['Selector']
    expr: Optional[pl.Expr]
    agg_func: Optional[str]
    _repr_str: str
    name: Optional[str]

    def __init__(self,
                 expr: Optional[pl.Expr],
                 column_name: Optional[str] = None,
                 repr_str: Optional[str] = None,
                 initial_column_name: Optional[str] = None,
                 selector: Optional['Selector'] = None,
                 agg_func: Optional[str] = None, # Allow setting agg_func during init
                 ddof: Optional[int] = None):

        self.expr = expr
        self.name = column_name
        self.agg_func = agg_func # Initialize agg_func
        self.selector = selector
        self._initial_column_name = initial_column_name or column_name

        # --- Determine Representation String ---
        if repr_str is not None:
            self._repr_str = repr_str
        elif self.selector is not None and self.agg_func is not None:
            selector_repr = self.selector._repr_str
            func_name = self.agg_func
            kwargs_dict = {}
            if func_name in ("std", "var") and ddof is not None:
                kwargs_dict['ddof'] = ddof
            kwargs_repr = _repr_args(**kwargs_dict)
            self._repr_str = f"{selector_repr}.{func_name}({kwargs_repr})"
            self.expr = None
        elif self.selector is not None:
            self._repr_str = f"{self.selector._repr_str}"
            self.expr = None
        elif self.expr is not None:
            _, default_repr = _get_expr_and_repr(self.expr)
            self._repr_str = default_repr
        else:
             # Allow creation with None expr if agg_func and selector are set later?
             # Or should selector/agg_func always imply None expr initially?
             # Let's stick to the original logic: must have one way to init.
            raise ValueError("Cannot initialize Expr without expr, repr_str, or selector+agg_func")


        if self.name is None and self.selector is None and self.expr is not None:
            try:
                self.name = self.expr._output_name
            except AttributeError:
                try:
                    self.name = self.expr._name
                except AttributeError:
                    pass

        self._str_namespace: Optional['StringMethods'] = None
        self._dt_namespace: Optional['DateTimeMethods'] = None

    def __repr__(self) -> str:
        return self._repr_str

    def _create_next_expr(self, method_name: str, result_expr: Optional[pl.Expr], *args, **kwargs) -> 'Expr':
        """Creates a new Expr instance, appending method call to repr string."""
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self._repr_str}.{method_name}({args_repr})"

        # Create new instance, inheriting current agg_func status by default
        new_expr_instance = Expr(result_expr, self.name, repr_str=new_repr,
                                 initial_column_name=self._initial_column_name,
                                 selector=None, # Chained ops lose selector link
                                 agg_func=self.agg_func) # Inherit agg_func
        return new_expr_instance

    def _create_binary_op_expr(self, op_symbol: str, other: Any, result_expr: Optional[pl.Expr]) -> 'Expr':
        """Creates a new Expr for binary operations."""
        if self.expr is None:
            raise ValueError(
                f"Cannot perform binary operation '{op_symbol}' on Expr without underlying polars expression."
            )

        other_expr, other_repr = _get_expr_and_repr(other)

        if other_expr is None and not isinstance(other, (int, float, str, bool, type(None))):
             raise ValueError(
                 f"Cannot perform binary operation '{op_symbol}' with operand without underlying polars expression or literal value: {other_repr}"
             )

        left_repr = f"({self._repr_str})" if ' ' in self._repr_str else self._repr_str
        right_repr = f"({other_repr})" if (' ' in other_repr or '(' in other_repr) else other_repr
        new_repr = f"{left_repr} {op_symbol} {right_repr}"

        # Binary ops clear the aggregation state and selector link
        return Expr(result_expr, None, repr_str=new_repr,
                    initial_column_name=self._initial_column_name,
                    selector=None, agg_func=None) # Reset agg_func

    @property
    def str(self) -> StringMethods:
        if self._str_namespace is None:
            self._str_namespace = StringMethods(self, self._repr_str)
        return self._str_namespace

    @property
    def dt(self) -> DateTimeMethods:
        if self._dt_namespace is None:
            self._dt_namespace = DateTimeMethods(self, self._repr_str)
        return self._dt_namespace

    # --- Aggregation methods ---
    # ****** MODIFIED SECTION START ******
    def sum(self):
        res_expr = self.expr.sum() if self.expr is not None else None
        result = self._create_next_expr("sum", res_expr)
        result.agg_func = "sum" # Explicitly set agg_func for this operation
        return result

    def mean(self):
        res_expr = self.expr.mean() if self.expr is not None else None
        result = self._create_next_expr("mean", res_expr)
        result.agg_func = "mean" # Explicitly set agg_func
        return result

    def min(self):
        res_expr = self.expr.min() if self.expr is not None else None
        result = self._create_next_expr("min", res_expr)
        result.agg_func = "min" # Explicitly set agg_func
        return result

    def max(self):
        res_expr = self.expr.max() if self.expr is not None else None
        result = self._create_next_expr("max", res_expr)
        result.agg_func = "max" # Explicitly set agg_func
        return result

    def median(self):
        res_expr = self.expr.median() if self.expr is not None else None
        result = self._create_next_expr("median", res_expr)
        result.agg_func = "median" # Explicitly set agg_func
        return result

    def count(self):
        res_expr = self.expr.count() if self.expr is not None else None
        result = self._create_next_expr("count", res_expr)
        result.agg_func = "count" # Explicitly set agg_func
        return result

    def first(self):
        res_expr = self.expr.first() if self.expr is not None else None
        result = self._create_next_expr("first", res_expr)
        result.agg_func = "first" # Explicitly set agg_func
        return result

    def last(self):
        res_expr = self.expr.last() if self.expr is not None else None
        result = self._create_next_expr("last", res_expr)
        result.agg_func = "last" # Explicitly set agg_func
        return result

    def n_unique(self):
        res_expr = self.expr.n_unique() if self.expr is not None else None
        result = self._create_next_expr("n_unique", res_expr)
        result.agg_func = "n_unique" # Explicitly set agg_func
        return result

    def std(self, ddof=1):
        res_expr = self.expr.std(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr("std", res_expr, ddof=ddof)
        result.agg_func = "std" # Explicitly set agg_func
        return result

    def cum_count(self, reverse: bool = False) -> "Expr":
        """
        Return the cumulative count of the non-null values in the column.

        Parameters
        ----------
        reverse : bool, default False
            Reverse the operation

        Returns
        -------
        Expr
            A new expression with the cumulative count
        """
        res_expr = (
            self.expr.cum_count(reverse=reverse) if self.expr is not None else None
        )
        result = self._create_next_expr("cum_count", res_expr, reverse=reverse)
        result.agg_func = None
        return result

    def var(self, ddof=1):
        res_expr = self.expr.var(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr("var", res_expr, ddof=ddof)
        result.agg_func = "var" # Explicitly set agg_func
        return result
    # ****** MODIFIED SECTION END ******

    # --- Arithmetic operations ---
    def __add__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr + other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("+", other, res_expr)

    def __sub__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr - other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("-", other, res_expr)

    def __mul__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr * other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("*", other, res_expr)

    def __truediv__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr / other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("/", other, res_expr)

    def __floordiv__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr // other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("//", other, res_expr)

    def __pow__(self, exponent):
        exp_expr, _ = _get_expr_and_repr(exponent)
        res_expr = self.expr.pow(exp_expr) if self.expr is not None and exp_expr is not None else None
        return self._create_binary_op_expr("**", exponent, res_expr)

    def __mod__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr % other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("%", other, res_expr)

    # --- Right-side Arithmetic ---
    def __radd__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} + {self._repr_str}"
        res_expr = other_expr + self.expr if other_expr is not None and self.expr is not None else None
        # Right-side ops also clear agg_func
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rsub__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} - {self._repr_str}"
        res_expr = other_expr - self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rmul__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} * {self._repr_str}"
        res_expr = other_expr * self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rtruediv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} / {self._repr_str}"
        res_expr = other_expr / self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rfloordiv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} // {self._repr_str}"
        res_expr = other_expr // self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rmod__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} % {self._repr_str}"
        res_expr = other_expr % self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    def __rpow__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} ** {self._repr_str}"
        base_expr = pl.lit(other) if not isinstance(other, (Expr, pl.Expr)) else other_expr
        res_expr = base_expr.pow(self.expr) if self.expr is not None and base_expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None)

    # --- Comparison operations ---
    def __eq__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr == other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("==", other, res_expr)

    def __ne__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr != other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("!=", other, res_expr)

    def __gt__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr > other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr(">", other, res_expr)

    def __lt__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr < other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("<", other, res_expr)

    def __ge__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr >= other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr(">=", other, res_expr)

    def __le__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr <= other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("<=", other, res_expr)

    # --- Logical operations ---
    def __and__(self, other):
        from flowfile_core.flowfile.flowfile_frame.selectors import Selector # Local import
        if isinstance(other, Selector):
            raise TypeError("Unsupported operation: Expr & Selector")
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr & other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("&", other, res_expr)

    def __or__(self, other):
        from flowfile_core.flowfile.flowfile_frame.selectors import Selector # Local import
        if isinstance(other, Selector):
            raise TypeError("Unsupported operation: Expr | Selector")
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr | other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("|", other, res_expr)

    def __invert__(self):
        new_repr = f"~({self._repr_str})"
        res_expr = ~self.expr if self.expr is not None else None
        # Invert clears agg_func
        return Expr(res_expr, None, repr_str=new_repr,
                    initial_column_name=self._initial_column_name, agg_func=None)

    # --- Other useful methods ---
    def is_null(self):
        res_expr = self.expr.is_null() if self.expr is not None else None
        # is_null is not an aggregation, resets agg_func
        result = self._create_next_expr("is_null", res_expr)
        result.agg_func = None
        return result


    def is_not_null(self):
        res_expr = self.expr.is_not_null() if self.expr is not None else None
         # is_not_null is not an aggregation, resets agg_func
        result = self._create_next_expr("is_not_null", res_expr)
        result.agg_func = None
        return result

    def is_in(self, values):
        res_expr = self.expr.is_in(values) if self.expr is not None else None
        # is_in is not an aggregation, resets agg_func
        result = self._create_next_expr("is_in", res_expr, values)
        result.agg_func = None
        return result

    def alias(self, name):
        """Rename the expression result."""
        new_pl_expr = self.expr.alias(name) if self.expr is not None else None
        new_repr = f"{self._repr_str}.alias({repr(name)})"
        # Alias preserves aggregation status
        new_instance = Expr(new_pl_expr, name, repr_str=new_repr,
                            initial_column_name=self._initial_column_name,
                            selector=None,
                            agg_func=self.agg_func) # Preserve agg_func
        return new_instance

    def fill_null(self, value):
        res_expr = self.expr.fill_null(value) if self.expr is not None else None
        # fill_null is not an aggregation, resets agg_func
        result = self._create_next_expr("fill_null", res_expr, value)
        result.agg_func = None
        return result

    def fill_nan(self, value):
        res_expr = None
        if self.expr is not None and hasattr(self.expr, 'fill_nan'):
             res_expr = self.expr.fill_nan(value)
        # fill_nan is not an aggregation, resets agg_func
        result = self._create_next_expr("fill_nan", res_expr, value)
        result.agg_func = None
        return result

    def _get_expr_repr(self, expr):
        """Helper to get appropriate string representation for an expression"""
        # Ensure this helper is robust or defined if it's used as self._get_expr_repr
        if isinstance(expr, (Expr, Column)): # Assuming Column is a subclass of Expr or similar
            return expr._repr_str
        elif isinstance(expr, str): # Should ideally be pl.col() or lit() for consistency
            return f"pl.col('{expr}')" # Or handle more generically
        elif isinstance(expr, pl.Expr):
             base_str = str(expr)
             if base_str.startswith("col("):
                 return f"pl.{base_str}"
             if base_str.startswith("lit("):
                 return f"pl.{base_str}"
             return f"pl.Expr({base_str})"
        else:
            return repr(expr)


    def over(
        self,
        partition_by: Union["Expr", str, List[Union["Expr", str]]],
        *more_exprs: Union["Expr", str],
        order_by: Optional[Union["Expr", str, List[Union["Expr", str]]]] = None,
        descending: bool = False,
        nulls_last: bool = False,
        mapping_strategy: Literal["group_to_rows", "join", "explode"] = "group_to_rows",
    ) -> "Expr":
        """
        Compute expressions over the given groups.
        String representation will show 'descending' and 'nulls_last' if they are True,
        regardless of 'order_by' presence.
        """
        # Process all partition columns (partition_by + more_exprs)
        all_partition_cols = [partition_by]
        if more_exprs:
            all_partition_cols.extend(more_exprs)

        processed_partition_cols = []
        for col_expr in all_partition_cols:
            if isinstance(col_expr, str):
                processed_partition_cols.append(col(col_expr)) # Use your col() factory
            elif isinstance(col_expr, list):
                processed_list = []
                for item in col_expr:
                    if isinstance(item, str):
                        processed_list.append(col(item)) # Use your col() factory
                    else:
                        processed_list.append(item)
                processed_partition_cols.extend(processed_list)
            else:
                processed_partition_cols.append(col_expr) # Assumes it's already an Expr-like object

        processed_order_by = None
        if order_by is not None:
            if isinstance(order_by, str):
                processed_order_by = col(order_by) # Use your col() factory
            elif isinstance(order_by, list):
                processed_order_by = [
                    col(o) if isinstance(o, str) else o for o in order_by
                ]
            else:
                processed_order_by = order_by # Assumes it's already an Expr-like object


        # --- Build string representation for .over() arguments ---
        over_arg_strings_for_repr = []

        # Handle partition_by representation
        if len(processed_partition_cols) == 1:
            over_arg_strings_for_repr.append(self._get_expr_repr(processed_partition_cols[0]))
        else:
            col_reprs = [self._get_expr_repr(p) for p in processed_partition_cols]
            over_arg_strings_for_repr.append(f"[{', '.join(col_reprs)}]")

        # Handle keyword-like arguments for string representation
        # order_by
        if processed_order_by is not None:
            if isinstance(processed_order_by, list):
                order_by_repr_val = f"[{', '.join([self._get_expr_repr(o) for o in processed_order_by])}]"
            else:
                order_by_repr_val = self._get_expr_repr(processed_order_by)
            over_arg_strings_for_repr.append(f"order_by={order_by_repr_val}")

        # descending (show in string if True, regardless of order_by)
        if descending: # `descending` is the boolean method parameter
            over_arg_strings_for_repr.append(f"descending={repr(descending)}") # repr(True) -> "True"

        # nulls_last (show in string if True, regardless of order_by)
        if nulls_last: # `nulls_last` is the boolean method parameter
            over_arg_strings_for_repr.append(f"nulls_last={repr(nulls_last)}")

        # mapping_strategy (show if not default)
        if mapping_strategy != "group_to_rows":
            over_arg_strings_for_repr.append(f"mapping_strategy='{mapping_strategy}'")

        args_str_for_repr = ", ".join(over_arg_strings_for_repr)

        # --- Create the actual polars expression (THIS LOGIC REMAINS UNCHANGED) ---
        res_expr = None
        if self.expr is not None:
            try:
                # Convert partition_by to Polars expressions
                if len(processed_partition_cols) == 1:
                    partition_arg = (
                        processed_partition_cols[0].expr
                        if hasattr(processed_partition_cols[0], "expr")
                        else processed_partition_cols[0] # Fallback if not your Expr type
                    )
                else:
                    partition_arg = [
                        p.expr if hasattr(p, "expr") else p
                        for p in processed_partition_cols
                    ]

                # Build kwargs for the actual polars over() call
                polars_call_kwargs = {"mapping_strategy": mapping_strategy}

                if processed_order_by is not None:
                    # Convert order_by to Polars expressions
                    if isinstance(processed_order_by, list):
                        polars_order_by_arg = [
                            o.expr if hasattr(o, "expr") else o
                            for o in processed_order_by
                        ]
                    else:
                        polars_order_by_arg = (
                            processed_order_by.expr
                            if hasattr(processed_order_by, "expr")
                            else processed_order_by
                        )
                    polars_call_kwargs["order_by"] = polars_order_by_arg
                    # These are tied to order_by for the actual Polars call
                    polars_call_kwargs["descending"] = descending
                    polars_call_kwargs["nulls_last"] = nulls_last

                res_expr = self.expr.over(partition_by=partition_arg, **polars_call_kwargs)

            except Exception as e:
                # It's good practice to either log this or allow it to propagate
                # if it's unexpected, rather than just printing.
                # For now, keeping your print statement.
                print(f"Warning: Could not create polars expression for over(): {e}")
                pass # res_expr will remain None

        # Window functions (over) clear the simple aggregation state
        return Expr(
            res_expr,
            self.name,
            repr_str=f"{self._repr_str}.over({args_str_for_repr})", # Use the modified string
            initial_column_name=self._initial_column_name,
            selector=None, # .over() typically removes selector link
            agg_func=None, # Window functions reset agg_func
        )
    def sort(self, *, descending=False, nulls_last=False):
        res_expr = self.expr.sort(descending=descending, nulls_last=nulls_last) if self.expr is not None else None
        # Sort is not an aggregation, resets agg_func
        return Expr(res_expr, self.name,
                    repr_str=f"{self._repr_str}.sort(descending={descending}, nulls_last={nulls_last})",
                    initial_column_name=self._initial_column_name, agg_func=None)

    def cast(self, dtype: Union[pl.DataType, str, pl.datatypes.classes.DataTypeClass], *, strict=True):
        """ Casts the Expr to a specified data type. """
        pl_dtype = dtype
        dtype_repr = repr(dtype)

        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}"
            except AttributeError:
                pass
        elif hasattr(dtype, '__name__'):
            dtype_repr = f"pl.{dtype.__name__}"
        elif isinstance(dtype, pl.DataType):
             dtype_repr = f"pl.{dtype!s}"

        res_expr = self.expr.cast(pl_dtype, strict=strict) if self.expr is not None else None
        # Cast preserves aggregation status (e.g., cast(col('a').sum()))
        new_expr = Expr(res_expr, self.name,
                        repr_str=f"{self._repr_str}.cast({dtype_repr}, strict={strict})",
                        initial_column_name=self._initial_column_name,
                        selector=None,
                        agg_func=self.agg_func) # Preserve agg_func
        return new_expr


class Column(Expr):
    """Special Expr representing a single column, preserving column identity through alias/cast."""
    _select_input: transform_schema.SelectInput

    def __init__(self, name: str, select_input: Optional[transform_schema.SelectInput] = None):
        super().__init__(expr=pl.col(name),
                         column_name=name,
                         repr_str=f"pl.col('{name}')",
                         initial_column_name=select_input.old_name if select_input else name,
                         selector=None,
                         agg_func=None) # Columns start with no agg func
        self._select_input = select_input or transform_schema.SelectInput(old_name=name)

    def alias(self, new_name: str) -> "Column":
        """Rename a column, returning a new Column instance."""
        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=new_name,
            data_type=self._select_input.data_type,
            data_type_change=self._select_input.data_type_change,
            is_altered=True
        )
        if self.expr is None: # Should not happen for Column
            raise ValueError("Cannot alias Column without underlying polars expression.")

        new_pl_expr = self.expr.alias(new_name)
        new_repr = f"{self._repr_str}.alias({repr(new_name)})"

        new_column = Column(new_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        # Alias on Column preserves that it's effectively still the 'same' column conceptually
        # Thus, agg_func remains None unless explicitly aggregated later.
        new_column.agg_func = self.agg_func # Should be None initially
        return new_column

    def cast(self, dtype: Union[pl.DataType, str, pl.datatypes.classes.DataTypeClass], *, strict=True) -> "Column":
        """Change the data type of a column, returning a new Column instance."""
        pl_dtype = dtype
        dtype_repr = repr(dtype)

        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}"
            except AttributeError:
                pass
        elif hasattr(dtype, '__name__'):
            dtype_repr = f"pl.{dtype.__name__}"
        elif isinstance(dtype, pl.DataType):
             dtype_repr = f"pl.{dtype!s}"

        if not isinstance(pl_dtype, pl.DataType):
            try:
                pl_dtype_instance = pl_dtype()
                if isinstance(pl_dtype_instance, pl.DataType):
                    pl_dtype = pl_dtype_instance
            except TypeError:
                 raise TypeError(f"Invalid Polars data type specified for cast: {dtype}")

        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=self._select_input.new_name,
            data_type=str(pl_dtype),
            data_type_change=True,
            is_altered=True
        )
        if self.expr is None: # Should not happen for Column
            raise ValueError("Cannot cast Column without underlying polars expression.")

        new_pl_expr = self.expr.cast(pl_dtype, strict=strict)
        new_repr = f"{self._repr_str}.cast({dtype_repr}, strict={strict})"
        display_name = self._select_input.new_name or self._select_input.old_name

        new_column = Column(display_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
         # Cast on Column preserves that it's effectively still the 'same' column conceptually
        new_column.agg_func = self.agg_func # Should be None initially
        return new_column

    def to_select_input(self) -> transform_schema.SelectInput:
        """Convert Column state back to a SelectInput schema object."""
        # This logic seems correct based on your previous version
        current_name = self.name
        original_name = self._select_input.old_name
        new_name_attr = self._select_input.new_name

        final_new_name = current_name if current_name != original_name else new_name_attr
        final_data_type = self._select_input.data_type if self._select_input.data_type_change else None
        final_data_type_change = bool(final_data_type)
        final_is_altered = bool(final_new_name or final_data_type_change)

        return transform_schema.SelectInput(
            old_name=original_name,
            new_name=final_new_name,
            data_type=final_data_type,
            data_type_change=final_data_type_change,
            is_altered=final_is_altered
        )

    # Properties return base Expr instances but use the column's repr
    @property
    def str(self) -> StringMethods:
        return super().str # Uses correct self._repr_str

    @property
    def dt(self) -> DateTimeMethods:
        return super().dt # Uses correct self._repr_str


# --- Top-Level Functions ---
def col(name: str) -> Column:
    """Creates a Column expression."""
    return Column(name)


def column(name: str) -> Column:
    """Alias for col(). Creates a Column expression."""
    return Column(name)


def lit(value: Any) -> Expr:
    """Creates a Literal expression."""
    # Literals don't have an agg_func
    return Expr(pl.lit(value), repr_str=f"pl.lit({repr(value)})", agg_func=None)


def cum_count(expr, reverse: bool = False) -> Expr:
    """
    Return the cumulative count of the non-null values in the column.

    Parameters
    ----------
    expr : str or Expr
        Expression to compute cumulative count on
    reverse : bool, default False
        Reverse the operation

    Returns
    -------
    Expr
        A new expression with the cumulative count
    """
    if isinstance(expr, str):
        expr = col(expr)
    return expr.cum_count(reverse=reverse)
