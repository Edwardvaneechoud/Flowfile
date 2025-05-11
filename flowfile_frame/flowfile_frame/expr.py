from __future__ import annotations

from typing import Any, Optional, Union, TYPE_CHECKING, List, Literal, TypeVar

import polars as pl
from polars.expr.string import ExprStringNameSpace

from flowfile_core.schemas import transform_schema

from builtins import len as built_in_len

# --- TYPE CHECKING IMPORTS ---
if TYPE_CHECKING:
    from flowfile_frame.selectors import Selector
    ExprType = TypeVar('ExprType', bound='Expr')
    ColumnType = "Column"  # Use string literal instead of direct class reference

ExprOrStr = Union['Expr', str]
ExprOrStrList = List[ExprOrStr]
ExprStrOrList = Union[ExprOrStr, ExprOrStrList]


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

    def _create_next_expr(self, *args,  method_name: str, result_expr: Optional[pl.Expr], is_complex: bool, **kwargs) -> 'Expr':
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.str.{method_name}({args_repr})"
        new_expr = Expr(result_expr, self.parent.name, repr_str=new_repr,
                        initial_column_name=self.parent._initial_column_name,
                        selector=None,
                        agg_func=self.parent.agg_func,
                        is_complex=is_complex)
        return new_expr

    # ... (String methods remain unchanged from your provided code) ...
    def contains(self, pattern, *, literal=False):
        res_expr = self.expr.contains(pattern, literal=literal) if self.expr is not None else None
        return self._create_next_expr(pattern, literal=literal, method_name="contains", result_expr=res_expr, is_complex=True)

    def starts_with(self, prefix):
        res_expr = self.expr.starts_with(prefix) if self.expr is not None else None
        return self._create_next_expr(prefix, is_complex=True, method_name="starts_with", result_expr=res_expr)

    def ends_with(self, suffix):
        res_expr = self.expr.ends_with(suffix) if self.expr is not None else None
        return self._create_next_expr(suffix,  result_expr=res_expr, method_name="ends_with", is_complex=True)

    def replace(self, pattern, replacement, *, literal=False):
        res_expr = self.expr.replace(pattern, replacement, literal=literal) if self.expr is not None else None
        return self._create_next_expr(pattern, replacement, method_name="replace",
                                      result_expr=res_expr, literal=literal, is_complex=True)

    def to_uppercase(self):
        res_expr = self.expr.to_uppercase() if self.expr is not None else None
        return self._create_next_expr(method_name="to_uppercase", result_expr=res_expr, is_complex=True)

    def to_lowercase(self):
        res_expr = self.expr.to_lowercase() if self.expr is not None else None
        return self._create_next_expr(method_name="to_lowercase", result_expr=res_expr, is_complex=True)

    def len_chars(self):
        res_expr = self.expr.len_chars() if self.expr is not None else None
        return self._create_next_expr(method_name="len_chars", result_expr=res_expr, is_complex=True)

    def len_bytes(self):
        res_expr = self.expr.len_bytes() if self.expr is not None else None
        return self._create_next_expr(method_name="len_bytes", result_expr=res_expr, is_complex=True)

    def to_titlecase(self):
        res_expr = self.expr.to_titlecase() if self.expr is not None else None
        return self._create_next_expr(method_name="to_titlecase", result_expr=res_expr, is_complex=True)

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
    expr: Optional[Any]

    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.dt if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, method_name: str, result_expr: Optional[pl.Expr], *args, **kwargs) -> 'Expr':
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.dt.{method_name}({args_repr})"

        new_expr = Expr(result_expr, self.parent.name, repr_str=new_repr,
                        initial_column_name=self.parent._initial_column_name,
                        selector=None,
                        agg_func=self.parent.agg_func,
                        is_complex=True)
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
    is_complex: bool = False

    def __init__(self,
                 expr: Optional[pl.Expr],
                 column_name: Optional[str] = None,
                 repr_str: Optional[str] = None,
                 initial_column_name: Optional[str] = None,
                 selector: Optional['Selector'] = None,
                 agg_func: Optional[str] = None,
                 ddof: Optional[int] = None,
                 is_complex: bool = False):

        self.expr = expr
        self.name = column_name
        self.agg_func = agg_func
        self.selector = selector
        self._initial_column_name = initial_column_name or column_name
        self.is_complex = is_complex
        # --- Determine Representation String ---
        if repr_str is not None:
            self._repr_str = repr_str
        elif self.selector is not None and self.agg_func is not None:
            selector_repr = self.selector.repr_str
            func_name = self.agg_func
            kwargs_dict = {}
            if func_name in ("std", "var") and ddof is not None:
                kwargs_dict['ddof'] = ddof
            kwargs_repr = _repr_args(**kwargs_dict)
            self._repr_str = f"{selector_repr}.{func_name}({kwargs_repr})"
            self.expr = None
        elif self.selector is not None:
            self._repr_str = f"{self.selector.repr_str}"
            self.expr = None
        elif self.expr is not None:
            _, default_repr = _get_expr_and_repr(self.expr)
            self._repr_str = default_repr
        else:
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

    @property
    def is_simple(self) -> bool:
        """
        Determines if this expression is a "simple" expression that can be directly
        converted to a GroupBy's AggColl structure.

        A simple expression is one that:
        1. References a single column directly (not through arithmetic/logical operations)
        2. May have an aggregation function applied (sum, mean, etc.)
        3. May have been aliased with a new name

        Returns
        -------
        bool
            True if this is a simple expression, False otherwise
        """
        # Check for selector expressions
        if self.selector is not None:
            # Selector expressions are complex - they select multiple columns
            return False

        # Check if this expression has any arithmetic/logical operators
        if hasattr(self, "_repr_str"):
            # Check for when/then/otherwise expressions
            if any(
                marker in self._repr_str
                for marker in ["when(", ".then(", ".otherwise("]
            ):
                return False

            # Look for arithmetic operators in the expression string
            for op in ["+", "-", "*", "/", "//", "%", "**", "&", "|", "==", "!=", "<", ">", "<=", ">=",]:
                if op in self._repr_str:
                    # If the operator is in a .alias() part, it's still simple
                    if f".alias('{op}" in self._repr_str:
                        continue

                    # Otherwise, we have a complex expression
                    return False

            # Check for other functions that might create complex expressions
            for func in [
                "filter(",
                "where(",
                "if_else(",
                "case_when(",
                "apply(",
                "map(",
            ]:
                if func in self._repr_str:
                    return False

        # If we reach here, it's a simple expression (just column reference and maybe aggregation)
        return True

    def _create_next_expr(self, *args,  method_name: str, result_expr: Optional[pl.Expr], is_complex: bool, **kwargs) -> 'Expr':
        """Creates a new Expr instance, appending method call to repr string."""
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self._repr_str}.{method_name}({args_repr})"

        # Create new instance, inheriting current agg_func status by default
        new_expr_instance = Expr(result_expr, self.name, repr_str=new_repr,
                                 initial_column_name=self._initial_column_name,
                                 selector=None,
                                 agg_func=self.agg_func,
                                 is_complex=is_complex)
        return new_expr_instance

    def _create_binary_op_expr(
        self, op_symbol: str, other: Any, result_expr: Optional[pl.Expr]
    ) -> "Expr":
        """Creates a new Expr for binary operations."""
        if self.expr is None:
            raise ValueError(
                f"Cannot perform binary operation '{op_symbol}' on Expr without underlying polars expression."
            )

        other_expr, other_repr = _get_expr_and_repr(other)

        if other_expr is None and not isinstance(
            other, (int, float, str, bool, type(None))
        ):
            raise ValueError(
                f"Cannot perform binary operation '{op_symbol}' with operand without underlying polars expression or literal value: {other_repr}"
            )

        # For binary operations, just construct the expression without extra parentheses
        new_repr = f"{self._repr_str} {op_symbol} {other_repr}"

        # Binary ops clear the aggregation state and selector link
        return Expr(
            result_expr,
            None,
            repr_str=f"({new_repr})",  # Add parentheses around the ENTIRE expression
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=None,
            is_complex=True
        )

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

    def sum(self):
        result_expr = self.expr.sum() if self.expr is not None else None
        result = self._create_next_expr(method_name="sum", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "sum"
        return result

    def mean(self):
        result_expr = self.expr.mean() if self.expr is not None else None
        result = self._create_next_expr(method_name="mean", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "mean"
        return result

    def min(self):
        result_expr = self.expr.min() if self.expr is not None else None
        result = self._create_next_expr(method_name="min", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "min"
        return result

    def max(self):
        result_expr = self.expr.max() if self.expr is not None else None
        result = self._create_next_expr(method_name="max", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "max"
        return result

    def median(self):
        result_expr = self.expr.median() if self.expr is not None else None
        result = self._create_next_expr(method_name="median", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "median"
        return result

    def count(self):
        result_expr = self.expr.count() if self.expr is not None else None
        result = self._create_next_expr(method_name="count", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "count"
        return result

    def first(self):
        result_expr = self.expr.first() if self.expr is not None else None
        result = self._create_next_expr(method_name="first", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "first"
        return result

    def last(self):
        result_expr = self.expr.last() if self.expr is not None else None
        result = self._create_next_expr(method_name="last", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "last"
        return result

    def n_unique(self):
        result_expr = self.expr.n_unique() if self.expr is not None else None
        result = self._create_next_expr(method_name="n_unique", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "n_unique"
        return result

    def std(self, ddof=1):
        result_expr = self.expr.std(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr(method_name="std", result_expr=result_expr, ddof=ddof, is_complex=True)
        result.agg_func = "std"
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
        result_expr = (
            self.expr.cum_count(reverse=reverse) if self.expr is not None else None
        )
        result = self._create_next_expr(method_name="cum_count", result_expr=result_expr, reverse=reverse, is_complex=True)
        result.agg_func = None
        return result

    def var(self, ddof=1):
        result_expr = self.expr.var(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr(method_name="var", result_expr=result_expr, ddof=ddof, is_complex=True)
        result.agg_func = "var"
        return result

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
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rsub__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} - {self._repr_str}"
        res_expr = other_expr - self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rmul__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} * {self._repr_str}"
        res_expr = other_expr * self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rtruediv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} / {self._repr_str}"
        res_expr = other_expr / self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rfloordiv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} // {self._repr_str}"
        res_expr = other_expr // self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rmod__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} % {self._repr_str}"
        res_expr = other_expr % self.expr if other_expr is not None and self.expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

    def __rpow__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} ** {self._repr_str}"
        base_expr = pl.lit(other) if not isinstance(other, (Expr, pl.Expr)) else other_expr
        res_expr = base_expr.pow(self.expr) if self.expr is not None and base_expr is not None else None
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True)

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
        from flowfile_frame.selectors import Selector
        if isinstance(other, Selector):
            raise TypeError("Unsupported operation: Expr & Selector")
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr & other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("&", other, res_expr)

    def __or__(self, other):
        from flowfile_frame.selectors import Selector
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
        result_expr = self.expr.is_null() if self.expr is not None else None
        # is_null is not an aggregation, resets agg_func
        result = self._create_next_expr(method_name="is_null", result_expr=result_expr, is_complex=True)
        result.agg_func = None
        return result

    def filter(self, *predicates, **constraints) -> "Expr":
        """
        Filter expression
        """
        # Build arguments for the filter representation
        args_strs = []
        for pred in predicates:
            if isinstance(pred, Expr):
                args_strs.append(str(pred))
            elif isinstance(pred, pl.Expr):
                _, pred_repr = _get_expr_and_repr(pred)
                args_strs.append(pred_repr)
            else:
                args_strs.append(repr(pred))

        # Add constraints as keyword arguments
        constraints_strs = [f"{k}={repr(v)}" for k, v in constraints.items()]
        all_args_str = ", ".join(args_strs + constraints_strs)

        # Process the predicates for the polars expression
        processed_predicates = []
        for pred in predicates:
            if isinstance(pred, Expr):
                if pred.expr is not None:
                    processed_predicates.append(pred.expr)
            else:
                # Handle non-Expr predicates (convert to polars Expr if possible)
                processed_predicates.append(pred)

        # Process constraints for the polars expression
        for col_name, value in constraints.items():
            # Create equivalent of pl.col(col_name).eq(value)
            constraint_expr = pl.col(col_name).eq(value)
            processed_predicates.append(constraint_expr)

        # Create the actual polars expression if possible
        res_expr = None
        if self.expr is not None:
            try:
                res_expr = self.expr.filter(*processed_predicates)
            except Exception as e:
                print(f"Warning: Could not create polars expression for filter(): {e}")
                pass  # res_expr will remain None

        return Expr(
            res_expr,
            self.name,
            repr_str=f"{self._repr_str}.filter({all_args_str})",
            initial_column_name=self._initial_column_name,
            selector=None,  # Filter typically removes selector link
            agg_func=self.agg_func,  # Preserve aggregation status
        )

    def is_not_null(self):
        result_expr = self.expr.is_not_null() if self.expr is not None else None
        result = self._create_next_expr(method_name="is_not_null", result_expr=result_expr, is_complex=True)
        result.agg_func = None
        return result

    def is_in(self, values):
        res_expr = self.expr.is_in(values) if self.expr is not None else None
        # is_in is not an aggregation, resets agg_func
        result = self._create_next_expr(values, method_name="is_in", result_expr=res_expr, is_complex=True)
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
                            agg_func=self.agg_func,
                            is_complex=self.is_complex)
        return new_instance

    def fill_null(self, value):
        res_expr = self.expr.fill_null(value) if self.expr is not None else None
        # fill_null is not an aggregation, resets agg_func
        result = self._create_next_expr(value, method_name="fill_null", result_expr=res_expr, is_complex=True)
        result.agg_func = None
        return result

    def fill_nan(self, value):
        res_expr = None
        if self.expr is not None and hasattr(self.expr, 'fill_nan'):
            res_expr = self.expr.fill_nan(value)
        result = self._create_next_expr(value, method_name="fill_nan", result_expr=res_expr, is_complex=True)
        result.agg_func = None
        return result

    @staticmethod
    def _get_expr_repr(expr):
        """Helper to get appropriate string representation for an expression"""
        if isinstance(expr, (Expr, Column)):
            return expr._repr_str
        elif isinstance(expr, str):
            return f"pl.col('{expr}')"
        elif isinstance(expr, pl.Expr):
            base_str = str(expr)
            if base_str.startswith("col("):
                return f"pl.{base_str}"
            if base_str.startswith("lit("):
                return f"pl.{base_str}"
            return f"pl.Expr({base_str})"
        else:
            return repr(expr)

    def over(self,
             partition_by: ExprStrOrList,  # Use the type alias defined earlier
             *more_exprs: ExprOrStr,
             order_by: Optional[ExprStrOrList] = None,
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
                processed_partition_cols.append(col(col_expr))
            elif isinstance(col_expr, list):
                processed_list = []
                for item in col_expr:
                    if isinstance(item, str):
                        processed_list.append(col(item))
                    else:
                        processed_list.append(item)
                processed_partition_cols.extend(processed_list)
            else:
                processed_partition_cols.append(col_expr)

        processed_order_by = None
        if order_by is not None:
            if isinstance(order_by, str):
                processed_order_by = col(order_by)
            elif isinstance(order_by, list):
                processed_order_by = [
                    col(o) if isinstance(o, str) else o for o in order_by
                ]
            else:
                processed_order_by = order_by

        over_arg_strings_for_repr = []

        if built_in_len(processed_partition_cols) == 1:
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

        if descending:
            over_arg_strings_for_repr.append(f"descending={repr(descending)}")

        if nulls_last:
            over_arg_strings_for_repr.append(f"nulls_last={repr(nulls_last)}")

        if mapping_strategy != "group_to_rows":
            over_arg_strings_for_repr.append(f"mapping_strategy='{mapping_strategy}'")

        args_str_for_repr = ", ".join(over_arg_strings_for_repr)

        res_expr = None
        if self.expr is not None:
            try:
                if len(processed_partition_cols) == 1:
                    partition_arg = (
                        processed_partition_cols[0].expr
                        if hasattr(processed_partition_cols[0], "expr")
                        else processed_partition_cols[0]
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

                print(f"Warning: Could not create polars expression for over(): {e}")
                pass

        return Expr(
            res_expr,
            self.name,
            repr_str=f"{self._repr_str}.over({args_str_for_repr})",
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=None,
        )

    def sort(self, *, descending=False, nulls_last=False):
        res_expr = self.expr.sort(descending=descending, nulls_last=nulls_last) if self.expr is not None else None
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
                        agg_func=self.agg_func,
                        is_complex=True)
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
                         agg_func=None)
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
        if self.expr is None:
            raise ValueError("Cannot alias Column without underlying polars expression.")

        new_pl_expr = self.expr.alias(new_name)
        new_repr = f"{self._repr_str}.alias({repr(new_name)})"

        new_column = Column(new_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr

        new_column.agg_func = self.agg_func
        new_column.is_complex = self.is_complex
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
        if self.expr is None:
            raise ValueError("Cannot cast Column without underlying polars expression.")

        new_pl_expr = self.expr.cast(pl_dtype, strict=strict)
        new_repr = f"{self._repr_str}.cast({dtype_repr}, strict={strict})"
        display_name = self._select_input.new_name or self._select_input.old_name

        new_column = Column(display_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        new_column.agg_func = self.agg_func
        new_column.is_complex = True
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

    @property
    def str(self) -> StringMethods:
        return super().str

    @property
    def dt(self) -> DateTimeMethods:
        return super().dt


class When(Expr):
    """Class that represents a when-then-otherwise expression chain."""

    def __init__(self, condition):
        """Initialize a When expression with a condition."""
        # Get the condition's expression and representation
        condition_expr, condition_repr = self._get_expr_and_repr(condition)
        self.condition = condition_expr

        # Build the initial representation string
        repr_str = f"pl.when({condition_repr})"
        # Initialize the base class
        super().__init__(expr=None, repr_str=repr_str, is_complex=True)
        self._branch_expr = None

    @staticmethod
    def _get_expr_and_repr(value):
        """Extract expression and representation from a value."""
        if hasattr(value, 'expr') and hasattr(value, '_repr_str'):
            return value.expr, value._repr_str
        elif isinstance(value, str) and not value.startswith("pl."):
            col_obj = col(value)
            return col_obj.expr, f"'{value}'"
        else:
            return value, repr(value)

    def then(self, value):
        """Set the value to use when the condition is True."""
        value_expr, value_repr = self._get_expr_and_repr(value)

        self._repr_str = f"{self._repr_str}.then({value_repr})"
        try:
            self._branch_expr = pl.when(self.condition).then(value_expr)
        except Exception as e:
            print(f"Warning: Error in then() creation: {e}")

        return self

    def otherwise(self, value):
        """Set the value to use when no condition is True."""
        # Get the value's expression and representation
        value_expr, value_repr = self._get_expr_and_repr(value)
        final_repr = f"{self._repr_str}.otherwise({value_repr})"

        pl_expr = None
        try:
            if self._branch_expr is not None:
                pl_expr = self._branch_expr.otherwise(value_expr)
        except Exception as e:
            print(f"Warning: Could not create when-then-otherwise expression: {e}")

        return Expr(pl_expr, repr_str=final_repr)

    def when(self, condition):
        """Create a new branch in the chain."""
        if self._branch_expr is None:
            print("Warning: Cannot add new branch without a then() first")
            return self

        condition_expr, condition_repr = self._get_expr_and_repr(condition)

        self._repr_str = f"{self._repr_str}.when({condition_repr})"

        try:
            self._branch_expr = self._branch_expr.when(condition_expr)
        except Exception as e:
            print(f"Warning: Error adding new when() branch: {e}")

        # Return self for chaining
        return self


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


def len() -> Expr:
    return Expr(pl.len()).alias('number_of_records')


def agg_function(func):
    """
    Decorator for aggregation functions that sets appropriate properties based on number of arguments.
    Uses the function name as the aggregation function name.

    Parameters:
    -----------
    func : function
        The aggregation function to decorate

    Returns:
    --------
    wrapper
        A wrapped function that returns the properly configured Expr
    """
    agg_func_name = func.__name__  # Use the function name as the agg_func

    def wrapper(*names):
        # Get the Polars expression from the original function
        pl_expr = func(*names)
        if built_in_len(names) == 1 and isinstance(names[0], str):
            return Expr(pl_expr, agg_func=agg_func_name, initial_column_name=names[0], is_complex=False)
        elif built_in_len(names) == 1 and isinstance(names[0], Expr):
            return Expr(pl_expr, agg_func=agg_func_name, initial_column_name=names[0].name, is_complex=names[0].is_complex)
        else:
            return Expr(pl_expr, agg_func=agg_func_name, is_complex=True)
    return wrapper


@agg_function
def max(*names) -> Expr:
    return pl.max(*names)


@agg_function
def min(*names) -> Expr:
    return pl.min(*names)


@agg_function
def first(*names) -> Expr:
    return pl.first(*names)


@agg_function
def last(*names) -> Expr:
    return pl.last(*names)


@agg_function
def mean(*names) -> Expr:
    return pl.mean(*names)


@agg_function
def count(*names) -> Expr:
    return pl.count(*names)


@agg_function
def sum(*names) -> Expr:
    return pl.sum(*names)


def std(column, ddof) -> Expr:
    return Expr(column, ddof=ddof, agg_func='std')


def var(column, ddof) -> Expr:
    return Expr(column, ddof=ddof, agg_func="var")


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


def when(condition):
    """Start a when-then-otherwise expression."""
    return When(condition)
