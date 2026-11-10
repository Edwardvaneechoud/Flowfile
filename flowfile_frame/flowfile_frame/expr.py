from __future__ import annotations

import datetime
import decimal
import math
from builtins import len as built_in_len
from collections.abc import Callable, Iterator
from functools import wraps
from typing import TYPE_CHECKING, Any, Literal, TypeVar, Union

import polars as pl
from polars.expr.string import ExprStringNameSpace

from flowfile_core.flowfile.code_generator.literals import python_literal
from flowfile_core.schemas import transform_schema
from flowfile_frame.adding_expr import add_expr_methods, refuse_parameter_argument
from flowfile_frame.config import logger
from flowfile_frame.expr_name import ExprNameNameSpace
from flowfile_frame.list_name_space import ExprListNameSpace
from flowfile_frame.parameters import Parameter, refuse_parameter_as_column

# --- TYPE CHECKING IMPORTS ---
if TYPE_CHECKING:
    from flowfile_frame.selectors import Selector

    ExprType = TypeVar("ExprType", bound="Expr")
    ColumnType = "Column"  # Use string literal instead of direct class reference
    from polars._typing import FillNullStrategy, IntoExprColumn, PolarsTemporalType

ExprOrStr = Union["Expr", str]  # noqa: UP007
ExprOrStrList = list[ExprOrStr]
ExprStrOrList = Union[ExprOrStr, ExprOrStrList]  # noqa: UP007


def _argument_code(value: Any) -> str:
    """Code for one argument: an ``Expr`` by its repr, anything else as a literal that runs (``float("nan")``)."""
    return repr(value) if isinstance(value, Expr) else python_literal(value, set())


def _repr_args(*args, **kwargs):
    """The arguments of a method call as code."""
    arg_reprs = [_argument_code(a) for a in _frame_value(args)]
    kwarg_reprs = []
    for k, v in _frame_value(kwargs).items():
        if k == "_function_sources":
            continue
        if isinstance(v, pl.DataType):
            kwarg_reprs.append(f"{k}={v!s}")
        elif isinstance(v, type) and issubclass(v, pl.DataType):
            kwarg_reprs.append(f"{k}=pl.{v.__name__}")
        else:
            kwarg_reprs.append(f"{k}={_argument_code(v)}")
    return ", ".join(arg_reprs + kwarg_reprs)


FF_OPERATOR_MAP = {
    "+": "+", "-": "-", "*": "*", "/": "/", "%": "%",
    "==": "==", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">=",
    "&": "and", "|": "or",
}

# Formula functions that are exactly Polars' strict cast (to_boolean parses text and turns null into false).
CAST_FF_MAP: dict[pl.DataType, str] = {
    pl.Int64(): "to_integer",
    pl.Float64(): "to_float",
    pl.String(): "to_string",
}

STRING_METHOD_FF_MAP: dict[str, tuple[str, int]] = {
    "to_uppercase": ("uppercase", 0),
    "to_lowercase": ("lowercase", 0),
    "to_titlecase": ("titlecase", 0),
    "len_chars": ("length", 0),
    "starts_with": ("starts_with", 1),
    "ends_with": ("ends_with", 1),
    "strip_chars_start": ("left_trim", 0),
    "strip_chars_end": ("right_trim", 0),
}

DT_METHOD_FF_MAP: dict[str, str] = {
    "year": "year", "month": "month", "day": "day",
    "hour": "hour", "minute": "minute", "second": "second",
    "quarter": "quarter", "week": "week",
}


def _get_ff_repr(value: Any) -> str | None:
    """Get flowfile function representation of a value."""
    if isinstance(value, Expr):
        return value._ff_repr
    elif isinstance(value, Parameter):
        return value.ref
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, int | float):
        return str(value) if math.isfinite(value) else None  # the formula language has no nan / inf
    elif isinstance(value, str):
        # The formula parser has no quote escape: use the quote kind the value lacks.
        escaped = value.replace("\\", "\\\\")
        if '"' not in escaped:
            return f'"{escaped}"'
        if "'" not in escaped:
            return f"'{escaped}'"
        return None
    elif value is None:
        return "null"
    else:
        return None


def _membership_ff_repr(ff_repr: str | None, values: Any) -> str | None:
    """``[col] in ("a", "b")`` when ``values`` is a plain sequence of scalar literals that each have a formula form
    (a NaN or an infinity has none), else None."""
    if ff_repr is None or not isinstance(values, list | tuple | set | frozenset):
        return None
    if not all(isinstance(v, bool | int | float | str) for v in values):
        return None
    parts = [_get_ff_repr(v) for v in values]
    if any(part is None for part in parts):
        return None
    return f"{ff_repr} in ({', '.join(parts)})"


def _compute_cast_ff_repr(ff_repr: str | None, pl_dtype: pl.DataType | type, strict: bool = True) -> str | None:
    """Compute ff_repr for a cast operation. Shared by Expr.cast() and Column.cast().

    Other casts (Int8, Float32, Date, non-strict, ...) keep the Polars-code path: the formula would change the result.
    """
    if ff_repr is None or not strict:
        return None
    return next((f"{function}({ff_repr})" for dtype, function in CAST_FF_MAP.items() if pl_dtype == dtype), None)


_PYTHON_FUNCTION_MARKERS = ('"AnonymousFunction":', '"callback":', '"RenameAlias":{"function":{"Map":')
"""Where a serialized plan carries a pickled Python function: ``map_elements`` / ``map_batches`` / ``map_groups``,
``fold`` / ``reduce`` and their cumulative forms, and ``name.map``."""
_SCALAR_LITERAL_TYPES = (
    type(None),
    bool,
    int,
    float,
    str,
    decimal.Decimal,
    datetime.date,
    datetime.time,
    datetime.timedelta,
)


def _polars_expr_code(value: pl.Expr) -> str | None:
    """Source that rebuilds ``value``: ``pl.col(name)`` for a column, ``pl.lit(...)`` for a scalar literal, else its
    JSON plan (``str(value)`` is Polars' display form, not code). ``None`` when the plan carries a pickled Python
    function, which no code rebuilds, or one Polars cannot pickle at all (a closure over a lock)."""
    if value.meta.is_column():
        return f"pl.col({value.meta.output_name()!r})"
    literal = _literal_code(value) if value.meta.is_literal() else None
    if literal is not None:
        return literal
    try:
        plan = value.meta.serialize(format="json")
    except Exception:
        return None
    if any(marker in plan for marker in _PYTHON_FUNCTION_MARKERS):
        return None
    return f"pl.Expr.deserialize(BytesIO({plan.encode()!r}), format='json')"


def _literal_code(value: pl.Expr) -> str | None:
    """``pl.lit(<scalar>[, dtype=pl.<dtype>])`` for a literal holding one scalar the sandbox can spell, else None.

    The dtype is written whenever ``pl.lit(<scalar>)`` alone is another expression: a literal the caller typed
    (``pl.lit(100, dtype=pl.Int32)``) keeps its type, while an untyped one stays dynamic and, as in Polars, takes the
    type of the column it meets.
    """
    try:
        frame = pl.select(value)
    except Exception:
        return None
    if frame.shape != (1, 1):
        return None
    scalar, dtype = frame.item(), frame.dtypes[0]
    if not isinstance(scalar, _SCALAR_LITERAL_TYPES) or getattr(scalar, "tzinfo", None) is not None:
        return None
    literal = python_literal(scalar, set())
    if pl.lit(scalar).meta.eq(value):
        return f"pl.lit({literal})"
    return f"pl.lit({literal}, dtype=pl.{dtype!r})"


def _get_expr_and_repr(value: Any) -> tuple[pl.Expr | None, str]:
    """Helper to get polars expr and repr string for operands."""
    if isinstance(value, Parameter):
        value = value.to_expr()
    value = _frame_value(value)
    if isinstance(value, Expr):
        return value.expr, value._repr_str
    # Assume literal
    return pl.lit(value), python_literal(value, set())


def _wrapped_operand(method: Callable[[Expr, Any], Expr]) -> Callable[[Expr, Any], Expr]:
    """``method`` given its operand with every Polars expression in it wrapped once, so each is serialised once."""

    @wraps(method)
    def wrapper(self: Expr, other: Any) -> Expr:
        return method(self, _frame_value(other))

    return wrapper


def _map_nested(value: Any, leaf: Callable[[Any], Any]) -> Any:
    """``value`` with ``leaf`` applied to every element of its lists, tuples and dicts, at any depth."""
    if isinstance(value, list | tuple):
        return type(value)(_map_nested(item, leaf) for item in value)
    if isinstance(value, dict):
        return {key: _map_nested(item, leaf) for key, item in value.items()}
    return leaf(value)


def _frame_value(value: Any) -> Any:
    """``value`` with every Polars expression in it, at any depth, wrapped as an ``Expr``, whose repr is code."""
    return _map_nested(value, lambda item: Expr(item) if isinstance(item, pl.Expr) else item)


def _polars_argument(value: Any) -> Any:
    """``value`` with every frame expression in it, at any depth, replaced by its Polars expression.

    Raises ``TypeError`` for a selector or an expression without a Polars expression.
    """
    from flowfile_frame.selectors import Selector

    def polars(item: Any) -> Any:
        if isinstance(item, Expr) and item.expr is not None:
            return item.expr
        if isinstance(item, Expr | Selector):
            raise TypeError("an argument has no Polars expression")
        return item

    return _map_nested(value, polars)


def _frame_exprs(value: Any) -> Iterator[Expr]:
    """Every frame expression in ``value``, at any depth (a bare Polars expression wrapped)."""
    if isinstance(value, pl.Expr):
        yield Expr(value)
    elif isinstance(value, Expr):
        yield value
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _frame_exprs(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _frame_exprs(item)


def _merge_operand_metadata(left: Any, *operands: Any) -> tuple[list[str], bool]:
    """Lambda sources and code-convertability of ``left`` and the frame expressions in ``operands``."""
    sources = list(left._function_sources)
    convertable = left.convertable_to_code
    for operand in _frame_exprs(operands):
        sources.extend(source for source in operand._function_sources if source not in sources)
        convertable = convertable and operand.convertable_to_code
    return sources, convertable


def _namespace_expr(
    parent: Expr,
    repr_str: str,
    operands: tuple,
    result_expr: pl.Expr | None,
    *,
    is_complex: bool = True,
    ff_repr: str | None = None,
) -> Expr:
    """The ``Expr`` a ``.str`` / ``.dt`` / ``.list`` method returns: ``parent``'s identity and aggregation state, and
    the function sources and code-convertibility of ``parent`` and of the frame expressions in ``operands``."""
    sources, convertable = _merge_operand_metadata(parent, *operands)
    return Expr(
        result_expr,
        parent.column_name,
        repr_str=repr_str,
        initial_column_name=parent._initial_column_name,
        selector=None,
        agg_func=parent.agg_func,
        is_complex=is_complex,
        convertable_to_code=convertable,
        _function_sources=sources,
        ff_repr=ff_repr,
    )


class StringMethods:
    expr: ExprStringNameSpace | None

    def __init__(self, parent_expr: Expr, parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.str if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    @property
    def convertable_to_code(self) -> bool:
        return self.parent.convertable_to_code

    def _create_next_expr(
        self, *args, method_name: str, result_expr: pl.Expr | None, is_complex: bool, **kwargs
    ) -> Expr:
        args, kwargs = _frame_value(args), _frame_value(kwargs)
        new_repr = f"{self.parent_repr_str}.str.{method_name}({_repr_args(*args, **kwargs)})"

        ff = None
        parent_ff = self.parent._ff_repr
        if parent_ff is not None and method_name in STRING_METHOD_FF_MAP:
            func_name, num_extra_args = STRING_METHOD_FF_MAP[method_name]
            if num_extra_args == 0:
                ff = f"{func_name}({parent_ff})"
            elif num_extra_args >= 1 and built_in_len(args) >= 1:
                extra_ff_args = [_get_ff_repr(a) for a in args[:num_extra_args]]
                if all(a is not None for a in extra_ff_args):
                    all_args = [parent_ff] + extra_ff_args
                    ff = f"{func_name}({', '.join(all_args)})"

        return _namespace_expr(
            self.parent, new_repr, (*args, *kwargs.values()), result_expr, is_complex=is_complex, ff_repr=ff
        )

    def contains(self, pattern, *, literal=False):
        res_expr = self.expr.contains(_polars_argument(pattern), literal=literal) if self.expr is not None else None
        return self._create_next_expr(
            pattern, literal=literal, method_name="contains", result_expr=res_expr, is_complex=True
        )

    def starts_with(self, prefix):
        res_expr = self.expr.starts_with(_polars_argument(prefix)) if self.expr is not None else None
        return self._create_next_expr(prefix, is_complex=True, method_name="starts_with", result_expr=res_expr)

    def ends_with(self, suffix):
        res_expr = self.expr.ends_with(_polars_argument(suffix)) if self.expr is not None else None
        return self._create_next_expr(suffix, result_expr=res_expr, method_name="ends_with", is_complex=True)

    def replace(self, pattern, replacement, *, literal=False):
        res_expr = (
            self.expr.replace(*_polars_argument((pattern, replacement)), literal=literal)
            if self.expr is not None
            else None
        )
        return self._create_next_expr(
            pattern, replacement, method_name="replace", result_expr=res_expr, literal=literal, is_complex=True
        )

    def to_uppercase(self):
        res_expr = self.expr.to_uppercase() if self.expr is not None else None
        return self._create_next_expr(method_name="to_uppercase", result_expr=res_expr, is_complex=True)

    def slice(self, offset: int | IntoExprColumn, length: int | IntoExprColumn | None = None) -> Expr:
        res_expr = self.expr.slice(offset=_polars_argument(offset), length=_polars_argument(length))
        return self._create_next_expr(
            method_name="slice", result_expr=res_expr, is_complex=True, offset=offset, length=length
        )

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

    def strip_chars_start(self, characters: str | None = None):
        res_expr = self.expr.strip_chars_start(_polars_argument(characters)) if self.expr is not None else None
        args = () if characters is None else (characters,)
        result = self._create_next_expr(*args, method_name="strip_chars_start", result_expr=res_expr, is_complex=True)
        if characters is not None:
            result._ff_repr = None  # left_trim formula only supports generic whitespace trim
        return result

    def strip_chars_end(self, characters: str | None = None):
        res_expr = self.expr.strip_chars_end(_polars_argument(characters)) if self.expr is not None else None
        args = () if characters is None else (characters,)
        result = self._create_next_expr(*args, method_name="strip_chars_end", result_expr=res_expr, is_complex=True)
        if characters is not None:
            result._ff_repr = None  # right_trim formula only supports generic whitespace trim
        return result

    def to_date(self, format: str, *, strict: bool = True, exact: bool = True, cache: bool = True):
        res_expr = self.expr.to_date(format, strict=strict, exact=exact, cache=cache) if self.expr is not None else None
        return self._create_next_expr(
            method_name="to_date",
            result_expr=res_expr,
            is_complex=True,
            format=format,
            strict=strict,
            exact=exact,
            cache=cache,
        )

    def to_datetime(
        self,
        format: str | None = None,
        *,
        time_unit: Literal["ns", "us", "ms"] | None = None,
        time_zone: str | None = None,
        strict: bool = True,
        exact: bool = True,
        cache: bool = True,
        ambiguous: Literal["earliest", "latest", "raise", "null"] | Expr = "raise",
    ) -> Expr:
        res_expr = self.expr.to_datetime(
            format,
            time_unit=time_unit,
            time_zone=time_zone,
            strict=strict,
            exact=exact,
            cache=cache,
            ambiguous=_polars_argument(ambiguous),
        )
        return self._create_next_expr(
            method_name="to_datetime",
            result_expr=res_expr,
            is_complex=True,
            format=format,
            time_unit=time_unit,
            time_zone=time_zone,
            strict=strict,
            exact=exact,
            cache=cache,
            ambiguous=ambiguous,
        )

    def strptime(
        self,
        dtype: PolarsTemporalType,
        format: str | None = None,
        *,
        strict: bool = True,
        exact: bool = True,
        cache: bool = True,
        ambiguous: Literal["earliest", "latest", "raise", "null"] | Expr = "raise",
    ) -> Expr:
        res_expr = self.expr.strptime(
            dtype, format, strict=strict, exact=exact, cache=cache, ambiguous=_polars_argument(ambiguous)
        )
        return self._create_next_expr(
            method_name="strptime",
            dtype=dtype,
            result_expr=res_expr,
            is_complex=True,
            format=format,
            strict=strict,
            exact=exact,
            cache=cache,
            ambiguous=ambiguous,
        )

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
                result = pl_attr(*_polars_argument(args), **_polars_argument(kwargs))
                # Assume generic getattr methods don't change aggregation status
                return self._create_next_expr(
                    *args, method_name=name, result_expr=result, is_complex=True, **kwargs
                )

            return wrapper
        else:
            return pl_attr


class DateTimeMethods:
    expr: Any | None

    def __init__(self, parent_expr: Expr, parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.dt if parent_expr.expr is not None else None
        self.parent_repr_str = parent_repr_str

    @property
    def convertable_to_code(self) -> bool:
        return self.parent.convertable_to_code

    def _create_next_expr(self, *args, method_name: str, result_expr: pl.Expr | None, **kwargs) -> Expr:
        args, kwargs = _frame_value(args), _frame_value(kwargs)
        new_repr = f"{self.parent_repr_str}.dt.{method_name}({_repr_args(*args, **kwargs)})"

        ff = None
        parent_ff = self.parent._ff_repr
        if parent_ff is not None and method_name in DT_METHOD_FF_MAP:
            ff = f"{DT_METHOD_FF_MAP[method_name]}({parent_ff})"

        return _namespace_expr(self.parent, new_repr, (*args, *kwargs.values()), result_expr, ff_repr=ff)

    def year(self):
        res_expr = self.expr.year() if self.expr is not None else None
        return self._create_next_expr(method_name="year", result_expr=res_expr)

    def month(self):
        res_expr = self.expr.month() if self.expr is not None else None
        return self._create_next_expr(method_name="month", result_expr=res_expr)

    def day(self):
        res_expr = self.expr.day() if self.expr is not None else None
        return self._create_next_expr(method_name="day", result_expr=res_expr)

    def hour(self):
        res_expr = self.expr.hour() if self.expr is not None else None
        return self._create_next_expr(method_name="hour", result_expr=res_expr)

    def minute(self):
        res_expr = self.expr.minute() if self.expr is not None else None
        return self._create_next_expr(method_name="minute", result_expr=res_expr)

    def second(self):
        res_expr = self.expr.second() if self.expr is not None else None
        return self._create_next_expr(method_name="second", result_expr=res_expr)

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
                result = pl_attr(*_polars_argument(args), **_polars_argument(kwargs))
                # Assume generic getattr methods don't change aggregation status
                return self._create_next_expr(*args, method_name=name, result_expr=result, **kwargs)

            return wrapper
        else:
            return pl_attr


class Expr:
    _initial_column_name: str | None
    selector: Selector | None
    expr: pl.Expr | None
    agg_func: str | None
    _repr_str: str
    _name_namespace: ExprNameNameSpace | None
    column_name: str | None
    is_complex: bool = False
    convertable_to_code: bool
    _function_sources: list[str]  # Add this attribute
    _window_spec: dict[str, Any] | None

    def __init__(
        self,
        expr: pl.Expr | None,
        column_name: str | None = None,
        repr_str: str | None = None,
        initial_column_name: str | None = None,
        selector: Selector | None = None,
        agg_func: str | None = None,
        ddof: int | None = None,
        is_complex: bool = False,
        convertable_to_code: bool = True,
        _function_sources: list[str] | None = None,
        ff_repr: str | None = None,
    ):
        self.expr = expr
        self.column_name = column_name
        self.agg_func = agg_func
        self.selector = selector
        self._initial_column_name = initial_column_name or column_name
        self.is_complex = is_complex
        self.convertable_to_code = convertable_to_code
        self._function_sources = _function_sources or []
        self._ff_repr = ff_repr
        self._window_spec = None
        # --- Determine Representation String ---
        if repr_str is not None:
            self._repr_str = repr_str
        elif self.selector is not None and self.agg_func is not None:
            selector_repr = self.selector.repr_str
            func_name = self.agg_func
            kwargs_dict = {}
            if func_name in ("std", "var") and ddof is not None:
                kwargs_dict["ddof"] = ddof
            kwargs_repr = _repr_args(**kwargs_dict)
            self._repr_str = f"{selector_repr}.{func_name}({kwargs_repr})"
            self.expr = None
        elif self.selector is not None:
            self._repr_str = f"{self.selector.repr_str}"
            self.expr = None
        elif isinstance(self.expr, pl.Expr):
            code = _polars_expr_code(self.expr)
            self._repr_str = str(self.expr) if code is None else code
            self.convertable_to_code = convertable_to_code and code is not None
        elif self.expr is not None:
            _, default_repr = _get_expr_and_repr(self.expr)
            self._repr_str = default_repr
        else:
            raise ValueError("Cannot initialize Expr without expr, repr_str, or selector+agg_func")

        if self.column_name is None and self.selector is None and self.expr is not None:
            try:
                self.column_name = self.expr._output_name
            except AttributeError:
                try:
                    self.column_name = self.expr._name
                except AttributeError:
                    try:
                        self.column_name = self.expr.meta.output_name()
                    except Exception:
                        pass
        self._list_namespace: ExprListNameSpace | None = None
        self._str_namespace: StringMethods | None = None
        self._dt_namespace: DateTimeMethods | None = None
        self._name_namespace: ExprNameNameSpace | None = None

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
        if self.selector is not None:
            # Selector expressions are complex - they select multiple columns
            return False

        if hasattr(self, "_repr_str"):
            if any(marker in self._repr_str for marker in ["when(", ".then(", ".otherwise("]):
                return False

            for op in [
                "+",
                "-",
                "*",
                "/",
                "//",
                "%",
                "**",
                "&",
                "|",
                "==",
                "!=",
                "<",
                ">",
                "<=",
                ">=",
            ]:
                if op in self._repr_str:
                    # If the operator is in a .alias() part, it's still simple
                    if f".alias('{op}" in self._repr_str:
                        continue

                    return False

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

        return True

    def arg_unique(self) -> Expr:
        result_expr = self.expr.arg_unique() if self.expr is not None else None
        return self._create_next_expr(method_name="arg_unique", result_expr=result_expr, is_complex=True)

    def arg_sort(self, *, descending: bool = False, nulls_last: bool = False) -> Expr:
        result_expr = (
            self.expr.arg_sort(descending=descending, nulls_last=nulls_last) if self.expr is not None else None
        )
        return self._create_next_expr(
            descending=descending,
            nulls_last=nulls_last,
            method_name="arg_sort",
            result_expr=result_expr,
            is_complex=True,
        )

    def _create_next_expr(
        self,
        *args,
        method_name: str,
        result_expr: pl.Expr | None,
        convertable_to_code: bool = None,
        is_complex: bool,
        _function_sources: list[str] | None = None,
        _repr_override: str | None = None,
        ff_repr: str | None = None,
        **kwargs,
    ) -> Expr:
        """Creates a new Expr instance, appending method call to repr string."""
        args, kwargs = _frame_value(args), _frame_value(kwargs)
        if _repr_override is not None:
            new_repr = _repr_override
        else:
            new_repr = f"{self._repr_str}.{method_name}({_repr_args(*args, **kwargs)})"

        if convertable_to_code is None:
            convertable_to_code = self.convertable_to_code
        sources, operands_convertable = _merge_operand_metadata(self, *args, *kwargs.values())
        sources.extend(source for source in _function_sources or () if source not in sources)

        # Create new instance, inheriting current agg_func status by default
        return Expr(
            result_expr,
            self.column_name,
            repr_str=new_repr,
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=self.agg_func,
            is_complex=is_complex,
            convertable_to_code=convertable_to_code and operands_convertable,
            _function_sources=sources,
            ff_repr=ff_repr,
        )

    @property
    def name(self) -> ExprNameNameSpace:
        """Access the name namespace for expression name operations."""
        if self._name_namespace is None:
            self._name_namespace = ExprNameNameSpace(self, self._repr_str)
        return self._name_namespace

    def _create_binary_op_expr(self, op_symbol: str, other: Any,
                               result_expr: pl.Expr | None, ff_repr: str | None = ...) -> Expr:
        """Creates a new Expr for binary operations."""
        if self.expr is None:
            raise ValueError(
                f"Cannot perform binary operation '{op_symbol}' on Expr without underlying polars expression."
            )

        other = _frame_value(other)
        other_expr, other_repr = _get_expr_and_repr(other)

        if other_expr is None and not isinstance(other, int | float | str | bool | type(None)):
            raise ValueError(
                f"Cannot perform binary operation '{op_symbol}' with operand "
                f"without underlying polars expression or literal value: {other_repr}"
            )

        # For binary operations, just construct the expression without extra parentheses
        new_repr = f"{self._repr_str} {op_symbol} {other_repr}"

        if ff_repr is not ...:
            new_ff = ff_repr
        else:
            ff_op = FF_OPERATOR_MAP.get(op_symbol)
            other_ff = _get_ff_repr(other)
            if self._ff_repr is not None and other_ff is not None and ff_op is not None:
                new_ff = f"({self._ff_repr} {ff_op} {other_ff})"
            else:
                new_ff = None

        output_column_name = None
        if result_expr is not None:
            try:
                output_column_name = result_expr.meta.output_name()
            except Exception:
                pass

        function_sources, convertable_to_code = _merge_operand_metadata(self, other)

        # Binary ops clear the aggregation state and selector link
        return Expr(
            result_expr,
            output_column_name,
            repr_str=f"({new_repr})",
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=None,
            is_complex=True,
            convertable_to_code=convertable_to_code,
            _function_sources=function_sources,
            ff_repr=new_ff,
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

    @property
    def list(self) -> ExprListNameSpace:
        if self._list_namespace is None:
            self._list_namespace = ExprListNameSpace(self, self._repr_str)
        return self._list_namespace

    def sum(self):
        result_expr = self.expr.sum() if self.expr is not None else None
        result = self._create_next_expr(method_name="sum", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "sum"
        return result

    def unique_counts(self):
        """
        Return the number of unique values in the column.

        Returns
        -------
        Expr
            A new expression with the unique counts
        """
        result_expr = self.expr.unique_counts() if self.expr is not None else None
        result = self._create_next_expr(
            method_name="unique_counts", result_expr=result_expr, is_complex=self.is_complex
        )
        result.agg_func = "unique_counts"
        return result

    def implode(self):
        result_expr = self.expr.implode() if self.expr is not None else None
        result = self._create_next_expr(method_name="implode", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "implode"
        return result

    def explode(self):
        result_expr = self.expr.explode() if self.expr is not None else None
        result = self._create_next_expr(method_name="explode", result_expr=result_expr, is_complex=self.is_complex)
        result.agg_func = "explode"
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
        """Standard deviation; at the default ``ddof=1`` (the Group By node's) it aggregates natively."""
        result_expr = self.expr.std(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr(
            method_name="std", result_expr=result_expr, ddof=ddof, is_complex=self.is_complex or ddof != 1
        )
        result.agg_func = "std"
        return result

    def cum_count(self, reverse: bool = False) -> Expr:
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
        result_expr = self.expr.cum_count(reverse=reverse) if self.expr is not None else None
        result = self._create_next_expr(
            method_name="cum_count", result_expr=result_expr, reverse=reverse, is_complex=True
        )
        result.agg_func = None
        return result

    def var(self, ddof=1):
        """Variance; at the default ``ddof=1`` (the Group By node's) it aggregates natively."""
        result_expr = self.expr.var(ddof=ddof) if self.expr is not None else None
        result = self._create_next_expr(
            method_name="var", result_expr=result_expr, ddof=ddof, is_complex=self.is_complex or ddof != 1
        )
        result.agg_func = "var"
        return result

    @_wrapped_operand
    def __add__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr + other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("+", other, res_expr)

    @_wrapped_operand
    def __sub__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr - other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("-", other, res_expr)

    @_wrapped_operand
    def __mul__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr * other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("*", other, res_expr)

    @_wrapped_operand
    def __truediv__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr / other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("/", other, res_expr)

    @_wrapped_operand
    def __floordiv__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr // other_expr if self.expr is not None and other_expr is not None else None
        other_ff = _get_ff_repr(other)
        ff = f"floor(({self._ff_repr} / {other_ff}))" if self._ff_repr is not None and other_ff is not None else None
        return self._create_binary_op_expr("//", other, res_expr, ff_repr=ff)

    @_wrapped_operand
    def __pow__(self, exponent) -> Expr:
        exp_expr, _ = _get_expr_and_repr(exponent)
        res_expr = self.expr.pow(exp_expr) if self.expr is not None and exp_expr is not None else None
        exp_ff = _get_ff_repr(exponent)
        ff = f"power({self._ff_repr}, {exp_ff})" if self._ff_repr is not None and exp_ff is not None else None
        return self._create_binary_op_expr("**", exponent, res_expr, ff_repr=ff)

    @_wrapped_operand
    def __mod__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr % other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("%", other, res_expr)

    # --- Right-side Arithmetic ---
    def _create_reverse_binary_ff_repr(self, op_symbol: str, other: Any) -> str | None:
        """Compute ff_repr for reverse binary operations (e.g., 5 + col('x'))."""
        ff_op = FF_OPERATOR_MAP.get(op_symbol)
        other_ff = _get_ff_repr(other)
        if other_ff is not None and self._ff_repr is not None and ff_op is not None:
            return f"({other_ff} {ff_op} {self._ff_repr})"
        return None

    @_wrapped_operand
    def __radd__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} + {self._repr_str})"
        res_expr = other_expr + self.expr if other_expr is not None and self.expr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources,
                    ff_repr=self._create_reverse_binary_ff_repr("+", other))

    @_wrapped_operand
    def __rsub__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} - {self._repr_str})"
        res_expr = other_expr - self.expr if other_expr is not None and self.expr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources,
                    ff_repr=self._create_reverse_binary_ff_repr("-", other))

    @_wrapped_operand
    def __rmul__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} * {self._repr_str})"
        res_expr = other_expr * self.expr if other_expr is not None and self.expr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources,
                    ff_repr=self._create_reverse_binary_ff_repr("*", other))

    @_wrapped_operand
    def __rtruediv__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} / {self._repr_str})"
        res_expr = other_expr / self.expr if other_expr is not None and self.expr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources,
                    ff_repr=self._create_reverse_binary_ff_repr("/", other))

    @_wrapped_operand
    def __rfloordiv__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} // {self._repr_str})"
        res_expr = other_expr // self.expr if other_expr is not None and self.expr is not None else None
        other_ff = _get_ff_repr(other)
        ff = f"floor(({other_ff} / {self._ff_repr}))" if other_ff is not None and self._ff_repr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources, ff_repr=ff)

    @_wrapped_operand
    def __rmod__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} % {self._repr_str})"
        res_expr = other_expr % self.expr if other_expr is not None and self.expr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources,
                    ff_repr=self._create_reverse_binary_ff_repr("%", other))

    @_wrapped_operand
    def __rpow__(self, other) -> Expr:
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"({other_repr} ** {self._repr_str})"
        base_expr = pl.lit(other) if not isinstance(other, Expr | pl.Expr) else other_expr
        res_expr = base_expr.pow(self.expr) if self.expr is not None and base_expr is not None else None
        other_ff = _get_ff_repr(other)
        ff = f"power({other_ff}, {self._ff_repr})" if other_ff is not None and self._ff_repr is not None else None
        function_sources, convertable_to_code = _merge_operand_metadata(self, other)
        return Expr(res_expr, None, repr_str=new_repr, agg_func=None, is_complex=True,
                    convertable_to_code=convertable_to_code, _function_sources=function_sources, ff_repr=ff)

    # --- Comparison operations ---
    @_wrapped_operand
    def __eq__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr == other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("==", other, res_expr)

    @_wrapped_operand
    def __ne__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr != other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("!=", other, res_expr)

    @_wrapped_operand
    def __gt__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr > other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr(">", other, res_expr)

    @_wrapped_operand
    def __lt__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr < other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("<", other, res_expr)

    @_wrapped_operand
    def __ge__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr >= other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr(">=", other, res_expr)

    @_wrapped_operand
    def __le__(self, other) -> Expr:
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr <= other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("<=", other, res_expr)

    # --- Logical operations ---
    def __bool__(self) -> bool:
        """Refuse truthiness, as Polars does: ``and``/``or``/``not`` would silently drop a condition."""
        raise TypeError(
            "the truth value of an Expr is ambiguous\n\n"
            "Use `&` instead of `and`, `|` instead of `or` and `~` instead of `not`, "
            "e.g. `(ff.col('a') > 0) & (ff.col('b') > 0)`."
        )

    @_wrapped_operand
    def __and__(self, other) -> Expr:
        from flowfile_frame.selectors import Selector

        if isinstance(other, Selector):
            raise TypeError("Unsupported operation: Expr & Selector")
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr & other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("&", other, res_expr)

    @_wrapped_operand
    def __or__(self, other) -> Expr:
        from flowfile_frame.selectors import Selector

        if isinstance(other, Selector):
            raise TypeError("Unsupported operation: Expr | Selector")
        other_expr, _ = _get_expr_and_repr(other)
        res_expr = self.expr | other_expr if self.expr is not None and other_expr is not None else None
        return self._create_binary_op_expr("|", other, res_expr)

    def __invert__(self) -> Expr:
        new_repr = f"~({self._repr_str})"
        res_expr = ~self.expr if self.expr is not None else None
        ff = f"not({self._ff_repr})" if self._ff_repr is not None else None
        # Invert clears agg_func
        return Expr(
            res_expr,
            None,
            repr_str=new_repr,
            initial_column_name=self._initial_column_name,
            agg_func=None,
            convertable_to_code=self.convertable_to_code,
            _function_sources=self._function_sources,
            ff_repr=ff,
        )

    def __neg__(self) -> Expr:
        new_repr = f"-{self._repr_str}"
        res_expr = -self.expr if self.expr is not None else None
        return Expr(
            res_expr,
            None,
            repr_str=new_repr,
            initial_column_name=self._initial_column_name,
            agg_func=None,
            convertable_to_code=self.convertable_to_code,
            _function_sources=self._function_sources,
        )

    def is_null(self):
        result_expr = self.expr.is_null() if self.expr is not None else None
        ff = f"is_empty({self._ff_repr})" if self._ff_repr is not None else None
        result = self._create_next_expr(method_name="is_null", result_expr=result_expr, is_complex=True, ff_repr=ff)
        result.agg_func = None
        return result

    def filter(self, *predicates, **constraints) -> Expr:
        """
        Filter expression
        """
        args_strs = []
        for pred in predicates:
            if isinstance(pred, Expr):
                args_strs.append(str(pred))
            elif isinstance(pred, pl.Expr):
                _, pred_repr = _get_expr_and_repr(pred)
                args_strs.append(pred_repr)
            else:
                args_strs.append(python_literal(pred, set()))

        constraints_strs = [f"{k}={_argument_code(v)}" for k, v in constraints.items()]
        all_args_str = ", ".join(args_strs + constraints_strs)

        processed_predicates = []
        for pred in predicates:
            if isinstance(pred, Expr):
                if pred.expr is not None:
                    processed_predicates.append(pred.expr)
            else:
                processed_predicates.append(pred)

        for col_name, value in constraints.items():
            constraint_expr = pl.col(col_name).eq(_polars_argument(value))
            processed_predicates.append(constraint_expr)

        res_expr = None
        if self.expr is not None:
            try:
                res_expr = self.expr.filter(*processed_predicates)
            except Exception:
                logger.warning("Could not create polars expression for filter(): {e}")
                pass  # res_expr will remain None

        sources, convertable = _merge_operand_metadata(self, *predicates, *constraints.values())
        return Expr(
            res_expr,
            self.column_name,
            repr_str=f"{self._repr_str}.filter({all_args_str})",
            initial_column_name=self._initial_column_name,
            selector=None,  # Filter typically removes selector link
            agg_func=self.agg_func,  # Preserve aggregation status
            convertable_to_code=convertable,
            _function_sources=sources,
        )

    def is_not_null(self):
        result_expr = self.expr.is_not_null() if self.expr is not None else None
        ff = f"is_not_empty({self._ff_repr})" if self._ff_repr is not None else None
        result = self._create_next_expr(method_name="is_not_null", result_expr=result_expr, is_complex=True, ff_repr=ff)
        result.agg_func = None
        return result

    def is_in(self, values):
        refuse_parameter_argument("is_in", (values,), {}, hint="compare with == instead: (ff.col(x) == parameter)")
        res_expr = self.expr.is_in(_polars_argument(values)) if self.expr is not None else None
        # is_in is not an aggregation, resets agg_func
        result = self._create_next_expr(
            values,
            method_name="is_in",
            result_expr=res_expr,
            is_complex=True,
            ff_repr=_membership_ff_repr(self._ff_repr, values),
        )
        result.agg_func = None
        return result

    def alias(self, name):
        """Rename the expression result."""
        refuse_parameter_as_column(name, "alias")
        new_pl_expr = self.expr.alias(name) if self.expr is not None else None
        new_repr = f"{self._repr_str}.alias({repr(name)})"
        # Alias preserves aggregation status and ff_repr
        new_instance = Expr(
            new_pl_expr,
            name,
            repr_str=new_repr,
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=self.agg_func,
            is_complex=self.is_complex,
            convertable_to_code=self.convertable_to_code,
            _function_sources=self._function_sources,
            ff_repr=self._ff_repr,
        )
        new_instance._window_spec = self._window_spec
        return new_instance

    def fill_null(
        self,
        value: Any | Expr | pl.Expr | None = None,
        strategy: FillNullStrategy | None = None,
        limit: int | None = None,
    ) -> Expr:
        refuse_parameter_argument("fill_null", (value,), {})
        options = {name: option for name, option in (("strategy", strategy), ("limit", limit)) if option is not None}
        res_expr = self.expr.fill_null(_polars_argument(value), **options) if self.expr is not None else None
        val_ff = _get_ff_repr(value) if value is not None and strategy is None else None
        ff = f"coalesce({self._ff_repr}, {val_ff})" if self._ff_repr is not None and val_ff is not None else None
        args = () if value is None else (value,)
        result = self._create_next_expr(*args, method_name="fill_null", result_expr=res_expr, is_complex=True,
                                        ff_repr=ff, **options)
        result.agg_func = None
        return result

    def fill_nan(self, value: int | float | Expr | pl.Expr | None) -> Expr:
        refuse_parameter_argument("fill_nan", (value,), {})
        res_expr = None
        if self.expr is not None and hasattr(self.expr, "fill_nan"):
            res_expr = self.expr.fill_nan(_polars_argument(value))
        result = self._create_next_expr(value, method_name="fill_nan", result_expr=res_expr, is_complex=True)
        result.agg_func = None
        return result

    def abs(self):
        res_expr = self.expr.abs() if self.expr is not None else None
        ff = f"abs({self._ff_repr})" if self._ff_repr is not None else None
        return self._create_next_expr(method_name="abs", result_expr=res_expr, is_complex=True, ff_repr=ff)

    def round(self, decimals: int = 0):
        res_expr = self.expr.round(decimals) if self.expr is not None else None
        ff = f"round({self._ff_repr}, {decimals})" if self._ff_repr is not None else None
        return self._create_next_expr(decimals, method_name="round", result_expr=res_expr, is_complex=True, ff_repr=ff)

    def ceil(self):
        res_expr = self.expr.ceil() if self.expr is not None else None
        ff = f"ceil({self._ff_repr})" if self._ff_repr is not None else None
        return self._create_next_expr(method_name="ceil", result_expr=res_expr, is_complex=True, ff_repr=ff)

    def floor(self):
        res_expr = self.expr.floor() if self.expr is not None else None
        ff = f"floor({self._ff_repr})" if self._ff_repr is not None else None
        return self._create_next_expr(method_name="floor", result_expr=res_expr, is_complex=True, ff_repr=ff)

    @staticmethod
    def _get_expr_repr(expr):
        """Helper to get appropriate string representation for an expression"""
        if isinstance(expr, Expr | Column):
            return expr._repr_str
        elif isinstance(expr, str):
            return f"pl.col('{expr}')"
        elif isinstance(expr, pl.Expr):
            return Expr(expr)._repr_str
        else:
            return repr(expr)

    def over(
        self,
        partition_by: ExprStrOrList,  # Use the type alias defined earlier
        *more_exprs: ExprOrStr,
        order_by: ExprStrOrList | None = None,
        descending: bool = False,
        nulls_last: bool = False,
        mapping_strategy: Literal["group_to_rows", "join", "explode"] = "group_to_rows",
    ) -> Expr:
        """
        Compute expressions over the given groups.
        String representation will show 'descending' and 'nulls_last' if they are True,
        regardless of 'order_by' presence.
        """
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
                processed_order_by = [col(o) if isinstance(o, str) else o for o in order_by]
            else:
                processed_order_by = order_by

        over_arg_strings_for_repr = []

        if built_in_len(processed_partition_cols) == 1:
            over_arg_strings_for_repr.append(self._get_expr_repr(processed_partition_cols[0]))
        else:
            col_reprs = [self._get_expr_repr(p) for p in processed_partition_cols]
            over_arg_strings_for_repr.append(f"[{', '.join(col_reprs)}]")

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
                if built_in_len(processed_partition_cols) == 1:
                    partition_arg = (
                        processed_partition_cols[0].expr
                        if hasattr(processed_partition_cols[0], "expr")
                        else processed_partition_cols[0]
                    )
                else:
                    partition_arg = [p.expr if hasattr(p, "expr") else p for p in processed_partition_cols]

                polars_call_kwargs = {"mapping_strategy": mapping_strategy}

                if processed_order_by is not None:
                    if isinstance(processed_order_by, list):
                        polars_order_by_arg = [o.expr if hasattr(o, "expr") else o for o in processed_order_by]
                    else:
                        polars_order_by_arg = (
                            processed_order_by.expr if hasattr(processed_order_by, "expr") else processed_order_by
                        )
                    polars_call_kwargs["order_by"] = polars_order_by_arg
                    # These are tied to order_by for the actual Polars call
                    polars_call_kwargs["descending"] = descending
                    polars_call_kwargs["nulls_last"] = nulls_last

                res_expr = self.expr.over(partition_by=partition_arg, **polars_call_kwargs)

            except Exception as e:
                logger.warning(f"Could not create polars expression for over(): {e}")

        result = Expr(
            res_expr,
            self.column_name,
            repr_str=f"{self._repr_str}.over({args_str_for_repr})",
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=None,
            _function_sources=self._function_sources,
        )
        if order_by is None and not descending and not nulls_last and mapping_strategy == "group_to_rows":
            result._window_spec = self._partition_aggregate_spec(processed_partition_cols)
        return result

    def _partition_aggregate_spec(self, partition_cols: list) -> dict[str, Any] | None:
        """Structured form of ``pl.col(x).<agg>()`` over plain columns, so that
        ``FlowFrame.with_columns`` can emit a native Window Functions node
        instead of a Polars-code node. ``None`` when the expression is anything else."""
        func = self.agg_func
        source = self._initial_column_name
        if func not in transform_schema.AGGREGATE_WINDOW_FUNCTIONS or source is None:
            return None
        expected_args = "ddof=1" if func == "std" else ""
        if self._repr_str != f"pl.col({source!r}).{func}({expected_args})":
            return None
        partition_by: list[str] = []
        for part in partition_cols:
            if not isinstance(part, Column) or part._select_input.is_altered:
                return None
            partition_by.append(part.column_name)
        return {"column": source, "function": func, "partition_by": partition_by}

    def get_polars_code(self) -> str:
        """
        Get the Polars code representation of this expression, including any function definitions.

        Returns
        -------
        str
            The complete Polars code including function definitions if any.
        """
        if not self._function_sources:
            return self._repr_str

        unique_sources = []
        seen = set()
        for source in self._function_sources:
            if source not in seen:
                seen.add(source)
                unique_sources.append(source)

        functions_section = "# Function definitions\n" + "\n\n".join(unique_sources)
        return functions_section + "\n#─────SPLIT─────\n\n" + self._repr_str

    def sort(self, *, descending=False, nulls_last=False):
        res_expr = self.expr.sort(descending=descending, nulls_last=nulls_last) if self.expr is not None else None
        return Expr(
            res_expr,
            self.column_name,
            repr_str=f"{self._repr_str}.sort(descending={descending}, nulls_last={nulls_last})",
            initial_column_name=self._initial_column_name,
            agg_func=None,
            _function_sources=self._function_sources,
        )

    def cast(self, dtype: pl.DataType | str | pl.datatypes.classes.DataTypeClass, *, strict=True):
        """Casts the Expr to a specified data type."""
        pl_dtype = dtype
        dtype_repr = repr(dtype)

        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}"
            except AttributeError:
                pass
        elif hasattr(dtype, "__name__"):
            dtype_repr = f"pl.{dtype.__name__}"
        elif isinstance(dtype, pl.DataType):
            dtype_repr = f"pl.{dtype!s}"

        res_expr = self.expr.cast(pl_dtype, strict=strict) if self.expr is not None else None
        cast_ff = _compute_cast_ff_repr(self._ff_repr, pl_dtype, strict)

        # Cast preserves aggregation status (e.g., cast(col('a').sum()))
        new_expr = Expr(
            res_expr,
            self.column_name,
            repr_str=f"{self._repr_str}.cast({dtype_repr}, strict={strict})",
            initial_column_name=self._initial_column_name,
            selector=None,
            agg_func=self.agg_func,
            is_complex=True,
            convertable_to_code=self.convertable_to_code,
            _function_sources=self._function_sources,
            ff_repr=cast_ff,
        )
        return new_expr


class Column(Expr):
    """Special Expr representing a single column, preserving column identity through alias/cast."""

    _select_input: transform_schema.SelectInput

    def __init__(self, name: str, select_input: transform_schema.SelectInput | None = None):
        refuse_parameter_as_column(name, "ff.col")
        super().__init__(
            expr=pl.col(name),
            column_name=name,
            repr_str=f"pl.col('{name}')",
            initial_column_name=select_input.old_name if select_input else name,
            selector=None,
            agg_func=None,
            ff_repr=f"[{name}]",
        )
        self._select_input = select_input or transform_schema.SelectInput(old_name=name)

    def alias(self, new_name: str) -> Column:
        """Rename a column, returning a new Column instance."""
        refuse_parameter_as_column(new_name, "alias")
        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=new_name,
            data_type=self._select_input.data_type,
            data_type_change=self._select_input.data_type_change,
            is_altered=True,
        )
        if self.expr is None:
            raise ValueError("Cannot alias Column without underlying polars expression.")

        new_pl_expr = self.expr.alias(new_name)
        new_repr = f"{self._repr_str}.alias({repr(new_name)})"

        new_column = Column(new_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        new_column._ff_repr = self._ff_repr

        new_column.agg_func = self.agg_func
        new_column.is_complex = self.is_complex
        return new_column

    def cast(self, dtype: pl.DataType | str | pl.datatypes.classes.DataTypeClass, *, strict=True) -> Column:
        """Change the data type of a column, returning a new Column instance."""
        pl_dtype = dtype
        dtype_repr = repr(dtype)

        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}"
            except AttributeError:
                pass
        elif hasattr(dtype, "__name__"):
            dtype_repr = f"pl.{dtype.__name__}"
        elif isinstance(dtype, pl.DataType):
            dtype_repr = f"pl.{dtype!s}"

        if not isinstance(pl_dtype, pl.DataType):
            try:
                pl_dtype_instance = pl_dtype()
                if isinstance(pl_dtype_instance, pl.DataType):
                    pl_dtype = pl_dtype_instance
            except TypeError:
                raise TypeError(f"Invalid Polars data type specified for cast: {dtype}") from None

        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=self._select_input.new_name,
            data_type=str(pl_dtype),
            data_type_change=True,
            is_altered=True,
        )
        if self.expr is None:
            raise ValueError("Cannot cast Column without underlying polars expression.")

        new_pl_expr = self.expr.cast(pl_dtype, strict=strict)
        new_repr = f"{self._repr_str}.cast({dtype_repr}, strict={strict})"
        display_name = self._select_input.new_name or self._select_input.old_name

        cast_ff = _compute_cast_ff_repr(self._ff_repr, pl_dtype, strict)

        new_column = Column(display_name, new_select)
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        new_column._ff_repr = cast_ff
        new_column.agg_func = self.agg_func
        new_column.is_complex = True
        return new_column

    def to_select_input(self) -> transform_schema.SelectInput:
        """Convert Column state back to a SelectInput schema object."""
        current_name = self.column_name
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
            is_altered=final_is_altered,
        )

    @property
    def str(self) -> StringMethods:
        return super().str

    @property
    def dt(self) -> DateTimeMethods:
        return super().dt


add_expr_methods(Expr)


class When(Expr):
    """Class that represents a when-then-otherwise expression chain.

    Alongside the Polars source string, the chain keeps a flowfile-formula form
    (``if c1 then v1 elseif c2 then v2 else v endif``) whenever every condition
    and value has one, so ``with_columns`` can lower it onto a native Formula
    node. A chain used without ``otherwise`` renders with ``else null``, which is
    Polars' implicit default.
    """

    def __init__(self, condition):
        """Initialize a When expression with a condition."""
        condition = _frame_value(condition)
        condition_expr, condition_repr = self._get_expr_and_repr(condition)
        self.condition = condition_expr

        repr_str = f"pl.when({condition_repr})"
        super().__init__(expr=None, repr_str=repr_str, is_complex=True)
        self._absorb(condition)
        self._branch_expr = None
        self._ff_branches: list[tuple[str | None, str | None]] = []
        self._pending_ff_condition = self._get_branch_ff_repr(condition)

    def _absorb(self, value) -> None:
        """Carry a branch operand's function sources and code-convertibility on the chain."""
        self._function_sources, self.convertable_to_code = _merge_operand_metadata(self, value)

    @staticmethod
    def _get_expr_and_repr(value):
        """Extract expression and representation from a value (a Polars expression comes wrapped)."""
        if hasattr(value, "expr") and hasattr(value, "_repr_str"):
            return value.expr, value._repr_str
        elif isinstance(value, str) and not value.startswith("pl."):
            col_obj = col(value)
            return col_obj.expr, f"'{value}'"
        else:
            return value, python_literal(value, set())

    @staticmethod
    def _get_branch_ff_repr(value) -> str | None:
        """Formula form of a branch operand; bare strings are column names, as in Polars."""
        if isinstance(value, str) and not value.startswith("pl."):
            return f"[{value}]"
        return _get_ff_repr(value)

    def _build_ff_repr(self, else_ff: str | None) -> str | None:
        if else_ff is None or not self._ff_branches:
            return None
        parts = []
        for i, (condition_ff, then_ff) in enumerate(self._ff_branches):
            if condition_ff is None or then_ff is None:
                return None
            parts.append(f"{'if' if i == 0 else 'elseif'} {condition_ff} then {then_ff}")
        return f"{' '.join(parts)} else {else_ff} endif"

    def then(self, value):
        """Set the value to use when the condition is True."""
        value = _frame_value(value)
        value_expr, value_repr = self._get_expr_and_repr(value)

        self._repr_str = f"{self._repr_str}.then({value_repr})"
        try:
            chain = self._branch_expr if self._branch_expr is not None else pl.when(self.condition)
            self._branch_expr = chain.then(value_expr)
        except Exception as e:
            logger.warning(f"Error in then() creation: {e}")
        # A then() is a Polars expression (a when() alone is not), so the chain takes a method or is an argument.
        self.expr = self._branch_expr if isinstance(self._branch_expr, pl.Expr) else None
        self._absorb(value)

        self._ff_branches.append((self._pending_ff_condition, self._get_branch_ff_repr(value)))
        self._ff_repr = self._build_ff_repr("null")
        return self

    def otherwise(self, value):
        """Set the value to use when no condition is True."""
        value = _frame_value(value)
        value_expr, value_repr = self._get_expr_and_repr(value)
        final_repr = f"{self._repr_str}.otherwise({value_repr})"

        pl_expr = None
        try:
            if self._branch_expr is not None:
                pl_expr = self._branch_expr.otherwise(value_expr)
        except Exception as e:
            logger.warning(f"Could not create when-then-otherwise expression: {e}")

        sources, convertable = _merge_operand_metadata(self, value)
        return Expr(
            pl_expr,
            repr_str=final_repr,
            convertable_to_code=convertable,
            _function_sources=sources,
            ff_repr=self._build_ff_repr(self._get_branch_ff_repr(value)),
        )

    def when(self, condition):
        """Create a new branch in the chain."""
        if self._branch_expr is None:
            logger.warning("Cannot add new branch without a then() first")
            return self

        condition = _frame_value(condition)
        condition_expr, condition_repr = self._get_expr_and_repr(condition)

        self._repr_str = f"{self._repr_str}.when({condition_repr})"

        try:
            self._branch_expr = self._branch_expr.when(condition_expr)
        except Exception as e:
            logger.warning(f"Error adding new when() branch: {e}")
        self.expr = None  # a chained when() awaits its then()
        self._absorb(condition)

        self._pending_ff_condition = self._get_branch_ff_repr(condition)
        self._ff_repr = None
        return self


# --- Top-Level Functions ---
def col(name: str) -> Column:
    """Creates a Column expression."""
    return Column(name)


def column(name: str) -> Column:
    """Alias for col(). Creates a Column expression."""
    return Column(name)


def lit(value: Any) -> Expr:
    """Creates a Literal expression; a ``Parameter`` becomes its ``to_expr()``."""
    if isinstance(value, Parameter):
        return value.to_expr()
    # Literals don't have an agg_func
    return Expr(pl.lit(value, allow_object=True), repr_str=f"pl.lit({python_literal(value, set())})", agg_func=None,
                ff_repr=_get_ff_repr(value))


def len() -> Expr:
    return Expr(pl.len(), repr_str="pl.len()")


def agg_function(func=None, *, customize_repr=True):
    """
    Enhanced decorator for aggregation functions that sets appropriate properties
    and handles representation issues, now supporting all args and kwargs.

    Parameters
    ----------
    func : function, optional
        The aggregation function to decorate
    customize_repr : bool, default True
        Whether to create a custom representation string for the function

    Returns
    -------
    function
        A wrapped function that returns a properly configured Expr
    """

    def decorator(func):
        agg_func_name = func.__name__  # Use the function name as the agg_func

        @wraps(func)
        def wrapper(*args, **kwargs):
            from flowfile_frame.expr import Expr

            pl_expr = func(*args, **kwargs)

            if customize_repr:
                args_reprs = []
                for arg in args:
                    if isinstance(arg, str):
                        args_reprs.append(f"'{arg}'")
                    elif hasattr(arg, "_repr_str"):
                        args_reprs.append(arg._repr_str)
                    else:
                        args_reprs.append(repr(arg))

                kwargs_reprs = []
                for k, v in kwargs.items():
                    if isinstance(v, str) and not (k == "method" or k == "mapping_strategy"):
                        kwargs_reprs.append(f"{k}='{v}'")
                    elif isinstance(v, pl.DataType):
                        kwargs_reprs.append(f"{k}={v!s}")
                    elif isinstance(v, type) and issubclass(v, pl.DataType):
                        kwargs_reprs.append(f"{k}=pl.{v.__name__}")
                    else:
                        kwargs_reprs.append(f"{k}={repr(v)}")

                all_args = args_reprs + kwargs_reprs
                args_str = ", ".join(all_args)
                repr_str = f"pl.{agg_func_name}({args_str})"
            else:
                repr_str = None

            initial_column_name = None
            if built_in_len(args) > 0:
                first_arg = args[0]
                if isinstance(first_arg, str):
                    initial_column_name = first_arg
                elif hasattr(first_arg, "column_name"):
                    initial_column_name = first_arg.column_name

            is_complex = True
            if built_in_len(args) == 1 and isinstance(args[0], str) and not kwargs:
                is_complex = False

            return Expr(
                pl_expr,
                repr_str=repr_str,
                initial_column_name=initial_column_name,
                agg_func=agg_func_name,
                is_complex=is_complex,
            )

        return wrapper

    # Handle both @agg_function and @agg_function(customize_repr=True)
    if func is None:
        return decorator
    return decorator(func)


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
    if built_in_len(names) == 0:
        return pl.last()
    return pl.last(*names)


@agg_function
def mean(*names) -> Expr:
    return pl.mean(*names)


@agg_function
def count(*names) -> Expr:
    return pl.count(*names)


@agg_function
def implode(*names) -> Expr:
    return pl.implode(*names)


@agg_function
def explode(*names) -> Expr:
    return pl.explode(*names)


@agg_function
def sum(*names) -> Expr:
    return pl.sum(*names)


@agg_function
def corr(a: str | Expr, b: str | Expr, *, method: str = "pearson", propagate_nans: bool = False) -> Expr:
    """
    Compute the correlation between two columns.
    """
    a_expr = a.expr if isinstance(a, Expr) else pl.col(a) if isinstance(a, str) else a
    b_expr = b.expr if isinstance(b, Expr) else pl.col(b) if isinstance(b, str) else b

    return pl.corr(a_expr, b_expr, method=method, propagate_nans=propagate_nans)


@agg_function
def cov(a: str | Expr, b: str | Expr, ddof: int = 1) -> Expr:
    """
    Compute the covariance between two columns.
    """
    a_expr = a.expr if isinstance(a, Expr) else pl.col(a) if isinstance(a, str) else a
    b_expr = b.expr if isinstance(b, Expr) else pl.col(b) if isinstance(b, str) else b

    return pl.cov(a_expr, b_expr, ddof=ddof)


def std(column, ddof) -> Expr:
    return Expr(column, ddof=ddof, agg_func="std")


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
