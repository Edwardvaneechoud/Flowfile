from typing import List, Union, TYPE_CHECKING  # Added TYPE_CHECKING

import polars as pl

# --- TYPE CHECKING IMPORTS ---
# if TYPE_CHECKING:
#     Import Expr only for type hints
from flowfile_frame.expr import Expr


# --- Selector Base Classes (Compound, Complement) ---

class Selector:
    """Base class for column selectors, inspired by polars.selectors"""
    def __init__(self):
        self._repr_str = self._get_repr_str()  # Use base repr calculation method
        # No agg_func state stored here anymore

    @property
    def repr_str(self):
        return self._repr_str

    def _get_repr_str(self) -> str:
        """Get representation string for the selector itself."""
        # Default implementation, specific selectors override this
        return f"pl.selectors.{self.__class__.__name__}()"

    def __repr__(self) -> str:
        return self._repr_str

    def __or__(self, other: 'Selector') -> 'CompoundSelector':
        return CompoundSelector(self, other, 'union')

    def __and__(self, other: 'Selector') -> 'CompoundSelector':
        return CompoundSelector(self, other, 'intersection')

    def __sub__(self, other: 'Selector') -> 'CompoundSelector':
        return CompoundSelector(self, other, 'difference')

    def __xor__(self, other: 'Selector') -> 'CompoundSelector':
        return CompoundSelector(self, other, 'symmetric_difference')

    def __invert__(self) -> 'ComplementSelector':
        return ComplementSelector(self)

    # --- Aggregation Methods ---
    # These methods now return Expr objects, importing Expr locally

    def sum(self) -> 'Expr':
        """Create an expression to sum columns selected by this selector."""
        # Expr init will handle creating the 'pl.sum(selector)' repr
        return Expr(expr=None, selector=self, agg_func="sum")

    def mean(self) -> 'Expr':
        """Create an expression to average columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="mean")

    def median(self) -> 'Expr':
        """Create an expression to find the median of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="median")

    def min(self) -> 'Expr':
        """Create an expression to find the minimum of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="min")

    def max(self) -> 'Expr':
        """Create an expression to find the maximum of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="max")

    def std(self, ddof: int = 1) -> 'Expr':
        """Create an expression to find the standard deviation of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="std", ddof=ddof)

    def var(self, ddof: int = 1) -> 'Expr':
        """Create an expression to find the variance of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="var", ddof=ddof)

    def first(self) -> 'Expr':
        """Create an expression to get the first element of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="first")

    def last(self) -> 'Expr':
        """Create an expression to get the last element of columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="last")

    def count(self) -> 'Expr':
        """Create an expression to count elements in columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="count")

    def n_unique(self) -> 'Expr':
        """Create an expression to count unique elements in columns selected by this selector."""
        return Expr(expr=None, selector=self, agg_func="n_unique")

    # Removed alias method - belongs on Expr


class CompoundSelector(Selector):
    """Selector representing a compound operation between two selectors"""
    def __init__(self, left: Selector, right: Selector, operation: str):
        self.left = left
        self.right = right
        self.operation = operation
        super().__init__()

    def _get_repr_str(self) -> str:
        op_map = {'union': '|', 'intersection': '&', 'difference': '-', 'symmetric_difference': '^'}
        op_symbol = op_map.get(self.operation, '|')
        # Use base repr (_repr_str) of operands
        left_repr = f"({self.left._repr_str})" if isinstance(self.left, CompoundSelector) else self.left._repr_str
        right_repr = f"({self.right._repr_str})" if isinstance(self.right, CompoundSelector) else self.right._repr_str
        return f"{left_repr} {op_symbol} {right_repr}"


class ComplementSelector(Selector):
    """Selector representing the complement (NOT) of another selector"""
    def __init__(self, selector: Selector):
        self.selector = selector
        super().__init__()

    def _get_repr_str(self) -> str:
        selector_repr = f"({self.selector._repr_str})" if isinstance(self.selector, CompoundSelector) else self.selector._repr_str
        return f"~{selector_repr}"


class NumericSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.numeric()"


class FloatSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.float()"


class IntegerSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.integer()"


class StringSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.string()"


class TemporalSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.temporal()"


class DatetimeSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.datetime()"


class DateSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.date()"


class TimeSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.time()"


class DurationSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.duration()"


class BooleanSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.boolean()"


class CategoricalSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.categorical()"


class ObjectSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.object()"


class ListSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.list()"


class StructSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.struct()"


class AllSelector(Selector):
    def _get_repr_str(self) -> str: return "pl.selectors.all()"


class DtypeSelector(Selector):
    def __init__(self, dtypes: Union[pl.DataType, List[pl.DataType]]):
        self.dtypes = dtypes if isinstance(dtypes, list) else [dtypes]
        super().__init__()

    def _get_repr_str(self) -> str:
        dtype_strs = []
        for dt in self.dtypes:
            dt_repr = repr(dt)
            if dt_repr.startswith("DataType"):
                dt_repr = str(dt).capitalize()
            dtype_strs.append(f"pl.{dt_repr}")
        dtype_repr_arg = dtype_strs[0] if len(dtype_strs) == 1 else f"[{', '.join(dtype_strs)}]"
        return f"pl.selectors.by_dtype({dtype_repr_arg})"


class PatternSelector(Selector):
    def __init__(self, pattern: str):
        self.pattern = pattern
        super().__init__()


class ContainsSelector(PatternSelector):
    def _get_repr_str(self) -> str: return f"pl.selectors.contains({self.pattern!r})"


class StartsWithSelector(PatternSelector):
    def _get_repr_str(self) -> str: return f"pl.selectors.starts_with({self.pattern!r})"


class EndsWithSelector(PatternSelector):
    def _get_repr_str(self) -> str: return f"pl.selectors.ends_with({self.pattern!r})"


class MatchesSelector(PatternSelector):
    def _get_repr_str(self) -> str: return f"pl.selectors.matches({self.pattern!r})"


def numeric() -> NumericSelector: return NumericSelector()
def float_() -> FloatSelector: return FloatSelector()
def integer() -> IntegerSelector: return IntegerSelector()
def string() -> StringSelector: return StringSelector()
def temporal() -> TemporalSelector: return TemporalSelector()
def datetime() -> DatetimeSelector: return DatetimeSelector()
def date() -> DateSelector: return DateSelector()
def time() -> TimeSelector: return TimeSelector()
def duration() -> DurationSelector: return DurationSelector()
def boolean() -> BooleanSelector: return BooleanSelector()
def categorical() -> CategoricalSelector: return CategoricalSelector()
def object_() -> ObjectSelector: return ObjectSelector()
def list_() -> ListSelector: return ListSelector()
def struct() -> StructSelector: return StructSelector()
def all_() -> AllSelector: return AllSelector()
def by_dtype(dtypes: Union[pl.DataType, List[pl.DataType]]) -> DtypeSelector: return DtypeSelector(dtypes)
def contains(pattern: str) -> ContainsSelector: return ContainsSelector(pattern)
def starts_with(pattern: str) -> StartsWithSelector: return StartsWithSelector(pattern)
def ends_with(pattern: str) -> EndsWithSelector: return EndsWithSelector(pattern)
def matches(pattern: str) -> MatchesSelector: return MatchesSelector(pattern)
