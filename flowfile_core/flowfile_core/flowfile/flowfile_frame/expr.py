import uuid
from typing import Any, Optional, Union, Iterable

import polars as pl
from polars.expr.string import ExprStringNameSpace

from flowfile_core.schemas import transform_schema


# --- Helper Functions ---

def _repr_args(*args, **kwargs):
    """Helper to represent arguments for __repr__."""
    arg_reprs = [repr(a) for a in args]
    kwarg_reprs = []
    for k, v in kwargs.items():
        # Check if the value is a Polars DataType instance
        # Use the imported base class polars.DataType
        if isinstance(v, pl.DataType):
            kwarg_reprs.append(f"{k}={v!s}") # Use str(v) which should give 'Boolean' or similar

        elif isinstance(v, type) and issubclass(v, pl.DataType):
            kwarg_reprs.append(f"{k}=pl.{v.__name__}")
        else:
            kwarg_reprs.append(f"{k}={repr(v)}")
    # Remove the print unless debugging
    # print(kwarg_reprs)
    return ", ".join(arg_reprs + kwarg_reprs)


def _get_expr_and_repr(value: Any) -> tuple[Any, str]:
    """Helper to get polars expr and repr string for operands."""
    if isinstance(value, Expr):
        return value.expr, value._repr_str
    elif isinstance(value, pl.Expr):

        return value, f"pl.{value}"
    else:
        # Assume literal
        return pl.lit(value), repr(value)


class StringMethods:
    """String methods namespace for expressions, maintaining repr."""
    expr: ExprStringNameSpace

    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.str
        # Store the representation string of the parent expression
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, method_name: str, result_expr: pl.Expr, *args, **kwargs) -> 'Expr':
        """Factory method to create the next Expr with updated repr."""
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.str.{method_name}({args_repr})"
        # String methods usually result in an Expr, not necessarily a Column
        return Expr(result_expr, self.parent.name, repr_str=new_repr)

    def contains(self, pattern, *, literal=False):
        """Check if string contains pattern."""
        return self._create_next_expr(
            "contains",
            self.expr.contains(pattern, literal=literal),
            pattern, literal=literal
        )

    def starts_with(self, prefix):
        """Check if string starts with prefix."""
        return self._create_next_expr(
            "starts_with",
            self.expr.starts_with(prefix),
            prefix
        )

    def ends_with(self, suffix):
        """Check if string ends with suffix."""
        return self._create_next_expr(
            "ends_with",
            self.expr.ends_with(suffix),
            suffix
        )

    def replace(self, pattern, replacement, *, literal=False):
        """Replace pattern in string."""
        return self._create_next_expr(
            "replace",
            self.expr.replace(pattern, replacement, literal=literal),
            pattern, replacement, literal=literal
        )

    def to_uppercase(self):
        """Convert string to uppercase."""
        return self._create_next_expr(
            "to_uppercase",
            self.expr.to_uppercase()
        )

    def to_lowercase(self):
        """Convert string to lowercase."""
        return self._create_next_expr(
            "to_lowercase",
            self.expr.to_lowercase()
        )

    def len_chars(self):
        return self._create_next_expr(
            "len_chars",
            self.expr.len_chars()
        )

    def len_bytes(self):
        """Get string length."""
        return self._create_next_expr(
            "len_bytes",
            self.expr.len_bytes()
        )

    def to_titlecase(self):
        """ Convert a string to title case """
        return self._create_next_expr(
            "to_titlecase",
            self.expr.to_titlecase()
        )

    # Dynamic forwarding for other string methods
    def __getattr__(self, name):
        if not hasattr(self.expr, name):
             raise AttributeError(f"'StringMethods' object has no attribute '{name}'")
        pl_attr = getattr(self.expr, name)

        if callable(pl_attr):
            def wrapper(*args, **kwargs):
                result = pl_attr(*args, **kwargs)
                # Use the factory to create the next Expr
                return self._create_next_expr(name, result, *args, **kwargs)
            return wrapper
        else:
            # If it's not callable, return the attribute directly.
            # It's unlikely to return an Expr, so no repr modification needed.
            return pl_attr


class DateTimeMethods:
    """DateTime methods namespace for expressions, maintaining repr."""
    def __init__(self, parent_expr: 'Expr', parent_repr_str: str):
        self.parent = parent_expr
        self.expr = parent_expr.expr.dt
        # Store the representation string of the parent expression
        self.parent_repr_str = parent_repr_str

    def _create_next_expr(self, method_name: str, result_expr: pl.Expr, *args, **kwargs) -> 'Expr':
        """Factory method to create the next Expr with updated repr."""
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self.parent_repr_str}.dt.{method_name}({args_repr})"
         # Datetime methods usually result in an Expr, not necessarily a Column
        return Expr(result_expr, self.parent.name, repr_str=new_repr)

    def year(self):
        """Extract year from date."""
        return self._create_next_expr("year", self.expr.year())

    def month(self):
        """Extract month from date."""
        return self._create_next_expr("month", self.expr.month())

    def day(self):
        """Extract day from date."""
        return self._create_next_expr("day", self.expr.day())

    def hour(self):
        """Extract hour from date."""
        return self._create_next_expr("hour", self.expr.hour())

    def minute(self):
        """Extract minute from date."""
        return self._create_next_expr("minute", self.expr.minute())

    def second(self):
        """Extract second from date."""
        return self._create_next_expr("second", self.expr.second())

    # Dynamic forwarding for other datetime methods
    def __getattr__(self, name):
        if not hasattr(self.expr, name):
             raise AttributeError(f"'DateTimeMethods' object has no attribute '{name}'")
        pl_attr = getattr(self.expr, name)

        if callable(pl_attr):
            def wrapper(*args, **kwargs):
                result = pl_attr(*args, **kwargs)
                 # Use the factory to create the next Expr
                return self._create_next_expr(name, result, *args, **kwargs)
            return wrapper
        else:
            # If it's not callable, return the attribute directly.
            return pl_attr


class Expr:
    _initial_column_name: str
    """Wrapper around Polars expressions with chained representation."""

    def __init__(self, expr: pl.Expr, column_name: Optional[str] = None, repr_str: Optional[str] = None,
                 initial_column_name: str = None):
        self.expr = expr
        self.name = column_name
        self.agg_func = None  # Track aggregation for GroupByFrame
        if initial_column_name:
            self._initial_column_name = initial_column_name
        else:
            self._initial_column_name = column_name
        # Determine the representation string
        if repr_str is not None:
            self._repr_str = repr_str
        else:
            # Basic fallback if no repr_str is provided (e.g., direct Expr instantiation)
            # Might need refinement based on how Expr is directly created
            self._repr_str = f"pl.Expr({self.expr})" # Default fallback

        # Try to extract column name if not explicitly given
        if self.name is None:
            try:
                # Polars >= 0.19 uses _output_name
                self.name = self.expr._output_name
            except AttributeError:
                 try:
                     # Older polars might use _name
                     self.name = self.expr._name
                 except AttributeError:
                     pass # Keep name as None if not found

        self._str_namespace: Optional[StringMethods] = None
        self._dt_namespace: Optional[DateTimeMethods] = None

    def __repr__(self) -> str:
        """Return the stored representation string."""
        return self._repr_str

    # --- Factory for creating next expression ---
    def _create_next_expr(self, method_name: str, result_expr: pl.Expr, *args, **kwargs) -> 'Expr':
        """Creates a new Expr instance with an updated representation string."""
        args_repr = _repr_args(*args, **kwargs)
        new_repr = f"{self._repr_str}.{method_name}({args_repr})"
        new_expr_instance = Expr(result_expr, self.name, repr_str=new_repr, initial_column_name=self._initial_column_name)
        # Carry over aggregation function if set previously
        if hasattr(self, 'agg_func') and self.agg_func:
            new_expr_instance.agg_func = self.agg_func
        return new_expr_instance

    # --- Factory for binary/comparison operations ---
    def _create_binary_op_expr(self, op_symbol: str, other: Any, result_expr: pl.Expr) -> 'Expr':
        """Creates a new Expr for binary operations like +, -, ==, > etc."""
        _, other_repr = _get_expr_and_repr(other)
        # Enclose operands in parentheses if they are complex (contain operators themselves)
        # Basic check: contains space could indicate an operation
        left_repr = f"({self._repr_str})" if ' ' in self._repr_str else self._repr_str
        right_repr = f"({other_repr})" if ' ' in other_repr else other_repr

        new_repr = f"{left_repr} {op_symbol} {right_repr}"
        # Result name might be lost in binary ops, keep parent's name for now
        return Expr(result_expr, self.name, repr_str=new_repr, initial_column_name=self._initial_column_name)

    # --- Property accessors for nested namespaces ---
    @property
    def str(self) -> StringMethods:
        """Access string methods namespace."""
        if self._str_namespace is None:
            # Pass self and its repr string to the namespace constructor
            self._str_namespace = StringMethods(self, self._repr_str)
        return self._str_namespace

    @property
    def dt(self) -> DateTimeMethods:
        """Access datetime methods namespace."""
        if self._dt_namespace is None:
            # Pass self and its repr string to the namespace constructor
            self._dt_namespace = DateTimeMethods(self, self._repr_str)
        return self._dt_namespace

    # --- Aggregation methods ---
    # (Each returns a new Expr with updated repr and agg_func set)
    def sum(self):
        result = self._create_next_expr("sum", self.expr.sum())
        result.agg_func = "sum"
        return result

    def mean(self):
        result = self._create_next_expr("mean", self.expr.mean())
        result.agg_func = "mean"
        return result

    def min(self):
        result = self._create_next_expr("min", self.expr.min())
        result.agg_func = "min"
        return result

    def max(self):
        result = self._create_next_expr("max", self.expr.max())
        result.agg_func = "max"
        return result

    def median(self):
        result = self._create_next_expr("median", self.expr.median())
        result.agg_func = "median"
        return result

    def count(self):
        result = self._create_next_expr("count", self.expr.count())
        result.agg_func = "count"
        return result

    def first(self):
        result = self._create_next_expr("first", self.expr.first())
        result.agg_func = "first"
        return result

    def last(self):
        result = self._create_next_expr("last", self.expr.last())
        result.agg_func = "last"
        return result

    def n_unique(self):
        result = self._create_next_expr("n_unique", self.expr.n_unique())
        result.agg_func = "n_unique"
        return result

    def std(self, ddof=1): # Include ddof for polars compatibility
        result = self._create_next_expr("std", self.expr.std(ddof=ddof), ddof=ddof)
        result.agg_func = "std"
        return result

    def var(self, ddof=1): # Include ddof for polars compatibility
        result = self._create_next_expr("var", self.expr.var(ddof=ddof), ddof=ddof)
        result.agg_func = "var"
        return result

    # --- Arithmetic operations ---
    # Use the _create_binary_op_expr helper
    def __add__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("+", other, self.expr + other_expr)
    def __sub__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("-", other, self.expr - other_expr)
    def __mul__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("*", other, self.expr * other_expr)
    def __truediv__(self, other): # Use for standard division /
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("/", other, self.expr / other_expr)
    def __floordiv__(self, other): # Use for floor division //
         other_expr, _ = _get_expr_and_repr(other)
         return self._create_binary_op_expr("//", other, self.expr // other_expr)
    def __pow__(self, exponent):
        exp_expr, _ = _get_expr_and_repr(exponent)
        return self._create_binary_op_expr("**", exponent, self.expr.pow(exp_expr)) # Polars uses pow() method
    def __mod__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("%", other, self.expr % other_expr)

    # Allow right-side operations (e.g., 1 + col('a'))
    def __radd__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} + {self._repr_str}" # Order matters
        return Expr(other_expr + self.expr, self.name, repr_str=new_repr)
    def __rsub__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} - {self._repr_str}"
        return Expr(other_expr - self.expr, self.name, repr_str=new_repr)
    def __rmul__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} * {self._repr_str}"
        return Expr(other_expr * self.expr, self.name, repr_str=new_repr)
    def __rtruediv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} / {self._repr_str}"
        return Expr(other_expr / self.expr, self.name, repr_str=new_repr)
    def __rfloordiv__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} // {self._repr_str}"
        return Expr(other_expr // self.expr, self.name, repr_str=new_repr)
    def __rmod__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} % {self._repr_str}"
        return Expr(other_expr % self.expr, self.name, repr_str=new_repr)
    def __rpow__(self, other):
        other_expr, other_repr = _get_expr_and_repr(other)
        new_repr = f"{other_repr} ** {self._repr_str}"
        # Polars doesn't directly support literal ** expr, use pl.lit(base).pow(expr)
        return Expr(pl.lit(other).pow(self.expr), self.name, repr_str=new_repr)


    # --- Comparison operations ---
    def __eq__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("==", other, self.expr == other_expr)
    def __ne__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("!=", other, self.expr != other_expr)
    def __gt__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr(">", other, self.expr > other_expr)
    def __lt__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("<", other, self.expr < other_expr)
    def __ge__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr(">=", other, self.expr >= other_expr)
    def __le__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("<=", other, self.expr <= other_expr)

    # --- Logical operations ---
    def __and__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("&", other, self.expr & other_expr)
    def __or__(self, other):
        other_expr, _ = _get_expr_and_repr(other)
        return self._create_binary_op_expr("|", other, self.expr | other_expr)
    def __invert__(self): # For ~ (NOT)
        new_repr = f"~({self._repr_str})"
        return Expr(~self.expr, self.name, repr_str=new_repr)

    # --- Other useful methods ---
    def is_null(self):
        return self._create_next_expr("is_null", self.expr.is_null())

    def is_not_null(self):
        return self._create_next_expr("is_not_null", self.expr.is_not_null())

    def is_in(self, values):
        return self._create_next_expr("is_in", self.expr.is_in(values), values)

    def create_expression(self, expr: "Expr", column_name: str = None, repr_str: str = None):
        return Expr(expr, column_name, repr_str, initial_column_name=self._initial_column_name)

    def alias(self, name):
        """Rename the expression result."""
        # This should typically return a new Expr, not modify in place
        new_expr = self.expr.alias(name)
        new_repr = f"{self._repr_str}.alias({repr(name)})"
        # Create a new Expr, passing the new name and repr
        new_instance = self.create_expression(new_expr, name, repr_str=new_repr)

        if hasattr(self, "agg_func") and self.agg_func is not None:
            new_instance.agg_func = self.agg_func

        return new_instance

    # --- Miscellaneous ---
    def fill_null(self, value):
        return self._create_next_expr("fill_null", self.expr.fill_null(value), value)

    def fill_nan(self, value):
        # Make sure the underlying expr supports fill_nan
        if hasattr(self.expr, 'fill_nan'):
            return self._create_next_expr("fill_nan", self.expr.fill_nan(value), value)
        else:
            return self._create_next_expr("fill_nan", self.expr.fill_nan(value), value)

    def over(self, partition_by):
        # Handling 'over' representation can be complex depending on input type
        partition_repr = repr(partition_by)
        if isinstance(partition_by, Expr):
            partition_repr = partition_by._repr_str
        elif isinstance(partition_by, list) and all(isinstance(p, Expr) for p in partition_by):
             partition_repr = f"[{', '.join(p._repr_str for p in partition_by)}]"
        elif isinstance(partition_by, list) and all(isinstance(p, str) for p in partition_by):
             partition_repr = repr(partition_by) # Keep as list of strings repr

        return self._create_next_expr("over", self.expr.over(partition_by), partition_by=partition_repr) # Pass repr string

    def sort(self, *, descending=False, nulls_last=False): # Added nulls_last for polars compat
        return self._create_next_expr("sort", self.expr.sort(descending=descending, nulls_last=nulls_last), descending=descending, nulls_last=nulls_last)

    def shift(self, periods=1, *, fill_value=None): # Added fill_value
        return self._create_next_expr("shift", self.expr.shift(periods=periods, fill_value=fill_value), periods=periods, fill_value=fill_value)

    def cast(self, dtype: Union[pl.DataType, str], *, strict=True):
        """ Casts the Expr to a specified data type. """
        # Get the polars dtype object if a string is passed
        pl_dtype = dtype
        dtype_repr = repr(dtype)
        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}"
            except AttributeError:
                # If it's not a direct polars type name, keep original string repr
                pass  # Keep original repr(dtype) and pl_dtype as string
        else:
            dtype_repr = "pl." + dtype_repr

        # Create the next expression
        return self._create_next_expr(method_name="cast", result_expr=self.expr.cast(pl_dtype, strict=strict),
                                      dtype=pl_dtype, strict=strict)


class Column(Expr):
    """
    Special Expr representing a column, trying to maintain Column type for select ops.
    It also generates the initial representation string.
    """

    def __init__(
        self, name: str, select_input: Optional[transform_schema.SelectInput] = None
    ):
        # The initial representation string is defined here
        initial_repr = f"pl.col('{name}')"
        super().__init__(pl.col(name), name, repr_str=initial_repr, initial_column_name=select_input.old_name if select_input else name)
        self._select_input = select_input or transform_schema.SelectInput(old_name=name)

    def alias(self, new_name: str) -> "Column":
        """Rename a column, returning a new Column with updated repr."""
        # Prepare SelectInput (as before)
        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=new_name,
            data_type=self._select_input.data_type,
            data_type_change=self._select_input.data_type_change,
            is_altered=True,
        )
        # Create underlying polars expression
        new_pl_expr = self.expr.alias(new_name)
        # Create the new representation string
        new_repr = f"{self._repr_str}.alias({repr(new_name)})"

        new_column = Column(new_name, new_select)
        # Explicitly set the expr and repr_str on the new instance
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        return new_column

    def cast(self, dtype: Union[pl.DataType, str], *, strict=True) -> "Column":
        """Change the data type of a column, returning a new Column with updated repr."""
        new_select = transform_schema.SelectInput(
            old_name=self._select_input.old_name,
            new_name=self._select_input.new_name,
            data_type=str(dtype),
            data_type_change=True,
            is_altered=True,
        )

        pl_dtype = dtype
        dtype_repr = repr(dtype)
        if isinstance(dtype, str):
            try:
                pl_dtype = getattr(pl, dtype)
                dtype_repr = f"pl.{dtype}" # Use pl. prefix if it's a known type
            except AttributeError:
                pass # Keep original string representation
        else:
            dtype_repr = f"pl.{dtype_repr}"
        new_pl_expr = self.expr.cast(pl_dtype, strict=strict)
        new_repr = f"{self._repr_str}.cast({dtype_repr}, strict={strict})"
        display_name = self._select_input.new_name or self._select_input.old_name

        new_column = Column(display_name, new_select)
         # Explicitly set the expr and repr_str on the new instance
        new_column.expr = new_pl_expr
        new_column._repr_str = new_repr
        return new_column


    def to_select_input(self) -> transform_schema.SelectInput:
        """Convert to SelectInput."""
        # If alias or cast was used, _select_input was updated. Return it.
        # If not, create one based on the current state.
        # This might need adjustment if other Column methods modify the intended selection state.
        current_name = self.name # Name reflects alias if applied
        original_name = self._select_input.old_name if self._select_input else current_name # Track original name
        new_name = current_name if current_name != original_name else None

        # If cast was used, _select_input.data_type reflects it.
        data_type = self._select_input.data_type if self._select_input and self._select_input.data_type_change else None
        data_type_change = bool(data_type)

        is_altered = bool(new_name or data_type_change)


        return transform_schema.SelectInput(
            old_name=original_name,
            new_name=new_name,
            data_type=data_type,
            data_type_change=data_type_change,
            is_altered=is_altered,
        )

    # Override str and dt properties to ensure they use the correct repr string
    # They will return Expr instances, not Column instances, because the operation
    # fundamentally changes the data, not just the column selection/metadata.
    @property
    def str(self) -> StringMethods:
        """Access string methods namespace."""
        # This will correctly pass the Column's current _repr_str
        return super().str

    @property
    def dt(self) -> DateTimeMethods:
        """Access datetime methods namespace."""
        # This will correctly pass the Column's current _repr_str
        return super().dt

# --- Top-Level Functions ---

# Modified `col` to use the new Column class directly
def col(name: str) -> Column:
    """Create a Column object."""
    return Column(name)

# `column` is often used as an alias too
def column(name: str) -> Column:
    """Create a Column object."""
    return Column(name)

# Modified `lit` to create an Expr with the correct initial repr_str
def lit(value: Any) -> Expr:
    """Create a literal value expression."""
    return Expr(pl.lit(value), repr_str=f"pl.lit({repr(value)})")
