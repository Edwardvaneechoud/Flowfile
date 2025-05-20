import polars as pl
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Type, TYPE_CHECKING

T = TypeVar('T')
ExprT = TypeVar('ExprT', bound='Expr')


def create_expr_method_wrapper(method_name: str, original_method: Callable) -> Callable:
    """
    Creates a wrapper for a polars Expr method that properly integrates with your custom Expr class.

    Parameters
    ----------
    method_name : str
        Name of the polars Expr method.
    original_method : Callable
        The original polars Expr method.

    Returns
    -------
    Callable
        A wrapper method appropriate for your Expr class.
    """
    from flowfile_frame.expr import Expr
    @wraps(original_method)
    def wrapper(self: Expr, *args, **kwargs):
        from flowfile_frame.expr import Expr
        # Check if we have a valid underlying expression
        if self.expr is None:
            raise ValueError(
                f"Cannot call '{method_name}' on Expr with no underlying polars expression."
            )

        # Call the method on the underlying polars expression
        try:
            result_expr = getattr(self.expr, method_name)(*args, **kwargs)
        except Exception as e:
            print(f"Warning: Error in {method_name}() call: {e}")
            result_expr = None

        # Format arguments for repr string
        args_repr = ", ".join(repr(arg) for arg in args)
        kwargs_repr = ", ".join(f"{k}={repr(v)}" for k, v in kwargs.items())

        if args_repr and kwargs_repr:
            params_repr = f"{args_repr}, {kwargs_repr}"
        elif args_repr:
            params_repr = args_repr
        elif kwargs_repr:
            params_repr = kwargs_repr
        else:
            params_repr = ""

        # Create the repr string for this method call
        new_repr = f"{self._repr_str}.{method_name}({params_repr})"

        # Methods that typically change the aggregation status or complexity
        agg_methods = {
            "sum", "mean", "min", "max", "median", "first", "last", "std", "var",
            "count", "n_unique", "quantile", "implode", "explode"
        }
        # Methods that typically make expressions complex
        complex_methods = {
            "filter", "map", "shift", "fill_null", "fill_nan", "round", "abs", "alias",
            "cast", "is_between", "over", "sort", "arg_sort", "arg_unique", "arg_min",
            "arg_max", "rolling", "interpolate", "ewm_mean", "ewm_std", "ewm_var",
            "backward_fill", "forward_fill", "rank", "diff", "clip", "dot", "mode",
            "drop_nulls", "drop_nans", "take", "gather", "filter", "shift_and_fill"
        }

        # Determine new agg_func status
        new_agg_func = method_name if method_name in agg_methods else self.agg_func

        # Determine if this makes the expression complex
        is_complex = self.is_complex or method_name in complex_methods
        return self._create_next_expr(*args, **kwargs, result_expr=result_expr, is_complex=True,
                                      method_name=method_name)

    return wrapper


def add_expr_methods(cls: Type[ExprT]) -> Type[ExprT]:
    """
    Class decorator that adds all polars Expr methods to a custom Expr class.

    This adds the methods at class creation time, so they are visible to static type checkers.
    Methods already defined in the class are not overwritten.

    Parameters
    ----------
    cls : Type[ExprT]
        The class to which the methods will be added.

    Returns
    -------
    Type[ExprT]
        The modified class.
    """
    # Get methods already defined in the class (including inherited methods)
    existing_methods = set(dir(cls))

    # Skip properties and private methods
    skip_methods = {
        name for name in dir(pl.Expr)
        if name.startswith('_') or isinstance(getattr(pl.Expr, name, None), property)
    }

    # Add all public Expr methods that don't already exist
    for name in dir(pl.Expr):
        if name in existing_methods or name in skip_methods:
            continue

        attr = getattr(pl.Expr, name)
        if callable(attr):
            wrapped_method = create_expr_method_wrapper(name, attr)
            setattr(cls, name, wrapped_method)

    overlap = {
        name for name in existing_methods
        if name in dir(pl.Expr) and not name.startswith('_') and callable(getattr(pl.Expr, name))
    }
    if overlap:
        print(f"Preserved existing methods in {cls.__name__}: {', '.join(sorted(overlap))}")

    return cls


