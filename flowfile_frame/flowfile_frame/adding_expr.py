from collections.abc import Callable
from functools import wraps
from typing import TypeVar

import polars as pl

from flowfile_frame.callable_utils import process_callable_args, resolve_callable
from flowfile_frame.config import logger

T = TypeVar("T")
ExprT = TypeVar("ExprT", bound="Expr")
PASSTHROUGH_METHODS = {"map_elements", "map_batches"}


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
        # Check if we have a valid underlying expression
        if self.expr is None:
            raise ValueError(f"Cannot call '{method_name}' on Expr with no underlying polars expression.")

        processed = process_callable_args(args, kwargs)

        # Call the method on the underlying polars expression
        try:
            result_expr = getattr(self.expr, method_name)(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Warning: Error in {method_name}() call: {e}")
            result_expr = None

        # Methods that typically change the aggregation status or complexity
        agg_methods = {
            "sum",
            "mean",
            "min",
            "max",
            "median",
            "first",
            "last",
            "std",
            "var",
            "count",
            "n_unique",
            "quantile",
            "implode",
            "explode",
        }
        # Methods that typically make expressions complex
        complex_methods = {
            "filter",
            "map",
            "shift",
            "fill_null",
            "fill_nan",
            "round",
            "abs",
            "alias",
            "cast",
            "is_between",
            "over",
            "sort",
            "arg_sort",
            "arg_unique",
            "arg_min",
            "arg_max",
            "rolling",
            "interpolate",
            "ewm_mean",
            "ewm_std",
            "ewm_var",
            "backward_fill",
            "forward_fill",
            "rank",
            "diff",
            "clip",
            "dot",
            "mode",
            "drop_nulls",
            "drop_nans",
            "take",
            "gather",
            "shift_and_fill",
        }

        # Determine new agg_func status
        new_agg_func = method_name if method_name in agg_methods else self.agg_func

        # Determine if this makes the expression complex
        is_complex = self.is_complex or method_name in complex_methods

        # Pass function sources to _create_next_expr
        result = self._create_next_expr(
            *args,
            **kwargs,
            result_expr=result_expr,
            is_complex=is_complex,
            method_name=method_name,
            _function_sources=processed.function_sources,
        )

        # Set the agg_func if needed
        if new_agg_func != self.agg_func:
            result.agg_func = new_agg_func

        return result

    return wrapper


def add_expr_methods(cls: type[ExprT]) -> type[ExprT]:
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

    skip_methods = {
        name for name in dir(pl.Expr) if name.startswith("_") or isinstance(getattr(pl.Expr, name, None), property)
    }

    # Add all public Expr methods that don't already exist
    for name in dir(pl.Expr):
        if name in existing_methods or name in skip_methods:
            continue

        attr = getattr(pl.Expr, name)
        if callable(attr):
            if name in PASSTHROUGH_METHODS:
                # Create passthrough method that marks the expression as not convertible to code
                def create_passthrough_method(method_name, method_attr):
                    @wraps(method_attr)
                    def passthrough_method(self, *args, **kwargs):
                        if not hasattr(self, "expr") or self.expr is None:
                            raise ValueError(
                                f"Cannot call '{method_name}' on Expr with no underlying polars expression."
                            )

                        processed = process_callable_args(args, kwargs)
                        convertable_to_code = processed.all_resolved

                        if not convertable_to_code:
                            logger.warning(
                                f"Warning: Using anonymous functions in {method_name} is not convertable to UI code"
                            )
                            logger.warning(
                                "Consider using defined functions (def abc(a, b, c): return ...), "
                                "In a separate script"
                            )

                        # Call the underlying polars method
                        result_expr = getattr(self.expr, method_name)(*args, **kwargs)

                        # Create a representation string
                        new_repr = f"{self._repr_str}.{method_name}({processed.params_repr})"
                        # Return a new expression with the convertable_to_code flag set appropriately
                        result = self._create_next_expr(
                            *args,
                            method_name=method_name,
                            result_expr=result_expr,
                            is_complex=True,
                            convertable_to_code=convertable_to_code,
                            _function_sources=processed.function_sources,
                            _repr_override=new_repr,
                            **kwargs,
                        )
                        return result

                    return passthrough_method

                setattr(cls, name, create_passthrough_method(name, attr))
            else:
                # Use standard wrapper for other methods
                wrapped_method = create_expr_method_wrapper(name, attr)
                setattr(cls, name, wrapped_method)

    overlap = {
        name
        for name in existing_methods
        if name in dir(pl.Expr) and not name.startswith("_") and callable(getattr(pl.Expr, name))
    }
    if overlap:
        logger.debug(f"Preserved existing methods in {cls.__name__}: {', '.join(sorted(overlap))}")

    return cls
