import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

import polars as pl

from flowfile_frame.adding_expr import refuse_parameter_argument
from flowfile_frame.callable_utils import process_callable_args
from flowfile_frame.config import logger
from flowfile_frame.native import NativeNodeError
from flowfile_frame.notebook import current

PASSTHROUGH_METHODS = {
    "collect",
    "collect_async",
    "profile",
    "describe",
    "explain",
    "show_graph",
    "fetch",
    "collect_schema",
    "columns",
    "dtypes",
    "schema",
    "width",
    "estimated_size",
    "n_chunks",
    "is_empty",
    "chunk_lengths",
    "get_meta",
}

# Passthroughs that read data: on a deferred frame they run the flow first, like collect.
MATERIALISING_PASSTHROUGH_METHODS = {"collect_async", "profile", "describe", "fetch"}

PURE_TRANSFORMS = frozenset(
    {
        "drop", "select", "with_columns", "sort", "filter", "join", "head", "tail", "limit", "drop_nulls",
        "fill_null", "with_row_index", "group_by", "explode", "unique", "slice", "shift", "reverse", "max", "min",
        "sum", "mean", "median", "std", "var", "drop_nans", "fill_nan", "interpolate", "null_count", "quantile",
        "unpivot", "melt", "first", "last", "cast", "count", "unnest", "gather_every", "top_k", "bottom_k",
    }
)  # fmt: skip
"""LazyFrame methods that only describe a lazy transform: each returns a frame and reads, writes and runs nothing."""


def _has_build_time_effect(method_name: str) -> bool:
    """Whether the method's Polars-code node acts when it is built: sinks write, ``inspect`` prints."""
    return method_name.startswith("sink_") or method_name == "inspect"


def _holds_a_frame(value: Any) -> bool:
    """Whether ``value`` is a FlowFrame, or a list, tuple or dict holding one at any depth."""
    from flowfile_frame.flow_frame import FlowFrame

    if isinstance(value, FlowFrame):
        return True
    if isinstance(value, dict):
        return any(_holds_a_frame(item) for item in (*value.keys(), *value.values()))
    if isinstance(value, list | tuple):
        return any(_holds_a_frame(item) for item in value)
    return False


def _refuse_frame_argument(method_name: str, *values: Any) -> None:
    """In notebook mode, refuse a FlowFrame among ``values``: a Polars-code node would hold its plan as text."""
    if current() is not None and _holds_a_frame(values):
        raise NativeNodeError(
            f"{method_name} takes no frame as an argument in a notebook: the frame would be written into the "
            "node's code as text, not connected to it"
        )


def _check_notebook_arguments(
    method_name: str, original_method: Callable, frame: Any, args: tuple, kwargs: dict
) -> None:
    """In notebook mode, the arguments must bind to the Polars method and each must have a code form.

    The Polars-code node only holds the call as text: a wrong keyword would fail only when the flow
    runs, a frame argument would be written in as its plan text instead of being connected, and an
    argument without a code form would evaluate the method now into a node detached from the flow.
    """
    try:
        inspect.signature(original_method).bind(frame.data, *args, **kwargs)
    except TypeError as exc:
        raise TypeError(f"{method_name}() {exc}") from None
    _refuse_frame_argument(method_name, args, kwargs)
    if not all(getattr(value, "convertable_to_code", True) for value in (*args, *kwargs.values())):
        raise NativeNodeError(
            f"{method_name} with an expression that has no code form would be evaluated now into a node detached "
            "from the flow, which a notebook never does"
        )


def create_lazyframe_method_wrapper(method_name: str, original_method: Callable) -> Callable:
    """
    Creates a wrapper for a LazyFrame method that properly integrates with FlowFrame.

    Parameters
    ----------
    method_name : str
        Name of the LazyFrame method.
    original_method : Callable
        The original LazyFrame method.

    Returns
    -------
    Callable
        A wrapper method appropriate for FlowFrame.
    """
    non_lazyframe_methods = {
        "collect",
        "collect_schema",
        "fetch",
        "columns",
        "dtypes",
        "schema",
        "width",
        "describe",
        "explain",
        "profile",
        "show_graph",
    }

    returns_lazyframe = method_name in PURE_TRANSFORMS or (
        method_name not in non_lazyframe_methods and not method_name.startswith("_")
    )

    @wraps(original_method)
    def wrapper(self, *args, description: str | None = None, **kwargs):
        # Import here to avoid circular imports
        from flowfile_frame.flow_frame import generate_node_id

        refuse_parameter_argument(
            method_name, args, kwargs, hint="pass a plain value, or an expression such as ff.lit(parameter)"
        )
        if _has_build_time_effect(method_name) and current() is not None:
            raise NativeNodeError(
                f"{method_name} runs when it is built, which a notebook never does. Use a write_* method (a native "
                "Output node that writes when the flow runs)"
            )
        if _has_build_time_effect(method_name) and (self._deferred or self._below_a_gate()):
            reason = (
                "this frame only holds placeholder rows until the flow runs"
                if self._deferred
                else "this frame is below a gate, so building would act on both of its exits"
            )
            raise NativeNodeError(
                f"{method_name} runs when it is built, but {reason}. Use a write_* method (a native Output node "
                "that waits for the run) or collect the frame first."
            )
        if current() is not None:
            _check_notebook_arguments(method_name, original_method, self, args, kwargs)
        new_node_id = generate_node_id()

        if not all([True if not hasattr(arg, "convertable_to_code") else arg.convertable_to_code for arg in args]):
            if self._deferred:
                raise NativeNodeError(
                    f"{method_name} with an expression that has no code form cannot run on a deferred frame: it "
                    "would evaluate the placeholder rows now and detach from the flow. Collect the frame first."
                )
            logger.debug("Warning, could not create a good node")
            return self.__class__(getattr(self.data, method_name)(arg.expr for arg in args), flow_graph=self.flow_graph)

        processed = process_callable_args(args, kwargs)

        operation_code = f"input_df.{method_name}({processed.params_repr})"

        if processed.function_sources:
            unique_sources = list(dict.fromkeys(processed.function_sources))
            functions_section = "# Function definitions\n" + "\n\n".join(unique_sources)
            code = functions_section + "\n#─────SPLIT─────\n\noutput_df = " + operation_code
        else:
            code = "output_df = " + operation_code

        if description is None:
            description = f"{method_name.replace('_', ' ').title()} operation"

        self._add_polars_code(new_node_id, code, description)

        if returns_lazyframe:
            return self._create_child_frame(new_node_id, declared=self._planned_seed(method_name, args, kwargs))
        else:
            return getattr(self.data, method_name)(*args, **kwargs)

    return wrapper


def add_lazyframe_methods(cls):
    """
    Class decorator that adds all LazyFrame methods to a class.

    This adds the methods at class creation time, so they are visible to static type checkers.
    Methods already defined in the class are not overwritten.

    Parameters
    ----------
    cls : Type
        The class to which the methods will be added.

    Returns
    -------
    Type
        The modified class.
    """
    existing_methods = set(dir(cls))

    skip_methods = {
        name for name in dir(pl.LazyFrame) if name.startswith("_") or isinstance(getattr(pl.LazyFrame, name), property)
    }

    for name in dir(pl.LazyFrame):
        if name in existing_methods or name in skip_methods:
            continue
        attr = getattr(pl.LazyFrame, name)
        if name in PASSTHROUGH_METHODS:

            def create_passthrough_method(method_name, method_attr):
                @wraps(method_attr)
                def passthrough_method(self, *args, **kwargs):
                    if method_name in MATERIALISING_PASSTHROUGH_METHODS:
                        return getattr(self._materialised_lazyframe(), method_name)(*args, **kwargs)
                    return getattr(self.data, method_name)(*args, **kwargs)

                return passthrough_method

            setattr(cls, name, create_passthrough_method(name, attr))

        else:
            attr = getattr(pl.LazyFrame, name)
            if callable(attr):
                wrapped_method = create_lazyframe_method_wrapper(name, attr)
                setattr(cls, name, wrapped_method)

    overlap = {
        name
        for name in existing_methods
        if name in dir(pl.LazyFrame) and not name.startswith("_") and callable(getattr(pl.LazyFrame, name))
    }
    if overlap:
        logger.debug(f"Preserved existing methods in {cls.__name__}: {', '.join(sorted(overlap))}")
    return cls
