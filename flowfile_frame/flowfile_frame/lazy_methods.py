import polars as pl
from functools import wraps
from typing import Optional, TypeVar, Type, Callable
from flowfile_frame.utils import _get_function_source
from flowfile_frame.config import logger

T = TypeVar('T')
FlowFrameT = TypeVar('FlowFrameT', bound='FlowFrame')

PASSTHROUGH_METHODS = {
    'collect', 'collect_async', 'profile', 'describe', 'explain',
    'show_graph', 'fetch', 'collect_schema', 'columns', 'dtypes',
    'schema', 'width', 'estimated_size', 'n_chunks', 'is_empty',
    'chunk_lengths', 'get_meta'
}


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
    # Determine if the original method returns a LazyFrame based on known method names
    lazyframe_returning_methods = {
        "drop", "select", "with_columns", "sort", "filter", "join", "head", "tail",
        "limit", "drop_nulls", "fill_null", "with_row_index", "group_by", "explode",
        "unique", "slice", "shift", "reverse", "max", "min", "sum", "mean", "median",
        "std", "var", "drop_nans", "fill_nan", "interpolate", "null_count", "quantile",
        "unpivot", "melt", "first", "last"
    }

    non_lazyframe_methods = {
        "collect", "collect_schema", "fetch", "columns", "dtypes", "schema", "width",
        "describe", "explain", "profile", "show_graph"
    }

    returns_lazyframe = (
            method_name in lazyframe_returning_methods or
            (method_name not in non_lazyframe_methods and not method_name.startswith("_"))
    )

    @wraps(original_method)
    def wrapper(self, *args, description: Optional[str] = None, **kwargs):
        # Import here to avoid circular imports
        from flowfile_frame.flow_frame import generate_node_id
        new_node_id = generate_node_id()

        if not all([True if not hasattr(arg, "convertable_to_code") else getattr(arg, 'convertable_to_code') for arg in
                    args]):
            logger.debug("Warning, could not create a good node")
            return self.__class__(getattr(self.data, method_name)(arg.expr for arg in args), flow_graph=self.flow_graph)

        # Collect function sources and build representations
        function_sources = []
        args_representations = []
        kwargs_representations = []

        # Process positional arguments
        for arg in args:
            if callable(arg) and not isinstance(arg, type):
                # Try to get function source
                try:
                    source, is_module_level = _get_function_source(arg)
                    if source and hasattr(arg, '__name__') and arg.__name__ != '<lambda>':
                        function_sources.append(source)
                        # Use the function name in the representation
                        args_representations.append(arg.__name__)
                    else:
                        # Fallback to repr if we can't get the source
                        args_representations.append(repr(arg))
                except:
                    args_representations.append(repr(arg))
            else:
                args_representations.append(repr(arg))
        # Process keyword arguments
        for key, value in kwargs.items():
            if callable(value) and not isinstance(value, type):
                # Try to get function source
                try:
                    source, is_module_level = _get_function_source(value)
                    if source and hasattr(value, '__name__') and value.__name__ != '<lambda>':
                        function_sources.append(source)
                        kwargs_representations.append(f"{key}={value.__name__}")
                    else:
                        kwargs_representations.append(f"{key}={repr(value)}")
                except:
                    kwargs_representations.append(f"{key}={repr(value)}")
            else:
                kwargs_representations.append(f"{key}={repr(value)}")

        # Build parameter string
        args_str = ", ".join(args_representations)
        kwargs_str = ", ".join(kwargs_representations)

        if args_str and kwargs_str:
            params_str = f"{args_str}, {kwargs_str}"
        elif args_str:
            params_str = args_str
        elif kwargs_str:
            params_str = kwargs_str
        else:
            params_str = ""

        # Build the code
        operation_code = f"input_df.{method_name}({params_str})"

        if function_sources:
            unique_sources = []
            seen = set()
            for source in function_sources:
                if source not in seen:
                    seen.add(source)
                    unique_sources.append(source)

            functions_section = "# Function definitions\n" + "\n\n".join(unique_sources)
            code = functions_section + "\n#─────SPLIT─────\n\noutput_df = " + operation_code
        else:
            code = "output_df = " + operation_code

        # Use provided description or generate a default one
        if description is None:
            description = f"{method_name.replace('_', ' ').title()} operation"

        self._add_polars_code(new_node_id, code, description)

        if returns_lazyframe:
            # Return a new FlowFrame with the result
            return self._create_child_frame(new_node_id)
        else:
            # For methods that don't return a LazyFrame, return the result directly
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
    # Get methods already defined in the class (including inherited methods)
    existing_methods = set(dir(cls))

    # Skip properties and private methods
    skip_methods = {
        name for name in dir(pl.LazyFrame)
        if name.startswith('_') or isinstance(getattr(pl.LazyFrame, name), property)
    }

    # Add all public LazyFrame methods that don't already exist
    for name in dir(pl.LazyFrame):
        if name in existing_methods or name in skip_methods:
            continue
        attr = getattr(pl.LazyFrame, name)
        if name in PASSTHROUGH_METHODS:
            def create_passthrough_method(method_name, method_attr):

                @wraps(method_attr)
                def passthrough_method(self, *args, **kwargs):
                    return getattr(self.data, method_name)(*args, **kwargs)

                return passthrough_method

            setattr(cls, name, create_passthrough_method(name, attr))

        else:
            attr = getattr(pl.LazyFrame, name)
            if callable(attr):
                wrapped_method = create_lazyframe_method_wrapper(name, attr)
                setattr(cls, name, wrapped_method)

    overlap = {
        name for name in existing_methods
        if name in dir(pl.LazyFrame) and not name.startswith('_') and callable(getattr(pl.LazyFrame, name))
    }
    if overlap:
        logger.debug(f"Preserved existing methods in {cls.__name__}: {', '.join(sorted(overlap))}")
    return cls