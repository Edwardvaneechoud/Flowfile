import polars as pl

def polars_function_wrapper(polars_func_name, is_agg=False):
    """
    Create a wrapper for a polars function that returns an Expr.

    Parameters
    ----------
    polars_func_name : str
        Name of the polars function (e.g., 'fold', 'reduce', etc.)
    is_agg : bool, default False
        Whether this is an aggregation function

    Returns
    -------
    function
        A wrapped function that returns a properly configured Expr
    """
    def wrapper(*args, **kwargs):
        from flowfile_frame.expr import Expr, _get_expr_and_repr

        # Process args for representation
        args_repr = []
        pl_args = []
        convertable_to_code = True

        for arg in args:
            # Handle callable arguments specially (e.g., functions)
            if callable(arg) and not hasattr(arg, 'expr'):
                if hasattr(arg, "__name__") and arg.__name__ != "<lambda>":
                    args_repr.append(arg.__name__)
                else:
                    args_repr.append("<lambda>")
                    convertable_to_code = False
                pl_args.append(arg)
            else:
                # Handle normal arguments
                arg_expr, repr_str = _get_expr_and_repr(arg)
                args_repr.append(repr_str)
                pl_args.append(arg_expr if arg_expr is not None else arg)

        # Process kwargs for representation
        kwargs_repr = []
        pl_kwargs = {}

        for key, value in kwargs.items():
            if callable(value) and not hasattr(value, 'expr'):
                if hasattr(value, "__name__") and value.__name__ != "<lambda>":
                    kwargs_repr.append(f"{key}={value.__name__}")
                else:
                    kwargs_repr.append(f"{key}=<lambda>")
                    convertable_to_code = False
                pl_kwargs[key] = value
            else:
                val_expr, val_repr = _get_expr_and_repr(value)
                kwargs_repr.append(f"{key}={val_repr}")
                pl_kwargs[key] = val_expr if val_expr is not None else value

        # Build the representation string
        full_repr = f"pl.{polars_func_name}({', '.join(args_repr)}"
        if kwargs_repr:
            if args_repr:
                full_repr += ", "
            full_repr += ", ".join(kwargs_repr)
        full_repr += ")"

        # Create the actual polars expression if possible
        pl_expr = None
        try:
            polars_func = getattr(pl, polars_func_name)
            pl_expr = polars_func(*pl_args, **pl_kwargs)
        except Exception as e:
            print(f"Warning: Could not create polars expression for {polars_func_name}(): {e}")

        # Create and return the FlowFile expression
        return Expr(
            pl_expr,
            repr_str=full_repr,
            agg_func=polars_func_name if is_agg else None,
            is_complex=True,
            convertable_to_code=convertable_to_code
        )

    # Set the wrapper's name and docstring
    wrapper.__name__ = polars_func_name
    wrapper.__doc__ = f"""
    FlowFile wrapper for pl.{polars_func_name}.

    See polars documentation for full details on parameters and usage.
    """

    return wrapper

fold = polars_function_wrapper('fold')

