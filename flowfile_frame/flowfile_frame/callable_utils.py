"""
Utilities for resolving callable source code (lambdas and named functions)
and processing callable arguments for code generation.

This module centralizes the logic for extracting function definitions so
that FlowFrame graph nodes store human-readable code instead of serialized
LazyFrame blobs.
"""

import ast
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Low-level extraction helpers
# ---------------------------------------------------------------------------


def _get_function_source(func) -> tuple[str | None, bool]:
    """
    Get the source code of a named function if possible.

    Returns:
        tuple: (source_code, is_module_level)
    """
    try:
        source = inspect.getsource(func)

        if func.__name__ == "<lambda>":
            return None, False

        is_module_level = func.__code__.co_flags & 0x10 == 0
        source = textwrap.dedent(source)
        return source, is_module_level
    except (OSError, TypeError):
        return None, False


def _is_safely_representable(value: Any) -> bool:
    """Check if a value can be safely round-tripped through repr()."""
    if isinstance(value, (int, float, bool, str, bytes, type(None))):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_safely_representable(item) for item in value)
    if isinstance(value, dict):
        return all(
            _is_safely_representable(k) and _is_safely_representable(v)
            for k, v in value.items()
        )
    if isinstance(value, set):
        return all(_is_safely_representable(item) for item in value)
    return False


def _extract_lambda_source(func) -> tuple[str | None, str | None]:
    """
    Extract a lambda's source code and convert it to a named function definition.

    Uses inspect.getsource() + AST parsing to find the lambda's argument list
    and body, then generates a named function definition.  Closure variables
    are captured as constant assignments so the generated code is self-contained.

    Returns:
        (function_definition_source, function_name) or (None, None) on failure.
    """
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        return None, None

    source = textwrap.dedent(source).strip()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None, None

    lambdas = [node for node in ast.walk(tree) if isinstance(node, ast.Lambda)]
    if not lambdas:
        return None, None

    # Match the lambda to our function based on argument names
    expected_args = list(func.__code__.co_varnames[: func.__code__.co_argcount])
    matched_lambda = None
    for lambda_node in lambdas:
        node_args = [arg.arg for arg in lambda_node.args.args]
        if node_args == expected_args:
            matched_lambda = lambda_node
            break

    if matched_lambda is None:
        matched_lambda = lambdas[0]

    func_name = f"_lambda_fn_{abs(hash(func.__code__)) % 100000}"

    args_str = ast.unparse(matched_lambda.args)
    body_str = ast.unparse(matched_lambda.body)

    # Capture closure variables
    closure_defs: list[str] = []
    if func.__code__.co_freevars and func.__closure__:
        for var_name, cell in zip(func.__code__.co_freevars, func.__closure__):
            try:
                value = cell.cell_contents
            except ValueError:
                return None, None

            if not _is_safely_representable(value):
                return None, None

            closure_defs.append(f"{var_name} = {repr(value)}")

    lines: list[str] = []
    if closure_defs:
        lines.extend(closure_defs)
        lines.append("")
    lines.append(f"def {func_name}({args_str}):")
    lines.append(f"    return {body_str}")

    return "\n".join(lines), func_name


# ---------------------------------------------------------------------------
# High-level resolution
# ---------------------------------------------------------------------------


@dataclass
class ResolvedCallable:
    """Result of resolving a single callable for code generation."""

    source: str | None
    """Function definition source code, or ``None`` if extraction failed."""

    name: str
    """Name to use in the generated code (function name or ``repr(func)``)."""

    resolved: bool
    """Whether source code was successfully extracted."""


def resolve_callable(func: Any) -> ResolvedCallable:
    """
    Resolve a callable (lambda or named function) to its source code.

    * For lambdas: attempts AST extraction via ``_extract_lambda_source``.
    * For named functions: uses ``_get_function_source``.
    * Falls back to ``repr(func)`` if extraction fails.
    """
    if hasattr(func, "__name__") and func.__name__ == "<lambda>":
        func_def, func_name = _extract_lambda_source(func)
        if func_def and func_name:
            return ResolvedCallable(source=func_def, name=func_name, resolved=True)
        return ResolvedCallable(source=None, name=repr(func), resolved=False)

    if hasattr(func, "__name__"):
        try:
            source, _ = _get_function_source(func)
        except Exception:
            source = None
        if source:
            return ResolvedCallable(source=source, name=func.__name__, resolved=True)
        # Named function but source unavailable (e.g. built-in) — still use its name
        return ResolvedCallable(source=None, name=func.__name__, resolved=True)

    return ResolvedCallable(source=None, name=repr(func), resolved=False)


# ---------------------------------------------------------------------------
# Batch argument processing
# ---------------------------------------------------------------------------


@dataclass
class ProcessedArgs:
    """Result of processing a function's args and kwargs for code generation."""

    args_reprs: list[str] = field(default_factory=list)
    """String representations for each positional argument."""

    kwargs_reprs: list[str] = field(default_factory=list)
    """String representations for each keyword argument (``key=value``)."""

    function_sources: list[str] = field(default_factory=list)
    """Collected function definition source strings."""

    all_resolved: bool = True
    """False if any callable could not be resolved to source code."""

    @property
    def params_repr(self) -> str:
        """Join args and kwargs into a single comma-separated parameter string."""
        args_str = ", ".join(self.args_reprs)
        kwargs_str = ", ".join(self.kwargs_reprs)
        if args_str and kwargs_str:
            return f"{args_str}, {kwargs_str}"
        return args_str or kwargs_str


def process_callable_args(args: tuple, kwargs: dict) -> ProcessedArgs:
    """
    Process positional and keyword arguments, resolving any callables to source code.

    Non-callable arguments are converted via ``repr()``.

    Returns a :class:`ProcessedArgs` collecting the string representations,
    extracted function sources, and an ``all_resolved`` flag.
    """
    result = ProcessedArgs()

    for arg in args:
        if callable(arg) and not isinstance(arg, type):
            try:
                resolved = resolve_callable(arg)
                result.args_reprs.append(resolved.name)
                if resolved.source:
                    result.function_sources.append(resolved.source)
                if not resolved.resolved:
                    result.all_resolved = False
            except Exception:
                result.args_reprs.append(repr(arg))
                result.all_resolved = False
        else:
            result.args_reprs.append(repr(arg))

    for key, value in kwargs.items():
        if callable(value) and not isinstance(value, type):
            try:
                resolved = resolve_callable(value)
                result.kwargs_reprs.append(f"{key}={resolved.name}")
                if resolved.source:
                    result.function_sources.append(resolved.source)
                if not resolved.resolved:
                    result.all_resolved = False
            except Exception:
                result.kwargs_reprs.append(f"{key}={repr(value)}")
                result.all_resolved = False
        else:
            result.kwargs_reprs.append(f"{key}={repr(value)}")

    return result
