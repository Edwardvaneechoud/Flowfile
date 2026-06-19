# Auto-generated stub for flowfile_frame.lazy — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal
from flowfile_frame.expr import Expr

def polars_function_wrapper(polars_func_name_or_callable: str | Callable, is_agg: bool=False, return_type: Literal['FlowFrame', 'Expr'] | None=None) -> Any: ...
def fold(*args, **kwargs) -> 'Expr': ...
