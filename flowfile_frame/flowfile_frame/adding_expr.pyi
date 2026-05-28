# Auto-generated stub for flowfile_frame.adding_expr — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar
from flowfile_frame.expr import Expr

ExprT = TypeVar('ExprT', bound='Expr')

def create_expr_method_wrapper(method_name: str, original_method: Callable) -> Callable: ...
def add_expr_methods(cls: type[ExprT]) -> type[ExprT]: ...
