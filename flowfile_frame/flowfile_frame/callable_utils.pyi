# Auto-generated stub for flowfile_frame.callable_utils — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any

class ResolvedCallable:
    source: str | None
    name: str
    resolved: bool

class ProcessedArgs:
    args_reprs: list[str]
    kwargs_reprs: list[str]
    function_sources: list[str]
    all_resolved: bool
    @property
    def params_repr(self) -> str: ...


def resolve_callable(func: Any) -> ResolvedCallable: ...
def process_callable_args(args: tuple, kwargs: dict) -> ProcessedArgs: ...
