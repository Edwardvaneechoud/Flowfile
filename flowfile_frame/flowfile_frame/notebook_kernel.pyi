# Auto-generated stub for flowfile_frame.notebook_kernel — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Callable
from typing import Any

COMPUTED_HERE_TYPES: frozenset[str]
transport: Callable[[dict[str, Any]], dict[str, Any]]
run_transport: Callable[[dict[str, Any]], dict[str, Any]]
database_transport: Callable[[], dict[str, Any]]

def post_node_result(body: dict[str, Any]) -> dict[str, Any]: ...
def post_node_run(body: dict[str, Any]) -> dict[str, Any]: ...
def post_database_refresh() -> dict[str, Any]: ...
def handle(request_json: str, namespace: dict[str, Any]) -> None: ...
