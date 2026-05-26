# Auto-generated stub for flowfile_frame.rest_api — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any
from flowfile_frame.flow_frame import FlowFrame

def get_current_user_id() -> int: ...
def add_read_from_api(flow_graph, *, url: str, method: str='GET', headers: dict[str, str] | None=None, params: dict[str, str] | None=None, json_body: Any | None=None, auth: dict | Any | None=None, pagination: dict | Any | None=None, record_path: str='', timeout_seconds: float=30.0, max_retries: int=3, description: str | None=None) -> int: ...
def read_api(url: str, *, method: str='GET', headers: dict[str, str] | None=None, params: dict[str, str] | None=None, json_body: Any | None=None, auth: dict | Any | None=None, pagination: dict | Any | None=None, record_path: str='', timeout_seconds: float=30.0, max_retries: int=3, flow_graph=None) -> FlowFrame: ...
