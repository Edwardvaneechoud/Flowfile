"""REST API helper functions for FlowFrame operations.

This module provides a function for reading JSON data from a REST API with
configurable authentication and pagination, mirroring how ``kafka.py`` exposes
``read_kafka``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flowfile_core.schemas import input_schema
    from flowfile_frame.flow_frame import FlowFrame


def get_current_user_id() -> int:
    """Get the current user ID for REST API operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    return 1


def _coerce_auth(auth) -> input_schema.RestApiAuthSettings:
    from flowfile_core.schemas.input_schema import RestApiAuthSettings

    if auth is None:
        return RestApiAuthSettings()
    if isinstance(auth, RestApiAuthSettings):
        return auth
    return RestApiAuthSettings(**auth)


def _coerce_pagination(pagination) -> input_schema.RestApiPaginationSettings:
    from flowfile_core.schemas.input_schema import RestApiPaginationSettings

    if pagination is None:
        return RestApiPaginationSettings()
    if isinstance(pagination, RestApiPaginationSettings):
        return pagination
    return RestApiPaginationSettings(**pagination)


def add_read_from_api(
    flow_graph,
    *,
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    json_body: Any | None = None,
    auth: dict | Any | None = None,
    pagination: dict | Any | None = None,
    record_path: str = "",
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
    description: str | None = None,
) -> int:
    """Add a REST API reader node to the flow graph.

    Args:
        flow_graph: The flow graph to add the node to.
        url: The request URL.
        method: HTTP method, ``"GET"`` or ``"POST"``.
        headers: Optional request headers.
        params: Optional query parameters.
        json_body: Optional JSON request body (POST).
        auth: Authentication settings as a ``RestApiAuthSettings`` or a dict,
            e.g. ``{"auth_type": "bearer", "secret": "..."}``. The plaintext
            secret is encrypted with the master key before persistence.
        pagination: Pagination settings as a ``RestApiPaginationSettings`` or a
            dict, e.g. ``{"pagination_type": "offset", "page_size": 100}``.
        record_path: Dot-path locating the record array in the JSON response.
        timeout_seconds: Per-request timeout.
        max_retries: Max retries for transient failures.
        description: Optional node description.

    Returns:
        int: The node ID of the created REST API reader node.
    """
    from flowfile_core.schemas.input_schema import NodeRestApiReader, RestApiSettings
    from flowfile_frame.utils import generate_node_id

    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    settings = NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        description=description,
        rest_api_settings=RestApiSettings(
            url=url,
            method=method,
            headers=headers or {},
            query_params=params or {},
            json_body=json_body,
            auth=_coerce_auth(auth),
            pagination=_coerce_pagination(pagination),
            record_path=record_path,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        ),
    )

    flow_graph.add_rest_api_reader(settings)
    return node_id


def read_api(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    json_body: Any | None = None,
    auth: dict | Any | None = None,
    pagination: dict | Any | None = None,
    record_path: str = "",
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
    flow_graph=None,
) -> FlowFrame:
    """Read JSON data from a REST API into a FlowFrame.

    Creates a REST API reader node, fetches the response (with the configured
    authentication and pagination), and returns a FlowFrame backed by the
    typed result.

    Args:
        url: The request URL.
        method: HTTP method, ``"GET"`` or ``"POST"``.
        headers: Optional request headers.
        params: Optional query parameters.
        json_body: Optional JSON request body (POST).
        auth: Authentication settings as a ``RestApiAuthSettings`` or a dict,
            e.g. ``{"auth_type": "bearer", "secret": "..."}``.
        pagination: Pagination settings as a ``RestApiPaginationSettings`` or a
            dict, e.g. ``{"pagination_type": "cursor", "cursor_param": "cursor",
            "cursor_response_path": "next"}``.
        record_path: Dot-path locating the record array in the JSON response
            (e.g. ``"data.items"``). Empty uses the top-level response.
        timeout_seconds: Per-request timeout.
        max_retries: Max retries for transient failures.
        flow_graph: Optional existing FlowGraph to add the node to.

    Returns:
        FlowFrame: A FlowFrame backed by a REST API reader node.
    """
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.utils import create_flow_graph

    if flow_graph is None:
        flow_graph = create_flow_graph()

    node_id = add_read_from_api(
        flow_graph,
        url=url,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        auth=auth,
        pagination=pagination,
        record_path=record_path,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id,
    )
