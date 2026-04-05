"""Frame-level helper for reading from REST APIs."""

from __future__ import annotations

from typing import Any, Literal

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.api_schemas import (
    ApiAuth,
    ApiPagination,
    ApiReadSettings,
    JsonPath,
    NoPagination,
)
from flowfile_frame.api.connection_manager import get_current_user_id
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.utils import create_flow_graph, generate_node_id


def read_api(
    url: str,
    *,
    method: Literal["GET", "POST", "PUT", "PATCH"] = "GET",
    headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
    body: dict[str, Any] | str | None = None,
    body_content_type: Literal["json", "form"] = "json",
    auth: ApiAuth | None = None,
    pagination: ApiPagination | None = None,
    records_path: JsonPath | None = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_backoff: float = 1.0,
    rate_limit_delay: float = 0.0,
    verify_ssl: bool = True,
    connection_name: str | None = None,
    flow_graph: FlowGraph | None = None,
    description: str | None = None,
) -> FlowFrame:
    """Read data from a REST API into a FlowFrame.

    Supports authentication, pagination, retries, and rate limiting.
    Data is fetched lazily — pages are only retrieved when ``.collect()``
    is called.

    Args:
        url: The API endpoint URL.
        method: HTTP method (GET, POST, PUT, PATCH).
        headers: Extra HTTP headers.
        query_params: Extra query parameters.
        body: Request body (for POST/PUT/PATCH).
        body_content_type: Body encoding ("json" or "form").
        auth: Authentication config (BearerAuth, ApiKeyAuth, etc.).
        pagination: Pagination strategy (OffsetPagination, CursorPagination, etc.).
        records_path: Typed path to the records array in the response,
            e.g. ``("data", "users")``.  ``None`` means the response itself
            is the array.
        timeout: Request timeout in seconds.
        max_retries: Maximum number of retries on failure.
        retry_backoff: Base delay for exponential backoff.
        rate_limit_delay: Seconds to wait between paginated requests.
        verify_ssl: Whether to verify SSL certificates.
        connection_name: Name of a stored API connection.
        flow_graph: Existing FlowGraph to add the node to.
        description: Optional node description.

    Returns:
        FlowFrame: A lazy frame wrapping the API data.
    """
    node_id = generate_node_id()
    if flow_graph is None:
        flow_graph = create_flow_graph()

    connection_mode = "reference" if connection_name else "inline"

    settings = input_schema.NodeApiReader(
        flow_id=flow_graph.flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        description=description,
        api_settings=ApiReadSettings(
            url=url,
            method=method,
            headers=headers,
            query_params=query_params,
            body=body,
            body_content_type=body_content_type,
            auth=auth,
            pagination=pagination or NoPagination(),
            records_path=records_path,
            timeout=timeout,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            rate_limit_delay=rate_limit_delay,
            verify_ssl=verify_ssl,
            connection_name=connection_name,
            connection_mode=connection_mode,
        ),
    )
    flow_graph.add_api_reader(settings)
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id,
    )
