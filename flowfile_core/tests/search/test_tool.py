"""The search's MCP tool definition and resource URIs stay complete as kinds are added."""

import json

from flowfile_core.search import RESOURCE_URI_TEMPLATES, SEARCH_TOOL_NAME, SearchKind, resource_uri
from flowfile_core.search.providers import PROVIDERS
from flowfile_core.search.tool import search_tool_definition


def test_every_kind_has_a_provider_and_a_uri_template():
    assert set(PROVIDERS) == set(SearchKind)
    assert set(RESOURCE_URI_TEMPLATES) == set(SearchKind)


def test_resource_uris_quote_their_ids():
    assert resource_uri(SearchKind.TABLE, 12) == "flowfile://catalog/tables/12"
    assert resource_uri(SearchKind.KERNEL, "my kernel/1") == "flowfile://kernels/my%20kernel%2F1"
    assert (
        resource_uri(SearchKind.COLUMN, "12:order id", table_id=12, name="order id")
        == "flowfile://catalog/tables/12/columns/order%20id"
    )


def test_tool_definition_is_an_mcp_tool():
    tool = search_tool_definition()
    assert tool["name"] == SEARCH_TOOL_NAME == "flowfile.catalog.search"
    assert tool["inputSchema"]["type"] == "object"
    assert tool["inputSchema"]["required"] == ["query"]
    assert set(tool["inputSchema"]["properties"]) == {"query", "kinds", "limit_per_kind"}
    assert tool["outputSchema"]["type"] == "object"
    assert {"hits", "counts", "tokens"} <= set(tool["outputSchema"]["properties"])
    assert tool["annotations"]["readOnlyHint"] is True
    json.dumps(tool)


def test_every_input_field_is_described_for_an_agent():
    properties = search_tool_definition()["inputSchema"]["properties"]
    assert all(spec.get("description") for spec in properties.values())
