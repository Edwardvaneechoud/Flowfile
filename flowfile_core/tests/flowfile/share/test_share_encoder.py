"""Round-trip, determinism, and agreement with the link the docs site ships.

Never compare the base64 blobs: DEFLATE implementations differ, so the only
meaningful comparison between two encoders is what their payloads *say*.
"""

import re
from pathlib import Path

import pytest

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.share import build_share_link
from flowfile_core.flowfile.share.encoder import (
    SHARE_HASH_PREFIX,
    WASM_DESIGNER_URL,
    build_share_url,
    decode_share_hash,
    encode_envelope,
)
from flowfile_core.flowfile.share.transform import build_share_envelope
from flowfile_core.schemas import input_schema, transform_schema

from tests.flowfile.share.conftest import add_read, make_graph

REPO_ROOT = Path(__file__).resolve().parents[4]
DOCS_LINK_HTML = REPO_ROOT / "docs" / "assets" / "try-sales-pipeline.html"
DOCS_FLOW_YAML = REPO_ROOT / "docs" / "assets" / "flows" / "sales_pipeline.yaml"


def _sales_pipeline_graph():
    """The docs flow, with its advanced filter rewritten as the basic one the
    hand-baked link carries (an advanced filter is executable and never travels)."""
    graph = open_flow(DOCS_FLOW_YAML)
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="quantity", operator="greater_than", value="7"),
            ),
        )
    )
    return graph


def test_round_trip_returns_the_same_envelope(canary_flow):
    envelope = build_share_envelope(canary_flow).envelope
    url, hash_chars = build_share_url(envelope)

    assert url.startswith(f"{WASM_DESIGNER_URL}{SHARE_HASH_PREFIX}")
    assert hash_chars == len(url.split(SHARE_HASH_PREFIX, 1)[1])
    assert decode_share_hash(url) == envelope
    assert decode_share_hash(url.split(SHARE_HASH_PREFIX, 1)[1]) == envelope


def test_encoding_is_deterministic():
    first = build_share_envelope(_sales_pipeline_graph()).envelope
    second = build_share_envelope(_sales_pipeline_graph()).envelope
    assert encode_envelope(first) == encode_envelope(second)


def test_padding_is_stripped_and_url_safe():
    blob = encode_envelope({"v": 1, "flow": {"nodes": []}})
    assert "=" not in blob
    assert re.fullmatch(r"[A-Za-z0-9_-]+", blob)


@pytest.mark.parametrize(
    "value",
    ["", "   ", "not base64 at all!!", "#flow=", "#flow=abcd", WASM_DESIGNER_URL, 42, None],
    ids=lambda v: repr(v)[:24],
)
def test_malformed_input_decodes_to_none(value):
    assert decode_share_hash(value) is None


def test_non_object_payload_decodes_to_none():
    blob = encode_envelope([1, 2, 3])
    assert decode_share_hash(blob) is None


def test_matches_the_link_the_docs_site_ships():
    """Semantic equivalence with the hand-baked docs link — never a byte comparison."""
    baked_blob = re.search(r"#flow=([A-Za-z0-9_-]+)", DOCS_LINK_HTML.read_text(encoding="utf-8")).group(1)
    baked = decode_share_hash(baked_blob)
    assert baked is not None

    minted = decode_share_hash(build_share_link(_sales_pipeline_graph()).url)

    assert minted["v"] == baked["v"] == 1
    assert "files" not in minted and "files" not in baked
    assert minted["flow"]["flowfile_id"] == baked["flow"]["flowfile_id"] == 1
    # open_flow names a graph after its file stem; the baked link used the display name.
    assert minted["flow"]["flowfile_name"].replace("_", " ").lower() == baked["flow"]["flowfile_name"].lower()
    for key, value in baked["flow"]["flowfile_settings"].items():
        if key != "description":
            assert minted["flow"]["flowfile_settings"][key] == value

    minted_nodes = {node["id"]: node for node in minted["flow"]["nodes"]}
    baked_nodes = {node["id"]: node for node in baked["flow"]["nodes"]}
    assert [n["type"] for n in minted["flow"]["nodes"]] == [n["type"] for n in baked["flow"]["nodes"]]
    assert set(minted_nodes) == set(baked_nodes)

    minted_read = minted_nodes[1]["setting_input"]["received_file"]
    baked_read = baked_nodes[1]["setting_input"]["received_file"]
    assert minted_read["path"] == baked_read["path"]
    assert minted_read["file_type"] == baked_read["file_type"]

    minted_filter = minted_nodes[3]["setting_input"]["filter_input"]
    baked_filter = baked_nodes[3]["setting_input"]["filter_input"]
    assert minted_filter["mode"] == baked_filter["mode"] == "basic"
    for field in ("field", "operator", "value"):
        assert minted_filter["basic_filter"][field] == baked_filter["basic_filter"][field]

    def aggs(node):
        return {
            (col["old_name"], col["agg"], col["new_name"])
            for col in node["setting_input"]["groupby_input"]["agg_cols"]
        }

    assert aggs(minted_nodes[4]) == aggs(baked_nodes[4])


def test_oversized_flow_refuses_to_mint_a_url(monkeypatch):
    from flowfile_core.flowfile import share

    monkeypatch.setattr(share, "_REFUSE_LINK_CHARS", 10)
    graph = make_graph(name="too_big")
    add_read(graph, 1, "/tmp/data.csv")

    response = share.build_share_link(graph)
    assert response.url is None
    assert response.compatible is False
    assert any("too large" in warning for warning in response.warnings)
    assert any("node 1 (read)" in warning for warning in response.warnings)


def test_long_link_only_warns(monkeypatch):
    from flowfile_core.flowfile import share

    monkeypatch.setattr(share, "_LONG_LINK_CHARS", 10)
    graph = make_graph(name="longish")
    add_read(graph, 1, "/tmp/data.csv")

    response = share.build_share_link(graph)
    assert response.url is not None
    assert response.compatible is True
    assert any("Long link" in warning for warning in response.warnings)
