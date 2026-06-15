"""Unit tests for the determinism rules in ``workspace.normalize`` (no DB)."""

from __future__ import annotations

import copy

from flowfile_core.workspace.normalize import (
    NORMALIZED_FLOW_ID,
    PathTokenizer,
    canonical_yaml_dump,
    canonical_yaml_load,
    denormalize_flow,
    normalize_flow,
)
from flowfile_core.workspace.secret_resolver import (
    env_var_name,
    make_placeholder,
    parse_placeholder,
)


def test_canonical_yaml_is_sorted_and_idempotent():
    a = {"b": 1, "a": {"z": 1, "y": 2}, "c": [3, 2, 1]}
    b = {"c": [3, 2, 1], "a": {"y": 2, "z": 1}, "b": 1}
    # Same data in different key order must serialize identically.
    assert canonical_yaml_dump(a) == canonical_yaml_dump(b)
    # Re-dumping a parsed dump is stable.
    text = canonical_yaml_dump(a)
    assert canonical_yaml_dump(canonical_yaml_load(text)) == text
    assert text.endswith("\n")


def test_path_tokenizer_roundtrip_and_longest_match():
    tok = PathTokenizer([("${user_data}", "/home/u"), ("${flows}", "/home/u/flows")])
    # Longest root wins: a flows path tokenizes to ${flows}, not ${user_data}.
    assert tok.to_token("/home/u/flows/a.yaml") == "${flows}/a.yaml"
    assert tok.to_token("/home/u/outputs/a.csv") == "${user_data}/outputs/a.csv"
    # Unknown roots are left verbatim.
    assert tok.to_token("/var/data/x") == "/var/data/x"
    # Round-trips back to an absolute path.
    assert tok.from_token("${flows}/a.yaml") == "/home/u/flows/a.yaml"
    assert tok.from_token("${user_data}/outputs/a.csv") == "/home/u/outputs/a.csv"


def _sample_flow_dump() -> dict:
    return {
        "flowfile_version": "0.12.0",
        "flowfile_id": 123456,
        "flowfile_name": "demo",
        "flowfile_settings": {"execution_mode": "Development"},
        "nodes": [
            {
                "id": 1,
                "type": "manual_input",
                "x_position": 100,
                "y_position": 200,
                "setting_input": {"raw": 1},
            },
            {
                "id": 2,
                "type": "output",
                "x_position": 400,
                "y_position": 250,
                "input_ids": [1],
                "setting_input": {"output_settings": {"abs_file_path": "/data/out.csv"}},
            },
        ],
        "groups": [{"id": 7, "name": "g", "x_position": 5.0, "y_position": 6.0, "width": 400.0, "height": 250.0}],
    }


def test_normalize_flow_strips_volatility_into_layout():
    tok = PathTokenizer([("${data}", "/data")])
    flow_doc, layout_doc = normalize_flow(_sample_flow_dump(), "uuid-1", tok)

    assert flow_doc["flow_uuid"] == "uuid-1"
    assert flow_doc["flowfile_id"] == NORMALIZED_FLOW_ID
    assert all(n["x_position"] == 0 and n["y_position"] == 0 for n in flow_doc["nodes"])
    # Data path tokenized for portability.
    assert flow_doc["nodes"][1]["setting_input"]["output_settings"]["abs_file_path"] == "${data}/out.csv"
    # Group geometry zeroed in the logic file, preserved in the layout.
    assert flow_doc["groups"][0]["x_position"] == 0.0
    assert layout_doc["nodes"][1] == {"x_position": 100, "y_position": 200}
    assert layout_doc["groups"][7]["x_position"] == 5.0


def test_normalize_denormalize_roundtrip_restores_state():
    tok = PathTokenizer([("${data}", "/data")])
    original = _sample_flow_dump()
    flow_doc, layout_doc = normalize_flow(copy.deepcopy(original), "uuid-1", tok)
    restored = denormalize_flow(flow_doc, layout_doc, tok, flow_id=999)

    assert restored["flowfile_id"] == 999
    assert "flow_uuid" not in restored
    assert restored["nodes"][0]["x_position"] == 100
    assert restored["nodes"][1]["setting_input"]["output_settings"]["abs_file_path"] == "/data/out.csv"
    assert restored["groups"][0]["x_position"] == 5.0


def test_normalize_is_position_diff_proof():
    """Dragging a node (position change only) must not touch the logic file."""
    tok = PathTokenizer([])
    flow_doc_a, _ = normalize_flow(_sample_flow_dump(), "uuid-1", tok)
    moved = _sample_flow_dump()
    moved["nodes"][0]["x_position"] = 9999
    moved["nodes"][0]["y_position"] = 8888
    flow_doc_b, layout_b = normalize_flow(moved, "uuid-1", tok)
    assert canonical_yaml_dump(flow_doc_a) == canonical_yaml_dump(flow_doc_b)
    assert layout_b["nodes"][1]["x_position"] == 9999


def test_secret_placeholder_helpers():
    assert make_placeholder("prod_pg") == "${secret:prod_pg}"
    assert parse_placeholder("${secret:prod_pg}") == "prod_pg"
    assert parse_placeholder("not a placeholder") is None
    assert parse_placeholder(None) is None
    assert env_var_name("prod_postgres_password") == "FLOWFILE_SECRET_PROD_POSTGRES_PASSWORD"
