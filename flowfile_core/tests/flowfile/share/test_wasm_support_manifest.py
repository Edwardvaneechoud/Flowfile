"""The shipped WASM support manifest must partition core's node registry."""

import json

import pytest

from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.flowfile.share import support


@pytest.fixture()
def manifest() -> dict:
    loaded = support.load_manifest()
    assert loaded is not None, "the manifest must ship — run 'make wasm_node_manifest'"
    return loaded


def test_every_core_node_type_has_exactly_one_tier(manifest):
    _, node_dict, _ = get_all_standard_nodes()
    assert set(manifest["nodes"]) == set(node_dict), (
        "the manifest and flowfile_core's node registry disagree. Run 'make wasm_node_manifest'."
    )
    for node_type in node_dict:
        assert support.tier_for(node_type) in set(support.SupportTier)


def test_tier_counts_match_the_browser_palette(manifest):
    counts = {tier: 0 for tier in ("supported", "locked", "absent")}
    for entry in manifest["nodes"].values():
        counts[entry["tier"]] += 1
    assert counts == {"supported": 23, "locked": 16, "absent": 8}
    assert manifest["counts"] == counts


def test_dialect_covers_the_renamed_types(manifest):
    assert manifest["dialect"] == {
        "sample": "head",
        "flow_input": "external_data",
        "flow_output": "external_output",
        "catalog_reader": "read_from_catalog",
        "catalog_writer": "write_to_catalog",
    }
    for core_type, wasm_type in manifest["dialect"].items():
        assert support.wasm_type_for(core_type) == wasm_type


def test_locked_nodes_carry_a_docs_anchor(manifest):
    locked = [name for name, entry in manifest["nodes"].items() if entry["tier"] == "locked"]
    assert locked
    for node_type in locked:
        assert support.docs_anchor_for(node_type), f"{node_type} has no docs anchor for the locked-node link"


def test_pivot_aggregations_are_a_strict_subset_of_group_by():
    group_by, pivot = support.group_by_aggs(), support.pivot_aggs()
    assert pivot < group_by
    assert {"n_unique", "concat"} <= group_by
    assert not {"n_unique", "concat"} & pivot


def test_file_types_are_what_the_browser_reads_and_writes():
    assert support.read_file_types() == frozenset({"csv", "excel", "parquet"})
    assert support.output_file_types() == frozenset({"csv", "excel", "parquet"})


def test_unknown_node_type_is_absent():
    assert support.tier_for("a_node_that_does_not_exist") is support.SupportTier.ABSENT
    assert support.wasm_type_for("a_node_that_does_not_exist") is None


def test_missing_manifest_fails_closed(tmp_path, monkeypatch):
    """No baseline must mean "nothing is supported", never "everything is"."""
    monkeypatch.setattr(support, "_MANIFEST_PATH", tmp_path / "absent.json")
    support.load_manifest.cache_clear()
    try:
        assert support.load_manifest() is None
        _, node_dict, _ = get_all_standard_nodes()
        for node_type in node_dict:
            assert support.tier_for(node_type) is support.SupportTier.ABSENT
        assert support.group_by_aggs() == frozenset()
        assert support.pivot_aggs() == frozenset()
        assert support.read_file_types() == frozenset()
    finally:
        support.load_manifest.cache_clear()


def test_malformed_manifest_fails_closed(tmp_path, monkeypatch):
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps({"nodes": "not a mapping"}), encoding="utf-8")
    monkeypatch.setattr(support, "_MANIFEST_PATH", broken)
    support.load_manifest.cache_clear()
    try:
        assert support.load_manifest() is None
        assert support.tier_for("filter") is support.SupportTier.ABSENT
    finally:
        support.load_manifest.cache_clear()
