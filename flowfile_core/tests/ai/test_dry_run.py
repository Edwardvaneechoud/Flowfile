"""Dry-run cache + sample-row sourcing tests.

The kernel call itself is mocked at the ``_run_kernel_for_dry_run`` seam so
these tests don't need Docker. The cache, hash determinism, and null-row
construction are pure-Python and tested directly.
"""

from __future__ import annotations

import polars as pl
import pytest

from flowfile_core.ai.tools.dry_run import (
    DEFAULT_CACHE_CAPACITY,
    DryRunCache,
    _build_null_row_from_schema,
    _hash_code,
    _hash_sample,
    dry_run_code,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

# DryRunCache


def test_cache_hit_returns_stored_schema() -> None:
    cache = DryRunCache()
    schema = [FlowfileColumn.from_input("a", "Integer")]
    cache.put("ch1", "sh1", schema)
    out = cache.get("ch1", "sh1")
    assert out is not None
    assert len(out) == 1
    assert out[0].column_name == "a"


def test_cache_miss_returns_none() -> None:
    cache = DryRunCache()
    assert cache.get("nope", "nada") is None


def test_cache_lru_eviction() -> None:
    cache = DryRunCache(capacity=3)
    for i in range(5):
        cache.put(f"c{i}", f"s{i}", [FlowfileColumn.from_input(f"col{i}", "Integer")])
    assert len(cache) == 3
    assert cache.get("c0", "s0") is None
    assert cache.get("c1", "s1") is None
    assert cache.get("c4", "s4") is not None


def test_cache_get_promotes_to_recent() -> None:
    cache = DryRunCache(capacity=2)
    cache.put("a", "1", [FlowfileColumn.from_input("a", "Integer")])
    cache.put("b", "1", [FlowfileColumn.from_input("b", "Integer")])
    cache.get("a", "1")
    cache.put("c", "1", [FlowfileColumn.from_input("c", "Integer")])
    assert cache.get("a", "1") is not None
    assert cache.get("b", "1") is None
    assert cache.get("c", "1") is not None


def test_default_cache_capacity() -> None:
    assert DEFAULT_CACHE_CAPACITY == 64


# Hash helpers


def test_hash_code_deterministic() -> None:
    h1 = _hash_code("main.select(['a'])")
    h2 = _hash_code("main.select(['a'])")
    assert h1 == h2
    assert _hash_code("main.select(['b'])") != h1


def test_hash_sample_deterministic_across_dataframe_construction() -> None:
    df1 = pl.DataFrame({"a": [None], "b": [None]}, schema={"a": pl.Int64, "b": pl.String})
    df2 = pl.DataFrame({"a": [None], "b": [None]}, schema={"a": pl.Int64, "b": pl.String})
    assert _hash_sample(df1) == _hash_sample(df2)


def test_hash_sample_distinguishes_data() -> None:
    df1 = pl.DataFrame({"a": [1]}, schema={"a": pl.Int64})
    df2 = pl.DataFrame({"a": [2]}, schema={"a": pl.Int64})
    assert _hash_sample(df1) != _hash_sample(df2)


# Null-row construction


def test_build_null_row_from_schema() -> None:
    schema = [
        FlowfileColumn.from_input("id", "Integer"),
        FlowfileColumn.from_input("label", "String"),
    ]
    lf = _build_null_row_from_schema(schema)
    df = lf.collect()
    assert df.shape == (1, 2)
    assert list(df.columns) == ["id", "label"]
    assert df["id"][0] is None
    assert df["label"][0] is None


def test_build_null_row_handles_unknown_dtype_string_fallback() -> None:
    schema = [FlowfileColumn.from_input("weird", "NotARealType")]
    lf = _build_null_row_from_schema(schema)
    df = lf.collect()
    assert df.shape == (1, 1)
    assert df.schema["weird"] == pl.String


# dry_run_code orchestration


class _FakeFlow:
    flow_id = 7

    def get_node(self, node_id):
        return None


def test_dry_run_uses_null_row_fallback_when_upstream_unrun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = [FlowfileColumn.from_input("a", "Integer")]
    captured: dict[str, object] = {}

    def fake_kernel(flow, node_id, code, output_names, sample):
        captured["sample"] = sample
        captured["code"] = code
        return [FlowfileColumn.from_input("out", "String")]

    from flowfile_core.ai.tools import dry_run as dry_run_module

    monkeypatch.setattr(dry_run_module, "_run_kernel_for_dry_run", fake_kernel)
    cache = DryRunCache()
    out = dry_run_code(
        flow=_FakeFlow(),
        node_id=99,
        upstream_node_ids=[1],
        code="main",
        output_names=["main"],
        cache=cache,
        upstream_schemas={1: schema},
    )
    assert len(out) == 1
    assert out[0].column_name == "out"
    assert captured["code"] == "main"
    assert cache.get(_hash_code("main"), _hash_sample(captured["sample"])) is not None


def test_dry_run_raises_when_no_sample_available(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = DryRunCache()
    with pytest.raises(RuntimeError, match="upstream"):
        dry_run_code(
            flow=_FakeFlow(),
            node_id=99,
            upstream_node_ids=[1],
            code="main",
            output_names=["main"],
            cache=cache,
            upstream_schemas={},
        )


def test_dry_run_requires_upstream_node_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = DryRunCache()
    with pytest.raises(RuntimeError, match="at least one upstream"):
        dry_run_code(
            flow=_FakeFlow(),
            node_id=99,
            upstream_node_ids=[],
            code="main",
            output_names=["main"],
            cache=cache,
            upstream_schemas={},
        )


def test_dry_run_lazy_litellm_contract() -> None:
    import sys

    leaked = [m for m in sys.modules if m == "litellm" or m.startswith("litellm.")]
    assert not leaked, f"litellm leaked from dry_run import: {leaked}"
