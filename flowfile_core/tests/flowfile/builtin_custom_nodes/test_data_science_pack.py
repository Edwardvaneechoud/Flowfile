"""Unit tests for the Data Science pure-transform node pack.

These tests exercise each node's ``process`` method the way the runtime
does — by handing it a LazyFrame. Each node should preserve laziness end
to end; tests collect only when they need to inspect values.
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from flowfile_core.flowfile.builtin_custom_nodes.data_science import (
    KMeansLabel,
    MinMaxScale,
    OneHotEncode,
    Standardize,
    ZScoreAnomaly,
)


@pytest.fixture
def numeric_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "x": [1.0, 2.0, 3.0, 4.0, 5.0],
            "y": [10.0, 20.0, 30.0, 40.0, 50.0],
            "label": ["a", "b", "a", "b", "a"],
        }
    )


@pytest.fixture
def numeric_lf(numeric_df: pl.DataFrame) -> pl.LazyFrame:
    return numeric_df.lazy()


def test_standardize_is_lazy_and_zeroes_mean(numeric_lf: pl.LazyFrame) -> None:
    node = Standardize()
    node.settings_schema.main_section.columns.set_value(["x", "y"])

    out = node.process(numeric_lf)
    assert isinstance(out, pl.LazyFrame), "standardize must stay lazy"

    collected = out.collect()
    assert math.isclose(collected["x"].mean(), 0.0, abs_tol=1e-9)
    assert math.isclose(collected["x"].std(), 1.0, abs_tol=1e-9)
    assert math.isclose(collected["y"].mean(), 0.0, abs_tol=1e-9)
    assert collected["label"].to_list() == ["a", "b", "a", "b", "a"]


def test_standardize_no_columns_is_noop(numeric_lf: pl.LazyFrame) -> None:
    node = Standardize()
    out = node.process(numeric_lf)
    assert isinstance(out, pl.LazyFrame)
    assert out.collect().equals(numeric_lf.collect())


def test_min_max_is_lazy_and_scales_into_unit_interval(numeric_lf: pl.LazyFrame) -> None:
    node = MinMaxScale()
    node.settings_schema.main_section.columns.set_value(["x", "y"])

    out = node.process(numeric_lf)
    assert isinstance(out, pl.LazyFrame)

    collected = out.collect()
    assert collected["x"].to_list() == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert collected["y"].to_list() == [0.0, 0.25, 0.5, 0.75, 1.0]


def test_min_max_handles_constant_column() -> None:
    lf = pl.LazyFrame({"c": [5.0, 5.0, 5.0]})
    node = MinMaxScale()
    node.settings_schema.main_section.columns.set_value(["c"])

    out = node.process(lf).collect()
    assert out["c"].to_list() == [0.0, 0.0, 0.0]


def test_one_hot_is_lazy_and_expands_categorical(numeric_lf: pl.LazyFrame) -> None:
    node = OneHotEncode()
    node.settings_schema.main_section.columns.set_value(["label"])

    out = node.process(numeric_lf)
    # The main data path must stay lazy — only a tiny unique-values query
    # gets materialized internally (pivot-style).
    assert isinstance(out, pl.LazyFrame)

    collected = out.collect()
    assert "label" not in collected.columns
    assert {"label_a", "label_b"}.issubset(set(collected.columns))
    assert collected["label_a"].to_list() == [1, 0, 1, 0, 1]
    assert collected["label_b"].to_list() == [0, 1, 0, 1, 0]
    # Indicator columns must be the compact UInt8 that to_dummies would produce.
    assert collected.schema["label_a"] == pl.UInt8


def test_one_hot_drop_first(numeric_lf: pl.LazyFrame) -> None:
    node = OneHotEncode()
    node.settings_schema.main_section.columns.set_value(["label"])
    node.settings_schema.main_section.drop_first.set_value(True)

    out = node.process(numeric_lf).collect()

    # drop_first removes the alphabetically-first category column.
    assert "label_a" not in out.columns
    assert "label_b" in out.columns


def test_zscore_is_lazy_and_flags_anomaly() -> None:
    lf = pl.LazyFrame({"x": [1.0, 1.0, 1.0, 1.0, 100.0]})
    node = ZScoreAnomaly()
    node.settings_schema.main_section.columns.set_value(["x"])
    node.settings_schema.main_section.threshold.set_value(1.5)

    out = node.process(lf)
    assert isinstance(out, pl.LazyFrame)

    collected = out.collect()
    assert "x_zscore" in collected.columns
    assert "is_anomaly" in collected.columns
    assert collected["is_anomaly"].to_list() == [False, False, False, False, True]


def test_kmeans_label_is_lazy_on_the_output_path() -> None:
    # Non-feature columns (``label``) must not be materialized by the node —
    # they should still arrive as a lazy step in the returned plan.
    lf = pl.LazyFrame(
        {
            "f1": [0.0, 0.1, 0.2, 9.0, 9.1, 9.2],
            "f2": [0.0, 0.1, 0.2, 9.0, 9.1, 9.2],
            "label": ["A", "B", "A", "B", "A", "B"],
        }
    )
    node = KMeansLabel()
    node.settings_schema.main_section.feature_columns.set_value(["f1", "f2"])
    node.settings_schema.main_section.n_clusters.set_value(2.0)
    node.settings_schema.main_section.seed.set_value(0.0)

    out = node.process(lf)
    assert isinstance(out, pl.LazyFrame)

    collected = out.collect()
    assert "cluster" in collected.columns
    assert collected["label"].to_list() == ["A", "B", "A", "B", "A", "B"]
    labels = collected["cluster"].to_list()
    # The first three rows must share a cluster, distinct from the last three.
    assert labels[0] == labels[1] == labels[2]
    assert labels[3] == labels[4] == labels[5]
    assert labels[0] != labels[3]


def test_all_pack_nodes_accept_dataframe_input_too(numeric_df: pl.DataFrame) -> None:
    """Same-shape nodes must also tolerate a DataFrame (for ad-hoc callers)."""
    for node_cls, setup in (
        (Standardize, lambda n: n.settings_schema.main_section.columns.set_value(["x", "y"])),
        (MinMaxScale, lambda n: n.settings_schema.main_section.columns.set_value(["x", "y"])),
        (OneHotEncode, lambda n: n.settings_schema.main_section.columns.set_value(["label"])),
        (ZScoreAnomaly, lambda n: n.settings_schema.main_section.columns.set_value(["x"])),
        (
            KMeansLabel,
            lambda n: (
                n.settings_schema.main_section.feature_columns.set_value(["x", "y"]),
                n.settings_schema.main_section.n_clusters.set_value(2.0),
            ),
        ),
    ):
        node = node_cls()
        setup(node)
        out = node.process(numeric_df)
        assert isinstance(out, pl.LazyFrame)
        # Calling .collect() confirms the plan is runnable.
        out.collect()


def test_pack_is_discovered_by_builtin_registry() -> None:
    from flowfile_core.configs.node_store.builtin_custom_node_registry import get_all_builtin_custom_nodes

    discovered = get_all_builtin_custom_nodes()
    expected_items = {"standardize", "min-max_scale", "one-hot_encode", "z-score_anomaly", "kmeans_label"}
    assert expected_items.issubset(set(discovered.keys())), discovered.keys()


def test_pack_is_registered_in_node_store() -> None:
    from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, node_dict

    for item in ("standardize", "min-max_scale", "one-hot_encode", "z-score_anomaly", "kmeans_label"):
        assert item in CUSTOM_NODE_STORE, f"missing from CUSTOM_NODE_STORE: {item}"
        assert item in node_dict, f"missing from node_dict: {item}"
        assert node_dict[item].node_group == "data_science"
