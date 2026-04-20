"""Unit tests for the Data Science pure-transform node pack.

These tests exercise each node's ``process`` method directly with realistic
DataFrames — no flow graph, no worker, no Docker. They also verify that the
pack is discovered by the builtin loader and registered in the node store.
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


def test_standardize_zeroes_mean_and_unit_std(numeric_df: pl.DataFrame) -> None:
    node = Standardize()
    node.settings_schema.main_section.columns.set_value(["x", "y"])

    out = node.process(numeric_df)

    assert math.isclose(out["x"].mean(), 0.0, abs_tol=1e-9)
    assert math.isclose(out["x"].std(), 1.0, abs_tol=1e-9)
    assert math.isclose(out["y"].mean(), 0.0, abs_tol=1e-9)
    assert out["label"].to_list() == ["a", "b", "a", "b", "a"]


def test_standardize_no_columns_is_noop(numeric_df: pl.DataFrame) -> None:
    node = Standardize()
    out = node.process(numeric_df)
    assert out.equals(numeric_df)


def test_min_max_scales_into_unit_interval(numeric_df: pl.DataFrame) -> None:
    node = MinMaxScale()
    node.settings_schema.main_section.columns.set_value(["x", "y"])

    out = node.process(numeric_df)

    assert out["x"].to_list() == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert out["y"].to_list() == [0.0, 0.25, 0.5, 0.75, 1.0]


def test_min_max_handles_constant_column() -> None:
    df = pl.DataFrame({"c": [5.0, 5.0, 5.0]})
    node = MinMaxScale()
    node.settings_schema.main_section.columns.set_value(["c"])

    out = node.process(df)

    assert out["c"].to_list() == [0.0, 0.0, 0.0]


def test_one_hot_expands_categorical(numeric_df: pl.DataFrame) -> None:
    node = OneHotEncode()
    node.settings_schema.main_section.columns.set_value(["label"])

    out = node.process(numeric_df)

    assert "label" not in out.columns
    assert {"label_a", "label_b"}.issubset(set(out.columns))
    assert out["label_a"].to_list() == [1, 0, 1, 0, 1]


def test_one_hot_drop_first(numeric_df: pl.DataFrame) -> None:
    node = OneHotEncode()
    node.settings_schema.main_section.columns.set_value(["label"])
    node.settings_schema.main_section.drop_first.set_value(True)

    out = node.process(numeric_df)

    # drop_first removes the alphabetically-first category column.
    assert "label_a" not in out.columns
    assert "label_b" in out.columns


def test_zscore_appends_score_and_flags_anomaly() -> None:
    df = pl.DataFrame({"x": [1.0, 1.0, 1.0, 1.0, 100.0]})
    node = ZScoreAnomaly()
    node.settings_schema.main_section.columns.set_value(["x"])
    node.settings_schema.main_section.threshold.set_value(1.5)

    out = node.process(df)

    assert "x_zscore" in out.columns
    assert "is_anomaly" in out.columns
    # The 100.0 outlier should be flagged; the others not.
    assert out["is_anomaly"].to_list() == [False, False, False, False, True]


def test_kmeans_label_appends_cluster_column() -> None:
    df = pl.DataFrame(
        {
            "f1": [0.0, 0.1, 0.2, 9.0, 9.1, 9.2],
            "f2": [0.0, 0.1, 0.2, 9.0, 9.1, 9.2],
        }
    )
    node = KMeansLabel()
    node.settings_schema.main_section.feature_columns.set_value(["f1", "f2"])
    node.settings_schema.main_section.n_clusters.set_value(2.0)
    node.settings_schema.main_section.seed.set_value(0.0)

    out = node.process(df)

    assert "cluster" in out.columns
    labels = out["cluster"].to_list()
    # The first three rows must share a cluster, distinct from the last three.
    assert labels[0] == labels[1] == labels[2]
    assert labels[3] == labels[4] == labels[5]
    assert labels[0] != labels[3]


def test_all_pack_nodes_accept_lazyframe_input(numeric_df: pl.DataFrame) -> None:
    """The framework's local-exec path passes a LazyFrame (fde.data_frame) to
    each node's ``process``. Every pack node must handle that without raising.
    This guards against bugs like ``LazyFrame.to_dummies`` not existing.
    """
    lf = numeric_df.lazy()

    std = Standardize()
    std.settings_schema.main_section.columns.set_value(["x", "y"])
    std.process(lf)

    mm = MinMaxScale()
    mm.settings_schema.main_section.columns.set_value(["x", "y"])
    mm.process(lf)

    oh = OneHotEncode()
    oh.settings_schema.main_section.columns.set_value(["label"])
    oh_out = oh.process(lf)
    assert "label_a" in oh_out.columns

    zs = ZScoreAnomaly()
    zs.settings_schema.main_section.columns.set_value(["x"])
    zs.settings_schema.main_section.threshold.set_value(3.0)
    zs.process(lf)

    km = KMeansLabel()
    km.settings_schema.main_section.feature_columns.set_value(["x", "y"])
    km.settings_schema.main_section.n_clusters.set_value(2.0)
    km_out = km.process(lf)
    assert "cluster" in km_out.columns


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
