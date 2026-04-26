"""Unit tests for :mod:`shared.ml.metrics`.

These exercise the metric expressions directly without going through a flow,
so they're cheap to run and pinpoint regressions in numeric correctness.
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from shared.ml.metrics import compute_metrics


def _metrics(df: pl.DataFrame, **kwargs) -> dict[str, float]:
    out = compute_metrics(df.lazy(), **kwargs).collect()
    return dict(zip(out["metric"].to_list(), out["value"].to_list(), strict=True))


def test_perfect_predictions_yield_zero_error_and_r2_one():
    df = pl.DataFrame({"y": [1.0, 2.0, 3.0, 4.0], "p": [1.0, 2.0, 3.0, 4.0]})
    m = _metrics(df, actual_column="y", predicted_column="p", task_type="regression")
    assert m["mae"] == 0.0
    assert m["mse"] == 0.0
    assert m["rmse"] == 0.0
    assert m["mape"] == 0.0
    assert m["r2"] == pytest.approx(1.0)
    assert m["n"] == 4.0


def test_known_regression_values():
    df = pl.DataFrame({"y": [1.0, 2.0, 3.0], "p": [2.0, 2.0, 4.0]})
    # errors: 1, 0, 1 -> mae=2/3, mse=2/3, rmse=sqrt(2/3)
    # SS_tot = (1-2)^2 + (2-2)^2 + (3-2)^2 = 2
    # SS_res = 1 + 0 + 1 = 2 -> R² = 0
    m = _metrics(df, actual_column="y", predicted_column="p", task_type="regression")
    assert m["mae"] == pytest.approx(2 / 3)
    assert m["mse"] == pytest.approx(2 / 3)
    assert m["rmse"] == pytest.approx(math.sqrt(2 / 3))
    assert m["r2"] == pytest.approx(0.0)


def test_mape_skips_rows_where_actual_is_zero():
    df = pl.DataFrame({"y": [0.0, 2.0, 4.0], "p": [99.0, 2.0, 4.0]})
    m = _metrics(df, actual_column="y", predicted_column="p", task_type="regression")
    # MAPE should ignore the y=0 row entirely; the remaining rows are perfect.
    assert m["mape"] == pytest.approx(0.0)


def test_nulls_are_dropped_before_aggregation():
    df = pl.DataFrame({"y": [1.0, None, 3.0], "p": [1.0, 2.0, 3.0]})
    m = _metrics(df, actual_column="y", predicted_column="p", task_type="regression")
    assert m["n"] == 2.0
    assert m["mae"] == 0.0


def test_missing_column_raises_with_helpful_message():
    df = pl.DataFrame({"y": [1.0], "p": [1.0]})
    with pytest.raises(ValueError, match="not found"):
        compute_metrics(df.lazy(), actual_column="missing", predicted_column="p", task_type="regression")


def test_same_column_for_actual_and_predicted_is_rejected():
    df = pl.DataFrame({"y": [1.0]})
    with pytest.raises(ValueError, match="must be different"):
        compute_metrics(df.lazy(), actual_column="y", predicted_column="y", task_type="regression")


def test_unsupported_task_type_lists_supported_options():
    df = pl.DataFrame({"y": [1.0], "p": [1.0]})
    with pytest.raises(ValueError, match="regression"):
        compute_metrics(df.lazy(), actual_column="y", predicted_column="p", task_type="classification")
