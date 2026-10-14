"""A frame's graph is local, so training and scoring a model needs no worker."""

import polars as pl
import pytest

import flowfile_frame as ff


class _WorkerRefused:
    def __init__(self, *args, **kwargs):
        raise AssertionError("a local flow called the worker")


@pytest.fixture
def no_worker(monkeypatch):
    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.MLTrainFetcher", _WorkerRefused)
    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.MLApplyFetcher", _WorkerRefused)


def test_train_wait_for_apply_runs_without_a_worker(no_worker):
    full = ff.from_dict({"x": [1.0, 2.0, 3.0, 4.0, 5.0], "y": [3.0, 5.0, 7.0, None, None]})
    labeled = full.filter(ff.col("y").is_not_null())
    unlabeled = full.filter(ff.col("y").is_null())

    model = labeled.train_model(target="y", features=["x"], model_type="linear_regression")
    scored = unlabeled.wait_for(model).apply_model(upstream=model, output_column="prediction")

    result = scored.collect()
    assert result["prediction"].to_list() == pytest.approx([9.0, 11.0])
    assert result.schema["prediction"] == pl.Float64


def test_a_classifier_scores_without_a_worker(no_worker):
    frame = ff.from_dict({"x": [0.0, 1.0, 2.0, 8.0, 9.0, 10.0], "label": [0, 0, 0, 1, 1, 1]})

    model = frame.train_model(target="label", features=["x"], model_type="logistic_regression")
    scored = frame.wait_for(model).apply_model(upstream=model, output_column="prediction")

    assert scored.collect()["prediction"].to_list() == [0, 0, 0, 1, 1, 1]
