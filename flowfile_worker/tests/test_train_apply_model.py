"""Tests for the worker-side ML training and apply tasks.

These exercise the *task* functions directly (no HTTP, no subprocess) so they
catch breakage in the trainer dispatch, file-writing, and queue contract that
the spawner relies on.
"""

import json
from multiprocessing import Queue

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.funcs import apply_model_task, train_model_task
from shared.ml.trainers import TRAINER_REGISTRY


def _shared_objects(queue_size: int = 1):
    return (
        mp_context.Value("i", 0),
        mp_context.Array("c", 1024),
        Queue(maxsize=queue_size),
    )


@pytest.fixture
def linear_data() -> pl.LazyFrame:
    # y = 2*x1 + 3*x2 + 1, with non-collinear features so the system is
    # well-conditioned and OLS recovers the coefficients exactly.
    x1 = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    x2 = [3.0, 1.0, 4.0, 1.0, 5.0, 9.0]
    y = [2 * a + 3 * b + 1 for a, b in zip(x1, x2, strict=True)]
    return pl.LazyFrame({"x1": x1, "x2": x2, "y": y})


def test_train_model_task_writes_artifact_and_reports_metadata(tmp_path, linear_data):
    progress, error_message, queue = _shared_objects()
    staging = tmp_path / "model.json"

    train_model_task(
        polars_serializable_object=linear_data.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",  # unused for train
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"add_bias": True},
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )

    # No error.
    with progress.get_lock():
        assert progress.value == 100, error_message.value.decode().rstrip("\x00")

    # Artifact written.
    assert staging.exists()
    model = json.loads(staging.read_bytes())
    assert model["model_type"] == "linear_regression"
    assert model["features"] == ["x1", "x2"]
    assert model["target"] == "y"
    assert len(model["coefficients"]) == 2
    # Coefficients should be ~[2, 3] and intercept ~1, but allow tolerance for
    # floating-point and polars-ds solver behaviour.
    assert model["coefficients"][0] == pytest.approx(2.0, abs=1e-6)
    assert model["coefficients"][1] == pytest.approx(3.0, abs=1e-6)
    assert model["intercept"] == pytest.approx(1.0, abs=1e-6)

    # Queue contract.
    msg = queue.get(timeout=1)
    assert set(msg) == {"sha256", "size_bytes", "model_type"}
    assert msg["size_bytes"] == staging.stat().st_size
    assert msg["model_type"] == "linear_regression"


def test_apply_model_task_writes_predictions(tmp_path, linear_data):
    # First train.
    progress_t, err_t, q_t = _shared_objects()
    staging = tmp_path / "model.json"
    train_model_task(
        polars_serializable_object=linear_data.serialize(),
        progress=progress_t,
        error_message=err_t,
        queue=q_t,
        file_path="",
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"add_bias": True},
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )

    # Apply.
    new_data = pl.LazyFrame({"x1": [10.0], "x2": [20.0]})
    progress_a, err_a, q_a = _shared_objects()
    out_ipc = tmp_path / "scored.arrow"
    apply_model_task(
        polars_serializable_object=new_data.serialize(),
        progress=progress_a,
        error_message=err_a,
        queue=q_a,
        file_path=str(out_ipc),
        model_path=str(staging),
        output_column="pred",
        flowfile_flow_id=1,
        flowfile_node_id=43,
    )
    with progress_a.get_lock():
        assert progress_a.value == 100, err_a.value.decode().rstrip("\x00")

    df = pl.read_ipc(out_ipc)
    assert df.columns == ["x1", "x2", "pred"]
    # 2*10 + 3*20 + 1 = 81
    assert df["pred"][0] == pytest.approx(81.0, abs=1e-4)


def test_train_model_task_unknown_type_marks_error(tmp_path, linear_data):
    progress, error_message, queue = _shared_objects()
    train_model_task(
        polars_serializable_object=linear_data.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",
        model_type="not_a_real_model",
        target_column="y",
        feature_columns=["x1"],
        params={},
        staging_path=str(tmp_path / "missing.json"),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )
    with progress.get_lock():
        assert progress.value == -1
    err = error_message.value.decode().rstrip("\x00")
    assert "Unknown model_type" in err


@pytest.mark.parametrize("model_type", sorted(TRAINER_REGISTRY))
def test_round_trip_for_each_trainer(tmp_path, linear_data, model_type):
    """Every registered trainer must train, write, and apply without error.

    We don't assert exact coefficients here — Lasso and Ridge bias toward zero
    so their predictions won't match OLS. We just validate the contract.
    """
    progress, error, queue = _shared_objects()
    staging = tmp_path / f"{model_type}.json"

    if model_type == "ridge_regression":
        params = {"add_bias": True, "l2_reg": 0.01}
    elif model_type == "lasso_regression":
        params = {"add_bias": True, "l1_reg": 0.01, "max_iter": 200}
    else:
        params = {"add_bias": True}

    train_model_task(
        polars_serializable_object=linear_data.serialize(),
        progress=progress,
        error_message=error,
        queue=queue,
        file_path="",
        model_type=model_type,
        target_column="y",
        feature_columns=["x1", "x2"],
        params=params,
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )
    with progress.get_lock():
        assert progress.value == 100, (
            f"{model_type} training failed: {error.value.decode().rstrip(chr(0))}"
        )
    model = json.loads(staging.read_bytes())
    assert model["model_type"] == model_type
    assert len(model["coefficients"]) == 2

    new_data = pl.LazyFrame({"x1": [3.0], "x2": [30.0]})
    p2, e2, q2 = _shared_objects()
    out_ipc = tmp_path / f"scored_{model_type}.arrow"
    apply_model_task(
        polars_serializable_object=new_data.serialize(),
        progress=p2,
        error_message=e2,
        queue=q2,
        file_path=str(out_ipc),
        model_path=str(staging),
        output_column="pred",
        flowfile_flow_id=1,
        flowfile_node_id=43,
    )
    with p2.get_lock():
        assert p2.value == 100, (
            f"{model_type} apply failed: {e2.value.decode().rstrip(chr(0))}"
        )
    df = pl.read_ipc(out_ipc)
    assert "pred" in df.columns
    assert df.height == 1


def test_apply_model_task_missing_feature_marks_error(tmp_path, linear_data):
    progress_t, err_t, q_t = _shared_objects()
    staging = tmp_path / "model.json"
    train_model_task(
        polars_serializable_object=linear_data.serialize(),
        progress=progress_t,
        error_message=err_t,
        queue=q_t,
        file_path="",
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"add_bias": True},
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )

    # x2 is missing — apply must fail with a clear error and not produce output.
    bad_data = pl.LazyFrame({"x1": [1.0]})
    progress, error_message, queue = _shared_objects()
    apply_model_task(
        polars_serializable_object=bad_data.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path=str(tmp_path / "should_not_exist.arrow"),
        model_path=str(staging),
        output_column="pred",
        flowfile_flow_id=1,
        flowfile_node_id=44,
    )
    with progress.get_lock():
        assert progress.value == -1
    err = error_message.value.decode().rstrip("\x00")
    assert "x2" in err
