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

    with progress.get_lock():
        assert progress.value == 100, error_message.value.decode().rstrip("\x00")

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

    msg = queue.get(timeout=1)
    assert set(msg) == {"sha256", "size_bytes", "model_type"}
    assert msg["size_bytes"] == staging.stat().st_size
    assert msg["model_type"] == "linear_regression"


def test_apply_model_task_writes_predictions(tmp_path, linear_data):
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


@pytest.fixture
def classification_data() -> pl.LazyFrame:
    # Two well-separated clusters in (x1, x2): negatives near (0, 0), positives
    # near (5, 5). Linearly separable so logistic regression hits 100% on the
    # training set, which makes the apply-side assertion an exact equality.
    x1 = [0.0, 0.5, 1.0, 0.2, 0.8, 5.0, 5.5, 6.0, 4.8, 5.2]
    x2 = [0.0, 0.3, 0.5, 0.8, 0.1, 5.0, 4.5, 5.5, 5.1, 4.9]
    y = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
    return pl.LazyFrame({"x1": x1, "x2": x2, "y": y})


@pytest.mark.parametrize("model_type", sorted(TRAINER_REGISTRY))
def test_round_trip_for_each_trainer(tmp_path, linear_data, model_type):
    """Every registered trainer must train, write, and apply without error.

    We don't assert exact coefficients here — Lasso and Ridge bias toward zero
    so their predictions won't match OLS. We just validate the contract.
    """
    if model_type in ("logistic_regression", "knn_classifier"):
        pytest.skip(
            f"{model_type} requires 0/1 target; covered by a dedicated round-trip test"
        )

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


def test_logistic_regression_round_trip(tmp_path, classification_data):
    """Logistic regression train + apply on a linearly separable fixture.

    Asserts the JSON wire format (model_type, task_type, output_dtype) and
    bit-exact predictions on the training data — separability means every row
    must be classified correctly.
    """
    progress, error_message, queue = _shared_objects()
    staging = tmp_path / "logistic.json"

    train_model_task(
        polars_serializable_object=classification_data.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",
        model_type="logistic_regression",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"add_bias": True},
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )
    with progress.get_lock():
        assert progress.value == 100, error_message.value.decode().rstrip("\x00")

    model = json.loads(staging.read_bytes())
    assert model["model_type"] == "logistic_regression"
    assert model["task_type"] == "classification"
    assert model["output_dtype"] == "Int64"
    assert model["features"] == ["x1", "x2"]
    assert len(model["coefficients"]) == 2

    msg = queue.get(timeout=1)
    assert msg["model_type"] == "logistic_regression"

    progress_a, err_a, q_a = _shared_objects()
    out_ipc = tmp_path / "scored.arrow"
    apply_model_task(
        polars_serializable_object=classification_data.drop("y").serialize(),
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
    assert df["pred"].dtype == pl.Int64
    assert df["pred"].to_list() == [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]


def test_knn_classifier_round_trip(tmp_path, classification_data):
    """KNN classifier train + apply on the linearly separable fixture.

    The model artifact carries the entire training set (KNN is non-parametric)
    and apply runs a fresh kd-tree query per call. Linearly separable input
    means every query row matches the closest cluster's label.
    """
    progress, error_message, queue = _shared_objects()
    staging = tmp_path / "knn.json"

    train_model_task(
        polars_serializable_object=classification_data.serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",
        model_type="knn_classifier",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"k": 3, "distance": "sql2"},
        staging_path=str(staging),
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )
    with progress.get_lock():
        assert progress.value == 100, error_message.value.decode().rstrip("\x00")

    model = json.loads(staging.read_bytes())
    assert model["model_type"] == "knn_classifier"
    assert model["task_type"] == "classification"
    assert model["output_dtype"] == "Int64"
    assert model["k"] == 3
    assert model["distance"] == "sql2"
    assert len(model["train_y"]) == 10

    msg = queue.get(timeout=1)
    assert msg["model_type"] == "knn_classifier"

    progress_a, err_a, q_a = _shared_objects()
    out_ipc = tmp_path / "scored.arrow"
    apply_model_task(
        polars_serializable_object=classification_data.drop("y").serialize(),
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
    assert df["pred"].dtype == pl.Int64
    assert df.height == 10
    # Linearly separable: x1<3 should predict class 0, x1>=3 class 1. KNN
    # apply runs through a group_by + join inside polars so we don't pin
    # the output to the input row order — instead assert the property
    # that matters (every point classified to its true cluster).
    assert (df["pred"] == (df["x1"] >= 3.0).cast(pl.Int64)).all()


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
