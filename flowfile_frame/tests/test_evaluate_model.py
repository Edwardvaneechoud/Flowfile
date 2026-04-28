import os

os.environ["TESTING"] = "True"

import polars as pl
import pytest

from flowfile_core.schemas.input_schema import NodeEvaluateModel
from flowfile_frame.flow_frame import FlowFrame


@pytest.fixture
def regression_df():
    return FlowFrame(
        {
            "y": [1.0, 2.0, 3.0, 4.0, 5.0],
            "prediction": [1.1, 1.9, 3.0, 4.2, 5.1],
        }
    )


@pytest.fixture
def classification_df():
    return FlowFrame(
        {
            "y": [0, 1, 1, 0, 1],
            "prediction": [0, 1, 1, 1, 1],
        }
    )


def test_evaluate_model_emits_metric_value_long_form(regression_df):
    result = regression_df.evaluate_model("y").collect()
    assert result.columns == ["metric", "value"]
    assert result.schema["metric"] == pl.String
    assert result.schema["value"] == pl.Float64
    metric_set = set(result["metric"].to_list())
    for metric in ("mae", "rmse", "r2", "n"):
        assert metric in metric_set, f"Expected metric '{metric}' in {metric_set}"


def test_evaluate_model_classification_task(classification_df):
    result = classification_df.evaluate_model("y", task_type="classification").collect()
    metric_set = set(result["metric"].to_list())
    for metric in ("accuracy", "precision", "recall", "f1"):
        assert metric in metric_set, f"Expected metric '{metric}' in {metric_set}"


def test_evaluate_model_validates_actual_column_in_columns(regression_df):
    with pytest.raises(ValueError, match="not in input columns"):
        regression_df.evaluate_model("does_not_exist")


def test_evaluate_model_validates_predicted_column_in_columns(regression_df):
    with pytest.raises(ValueError, match="not in input columns"):
        regression_df.evaluate_model("y", predicted_column="missing")


def test_evaluate_model_rejects_cross_flow_upstream(regression_df):
    other = FlowFrame({"y": [1.0, 2.0], "prediction": [1.0, 2.0]})
    with pytest.raises(ValueError, match="same flow"):
        regression_df.evaluate_model("y", upstream=other)


def test_evaluate_model_with_upstream_wires_node_id(regression_df):
    # Pass the same frame as upstream — _resolve_task_type will fall back to
    # "regression" because the node isn't a train_model, but the wiring of
    # upstream_train_node_id should still go through.
    result = regression_df.evaluate_model("y", upstream=regression_df)
    eval_input = result.get_node_settings().setting_input.evaluate_input
    assert eval_input.upstream_train_node_id == regression_df.node_id


def test_evaluate_model_node_type_and_settings(regression_df):
    result = regression_df.evaluate_model("y", predicted_column="prediction")
    settings_node = result.get_node_settings()
    assert settings_node.node_type == "evaluate_model"
    assert isinstance(settings_node.setting_input, NodeEvaluateModel)
    eval_input = settings_node.setting_input.evaluate_input
    assert eval_input.actual_column == "y"
    assert eval_input.predicted_column == "prediction"
    assert eval_input.task_type == "auto"
    assert eval_input.upstream_train_node_id is None


def test_evaluate_model_default_description_is_built(regression_df):
    result = regression_df.evaluate_model("y")
    assert result.get_node_settings().setting_input.description == "Evaluate prediction vs y"


def test_evaluate_model_custom_description_passes_through(regression_df):
    result = regression_df.evaluate_model("y", description="custom desc")
    assert result.get_node_settings().setting_input.description == "custom desc"
