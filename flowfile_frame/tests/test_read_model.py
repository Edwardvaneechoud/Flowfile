"""``FlowFrame.read_model``: the model a Train Model node fitted, as a ``TrainedModel``."""

import pytest

import flowfile_frame as ff
from flowfile_frame.native import NativeNodeError
from flowfile_frame.trained_model import TrainedModel

LINE = {"x": [1.0, 2.0, 3.0, 4.0], "y": [3.0, 5.0, 7.0, 9.0]}
CLASSES = {"x": [0.0, 1.0, 2.0, 8.0, 9.0, 10.0], "label": [0, 0, 0, 1, 1, 1]}


def test_a_linear_model_reads_back_its_fit():
    model = ff.from_dict(LINE).train_model(target="y", features=["x"]).read_model()

    assert (model.model_type, model.task_type, model.target, model.features) == (
        "linear_regression",
        "regression",
        "y",
        ["x"],
    )
    assert model.params == {"add_bias": True}
    assert model.coefficients == {"x": pytest.approx(2.0)}
    assert model.intercept == pytest.approx(1.0)
    assert "Coefficients: x=2" in repr(model)


def test_to_dict_is_the_stored_model_and_a_copy():
    model = ff.from_dict(LINE).train_model(target="y", features=["x"]).read_model()

    stored = model.to_dict()
    stored["features"].append("z")

    assert set(stored) == {"model_type", "task_type", "target", "features", "params", "output_dtype"} | {
        "coefficients",
        "intercept",
    }
    assert model.features == ["x"]
    assert stored["coefficients"] == list(model.coefficients.values())
    assert isinstance(model, TrainedModel)


def test_a_knn_model_has_no_coefficients_and_prints_without_its_training_rows():
    model = ff.from_dict(CLASSES).train_model(target="label", features=["x"], model_type="knn_classifier").read_model()

    assert model.coefficients is None and model.intercept is None
    assert {"k", "distance", "train_X", "train_y"} <= set(model.fitted)
    assert "train_X" not in repr(model) and "k: " in repr(model)


def test_read_model_on_another_frame_says_the_flow_has_no_model_yet():
    with pytest.raises(
        NativeNodeError, match="read_model reads the frame train_model returns.*no Train Model node yet"
    ):
        ff.from_dict(LINE).read_model()


def test_read_model_on_another_frame_names_the_flows_train_model_nodes():
    source = ff.from_dict(LINE)
    trained = source.train_model(target="y", features=["x"])

    with pytest.raises(NativeNodeError, match=rf"node {trained.node_id} \(linear_regression on 'y'\)"):
        source.filter(ff.col("x") > 1).read_model()


def test_a_train_model_frame_shows_its_model_when_printed():
    trained = ff.from_dict(LINE).train_model(target="y", features=["x"])

    display = repr(trained)

    assert "  Model: TrainedModel (linear_regression, regression)\n    Target: y\n" in display
    assert "Coefficients: x=2" in display


def test_a_train_model_frame_below_a_gate_prints_without_running():
    source = ff.from_dict(LINE)
    env = ff.Parameter("env", default="prod")
    ff.add_flow_parameter(source, env)
    trained = ff.Gate(source, parameter=env, value="prod").then.train_model(target="y", features=["x"])

    assert repr(trained).endswith("Model: fitted when the flow runs; read_model() runs it")


def test_a_model_below_a_gate_is_fitted_when_read_and_refused_once_gated_away():
    source = ff.from_dict(LINE)
    env = ff.Parameter("env", default="prod")
    ff.add_flow_parameter(source, env)
    trained = ff.Gate(source, parameter=env, value="prod").then.train_model(target="y", features=["x"])

    assert trained.read_model().coefficients == {"x": pytest.approx(2.0)}

    ff.set_flow_parameter(source, env, "dev")
    with pytest.raises(NativeNodeError, match="did not run"):
        trained.read_model()
