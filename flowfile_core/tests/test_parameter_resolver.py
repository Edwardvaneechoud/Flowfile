"""Unit tests for the parameter resolver module."""

import pytest
from pydantic import BaseModel

from flowfile_core.flowfile.parameter_resolver import (
    apply_parameters_in_place,
    resolve_node_settings,
    resolve_parameters,
    restore_parameters,
)


# resolve_parameters


def test_simple_substitution():
    assert resolve_parameters("${name}", {"name": "world"}) == "world"


def test_multiple_params_in_one_string():
    result = resolve_parameters("${dir}/${file}", {"dir": "/tmp/data", "file": "report.csv"})
    assert result == "/tmp/data/report.csv"


def test_unresolved_param_left_as_is():
    result = resolve_parameters("${missing}", {"other": "value"})
    assert result == "${missing}"


def test_no_params_passthrough():
    text = "no placeholders here"
    assert resolve_parameters(text, {}) == text


def test_empty_string():
    assert resolve_parameters("", {"x": "1"}) == ""


def test_adjacent_params():
    result = resolve_parameters("${a}${b}", {"a": "hello", "b": "world"})
    assert result == "helloworld"


# resolve_node_settings


class SimpleSettings(BaseModel):
    path: str
    threshold: str = "0"
    description: str = ""


class NestedSettings(BaseModel):
    inner: SimpleSettings
    tag: str = ""


def test_resolve_node_settings_simple():
    settings = SimpleSettings(path="${input_dir}/data.csv", threshold="${thresh}")
    resolved = resolve_node_settings(settings, {"input_dir": "/tmp/test", "thresh": "42"})
    assert resolved.path == "/tmp/test/data.csv"
    assert resolved.threshold == "42"


def test_resolve_node_settings_returns_new_instance():
    settings = SimpleSettings(path="${dir}/file.csv")
    resolved = resolve_node_settings(settings, {"dir": "/data"})
    assert resolved is not settings
    assert settings.path == "${dir}/file.csv"


def test_resolve_node_settings_no_params_returns_same():
    settings = SimpleSettings(path="${dir}/file.csv")
    result = resolve_node_settings(settings, {})
    assert result is settings


def test_resolve_node_settings_none_input():
    result = resolve_node_settings(None, {"x": "1"})
    assert result is None


def test_resolve_node_settings_nested_model():
    settings = NestedSettings(inner=SimpleSettings(path="${base}/${file}"), tag="${env}")
    resolved = resolve_node_settings(settings, {"base": "/mnt", "file": "out.csv", "env": "prod"})
    assert resolved.inner.path == "/mnt/out.csv"
    assert resolved.tag == "prod"


def test_resolve_node_settings_raises_on_unresolved():
    settings = SimpleSettings(path="${missing_param}/data.csv")
    with pytest.raises(ValueError, match="missing_param"):
        resolve_node_settings(settings, {"other": "value"})


def test_resolve_node_settings_non_pydantic_passthrough():
    obj = {"path": "${dir}/file"}
    result = resolve_node_settings(obj, {"dir": "/data"})
    assert result is obj


# apply_parameters_in_place / restore_parameters


def test_apply_in_place_mutates_and_restores():
    settings = SimpleSettings(path="${input_dir}/data.csv", threshold="${thresh}")
    restorations = apply_parameters_in_place(settings, {"input_dir": "/tmp", "thresh": "99"})
    assert settings.path == "/tmp/data.csv"
    assert settings.threshold == "99"
    restore_parameters(restorations)
    assert settings.path == "${input_dir}/data.csv"
    assert settings.threshold == "${thresh}"


def test_apply_in_place_same_object_identity():
    """Closure captures the original object — identity must be preserved."""
    settings = SimpleSettings(path="${dir}/file.csv")
    original_id = id(settings)
    apply_parameters_in_place(settings, {"dir": "/data"})
    assert id(settings) == original_id


def test_apply_in_place_nested_model():
    settings = NestedSettings(inner=SimpleSettings(path="${base}/${file}"), tag="${env}")
    restorations = apply_parameters_in_place(settings, {"base": "/mnt", "file": "out.csv", "env": "prod"})
    assert settings.inner.path == "/mnt/out.csv"
    assert settings.tag == "prod"
    restore_parameters(restorations)
    assert settings.inner.path == "${base}/${file}"
    assert settings.tag == "${env}"


def test_apply_in_place_raises_and_rolls_back_on_unresolved():
    settings = SimpleSettings(path="${unknown}/data.csv")
    with pytest.raises(ValueError, match="unknown"):
        apply_parameters_in_place(settings, {"other": "value"})
    assert settings.path == "${unknown}/data.csv"


def test_apply_in_place_no_params_returns_empty():
    settings = SimpleSettings(path="${dir}/file.csv")
    restorations = apply_parameters_in_place(settings, {})
    assert restorations == []
    assert settings.path == "${dir}/file.csv"


# typed parameters (whole-field injection vs embedded stringification)


class TypedTargetSettings(BaseModel):
    limit: str = ""
    ratio: str = ""
    enabled: str = ""
    note: str = ""
    tags: list[str] = []
    options: dict = {}


def test_whole_field_injects_typed_int():
    settings = TypedTargetSettings(limit="${max_rows}")
    restorations = apply_parameters_in_place(settings, {"max_rows": 100})
    assert settings.limit == 100
    assert isinstance(settings.limit, int)
    restore_parameters(restorations)
    assert settings.limit == "${max_rows}"


def test_whole_field_injects_typed_float_and_bool():
    settings = TypedTargetSettings(ratio="${r}", enabled="${on}")
    restorations = apply_parameters_in_place(settings, {"r": 0.5, "on": True})
    assert settings.ratio == 0.5
    assert settings.enabled is True
    restore_parameters(restorations)
    assert settings.ratio == "${r}"
    assert settings.enabled == "${on}"


def test_embedded_ref_stringifies_bool_lowercase():
    settings = TypedTargetSettings(note="flag is ${on}, count ${n}")
    apply_parameters_in_place(settings, {"on": False, "n": 7})
    assert settings.note == "flag is false, count 7"


def test_whole_field_injection_in_list_and_dict():
    settings = TypedTargetSettings(tags=["${n}", "keep ${n}"], options={"depth": "${n}"})
    restorations = apply_parameters_in_place(settings, {"n": 3})
    assert settings.tags[0] == 3
    assert settings.tags[1] == "keep 3"
    assert settings.options["depth"] == 3
    restore_parameters(restorations)
    assert settings.tags == ["${n}", "keep ${n}"]
    assert settings.options == {"depth": "${n}"}


def test_typed_injection_restores_after_unresolved_rollback():
    settings = TypedTargetSettings(limit="${n}", note="${missing}")
    with pytest.raises(ValueError, match="missing"):
        apply_parameters_in_place(settings, {"n": 5})
    assert settings.limit == "${n}"


def test_resolve_parameters_stringifies_typed_values():
    assert resolve_parameters("v=${b}", {"b": True}) == "v=true"
    assert resolve_parameters("v=${i}", {"i": 42}) == "v=42"


# param_types coercion


def test_coerce_param_value_all_types():
    from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value

    assert coerce_param_value("integer", "42") == 42
    assert coerce_param_value("float", "1.5") == 1.5
    assert coerce_param_value("boolean", "Yes") is True
    assert coerce_param_value("boolean", "0") is False
    assert coerce_param_value("enum", "a", ["a", "b"]) == "a"
    assert coerce_param_value("string", "anything") == "anything"
    with pytest.raises(ValueError):
        coerce_param_value("integer", "x")
    with pytest.raises(ValueError):
        coerce_param_value("float", "x")
    with pytest.raises(ValueError):
        coerce_param_value("boolean", "maybe")
    with pytest.raises(ValueError):
        coerce_param_value("enum", "c", ["a", "b"])
    assert stringify_param_value(True) == "true"
    assert stringify_param_value(False) == "false"
    assert stringify_param_value(3) == "3"


def test_flow_parameter_typed_model():
    from flowfile_core.schemas.schemas import FlowParameter

    p = FlowParameter(name="n", default_value="5", type="integer")
    assert p.typed_default() == 5
    p = FlowParameter(name="b", default_value="true", type="boolean")
    assert p.typed_default() is True
    # legacy payload without type validates as string
    p = FlowParameter.model_validate({"name": "s", "default_value": "x", "description": ""})
    assert p.type == "string"
    assert p.typed_default() == "x"
    # empty default is always allowed
    assert FlowParameter(name="n", type="integer").typed_default() == ""
    with pytest.raises(ValueError):
        FlowParameter(name="e", type="enum")  # enum requires enum_values
    with pytest.raises(ValueError):
        FlowParameter(name="n", default_value="abc", type="integer")
    p = FlowParameter(name="e", default_value="a", type="enum", enum_values=["a", "b"])
    assert p.typed_default() == "a"
