"""Unit tests for the parameter resolver module."""

import pytest
from pydantic import BaseModel

from flowfile_core.flowfile.parameter_resolver import (
    apply_parameters_in_place,
    resolve_node_settings,
    resolve_parameters,
    restore_parameters,
)


# ---------------------------------------------------------------------------
# resolve_parameters
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# resolve_node_settings
# ---------------------------------------------------------------------------


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
    assert settings.path == "${dir}/file.csv"  # original unchanged


def test_resolve_node_settings_no_params_returns_same():
    settings = SimpleSettings(path="${dir}/file.csv")
    result = resolve_node_settings(settings, {})
    # Empty dict — no substitution, returns original unchanged
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
    # Non-pydantic objects are returned unchanged
    assert result is obj


# ---------------------------------------------------------------------------
# apply_parameters_in_place / restore_parameters
# ---------------------------------------------------------------------------


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
    # Original value must be restored after failed substitution
    assert settings.path == "${unknown}/data.csv"


def test_apply_in_place_no_params_returns_empty():
    settings = SimpleSettings(path="${dir}/file.csv")
    restorations = apply_parameters_in_place(settings, {})
    assert restorations == []
    assert settings.path == "${dir}/file.csv"  # unchanged
