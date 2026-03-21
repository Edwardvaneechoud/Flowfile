"""Unit tests for the parameter resolver module."""

import pytest
from pydantic import BaseModel

from flowfile_core.flowfile.parameter_resolver import (
    resolve_node_settings,
    resolve_parameters,
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
