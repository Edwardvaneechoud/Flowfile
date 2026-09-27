"""Which ``join`` calls place one native join node: ``keep_right_keys``, ``coalesce`` and ``suffix`` on inner/left joins."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff

from .native_helpers import core_node

LEFT = {"id": [1, 2, 3], "age": [25, 30, 35], "city": ["x", "y", "z"]}
RIGHT = {"id": [1, 2, 4], "city": ["NYC", "LA", "B"], "pop": [1, 2, 3]}


def _join(how: str, right: dict = RIGHT, **kwargs) -> ff.FlowFrame:
    return ff.from_dict(LEFT).join(ff.from_dict(right), on="id", how=how, **kwargs)


def _polars(how: str, right: dict = RIGHT, **kwargs) -> pl.DataFrame:
    return pl.LazyFrame(LEFT).join(pl.LazyFrame(right), on="id", how=how, **kwargs).collect()


def _right_select(frame: ff.FlowFrame) -> list[tuple[str, str, bool]]:
    renames = core_node(frame).setting_input.join_input.right_select.renames
    return [(c.old_name, c.new_name, c.keep) for c in renames]


@pytest.mark.parametrize("how", ["inner", "left"])
def test_keep_right_keys_is_one_native_join_with_the_keys_after_the_other_right_columns(how):
    out = _join(how, keep_right_keys=True)
    assert core_node(out).node_type == "join"
    assert _right_select(out) == [("id", "id", True), ("city", "city", True), ("pop", "pop", True)]
    result = out.collect()
    assert result.columns == ["id", "age", "city", "city_right", "pop", "id_right"]
    expected = _polars(how, coalesce=False)
    assert_frame_equal(result.select(expected.columns).sort("id"), expected.sort("id"))


@pytest.mark.parametrize("how", ["inner", "left"])
def test_a_suffix_renames_the_clashing_right_columns_natively(how):
    out = _join(how, suffix="_r", keep_right_keys=True)
    assert core_node(out).node_type == "join"
    assert _right_select(out) == [("id", "id_r", True), ("city", "city_r", True), ("pop", "pop", True)]
    expected = _polars(how, suffix="_r", coalesce=False)
    assert_frame_equal(out.collect().select(expected.columns).sort("id"), expected.sort("id"))


def test_coalesce_true_is_the_native_default():
    out = _join("inner", coalesce=True)
    assert core_node(out).node_type == "join"
    assert_frame_equal(out.collect().sort("id"), _polars("inner", coalesce=True).sort("id"))


def test_coalesce_false_is_native_only_when_the_right_keys_already_trail():
    trailing = {"city": RIGHT["city"], "pop": RIGHT["pop"], "id": RIGHT["id"]}
    native = _join("left", right=trailing, coalesce=False)
    assert core_node(native).node_type == "join"
    assert_frame_equal(native.collect().sort("id"), _polars("left", right=trailing, coalesce=False).sort("id"))
    code = _join("left", coalesce=False)
    assert core_node(code).node_type == "polars_code"
    assert_frame_equal(code.collect().sort("id"), _polars("left", coalesce=False).sort("id"))


def test_a_suffix_that_still_clashes_is_left_to_polars_which_refuses_it():
    left = ff.from_dict({"id": [1, 2], "city": ["x", "y"], "city_r": ["y", "z"]})
    with pytest.raises(pl.exceptions.DuplicateError, match="city_r"):
        left.join(ff.from_dict(RIGHT), on="id", suffix="_r")


def test_keep_right_keys_on_a_full_join_stays_polars_code_with_coalesce_false():
    out = _join("full", keep_right_keys=True)
    assert core_node(out).node_type == "polars_code"
    assert "coalesce=False" in core_node(out).setting_input.polars_code_input.polars_code


def test_keep_right_keys_with_different_key_names():
    right = ff.from_dict({"key": [1, 2], "v": ["a", "b"]})
    out = ff.from_dict(LEFT).join(right, left_on="id", right_on="key", keep_right_keys=True)
    assert core_node(out).node_type == "join"
    assert out.collect().sort("id").to_dict(as_series=False)["key"] == [1, 2]


@pytest.mark.parametrize("kwargs", [{"how": "semi"}, {"how": "inner", "coalesce": True}])
def test_keep_right_keys_refuses_a_contradiction(kwargs):
    with pytest.raises(ValueError, match="keep_right_keys"):
        ff.from_dict(LEFT).join(ff.from_dict(RIGHT), on="id", keep_right_keys=True, **kwargs)
