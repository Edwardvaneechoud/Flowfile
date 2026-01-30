"""Tests for utils/utils module."""

from flowfile_core.utils.utils import (
    camel_case_to_snake_case,
    convert_to_string,
    ensure_similarity_dicts,
    standardize_col_dtype,
)


class TestCamelCaseToSnakeCase:
    """Test camel_case_to_snake_case function."""

    def test_simple(self):
        assert camel_case_to_snake_case("HelloWorld") == "hello_world"

    def test_single_word(self):
        assert camel_case_to_snake_case("Hello") == "hello"

    def test_multiple_capitals(self):
        assert camel_case_to_snake_case("OneTwo Three") == "one_two _three"

    def test_already_snake(self):
        assert camel_case_to_snake_case("already_snake") == "already_snake"

    def test_all_lowercase(self):
        assert camel_case_to_snake_case("lowercase") == "lowercase"


class TestConvertToString:
    """Test convert_to_string function."""

    def test_int(self):
        assert convert_to_string(42) == "42"

    def test_float(self):
        assert convert_to_string(3.14) == "3.14"

    def test_string(self):
        assert convert_to_string("hello") == "hello"

    def test_none(self):
        assert convert_to_string(None) == "None"

    def test_list(self):
        assert convert_to_string([1, 2]) == "[1, 2]"


class TestStandardizeColDtype:
    """Test standardize_col_dtype function."""

    def test_single_type(self):
        vals = [1, 2, 3]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_int_and_float(self):
        vals = [1, 2.0, 3]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_mixed_int_float_str(self):
        # int+float+str: since int and float are both present, it returns unchanged
        vals = [1, "a", 3.0]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_mixed_str_and_bool(self):
        # str+bool without int or float -> converts to string
        vals = ["a", True]
        result = standardize_col_dtype(vals)
        assert all(isinstance(v, str) or v is None for v in result)


class TestEnsureSimilarityDicts:
    """Test ensure_similarity_dicts function."""

    def test_same_keys(self):
        datas = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = ensure_similarity_dicts(datas)
        assert len(result) == 2
        assert result[0] == {"a": 1, "b": 2}
        assert result[1] == {"a": 3, "b": 4}

    def test_different_keys(self):
        datas = [{"a": 1}, {"b": 2}]
        result = ensure_similarity_dicts(datas)
        assert len(result) == 2
        assert result[0] == {"a": 1, "b": None}
        assert result[1] == {"a": None, "b": 2}

    def test_mixed_keys(self):
        datas = [{"a": 1, "b": 2}, {"b": 3, "c": 4}]
        result = ensure_similarity_dicts(datas)
        assert result[0] == {"a": 1, "b": 2, "c": None}
        assert result[1] == {"a": None, "b": 3, "c": 4}

    def test_respect_order_true(self):
        datas = [{"b": 1, "a": 2}, {"c": 3, "a": 4}]
        result = ensure_similarity_dicts(datas, respect_order=True)
        # b should come first, then a, then c (order of first appearance)
        keys = list(result[0].keys())
        assert keys == ["b", "a", "c"]

    def test_respect_order_false(self):
        datas = [{"b": 1, "a": 2}, {"c": 3, "a": 4}]
        result = ensure_similarity_dicts(datas, respect_order=False)
        # All keys should be present
        assert set(result[0].keys()) == {"a", "b", "c"}

    def test_empty_dicts(self):
        datas = [{}, {}]
        result = ensure_similarity_dicts(datas)
        assert result == [{}, {}]

    def test_single_dict(self):
        datas = [{"a": 1, "b": 2}]
        result = ensure_similarity_dicts(datas)
        assert result == [{"a": 1, "b": 2}]
