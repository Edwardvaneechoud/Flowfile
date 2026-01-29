"""Tests for flowfile/utils module."""

import datetime
import json
import os
import tempfile
from decimal import Decimal

from flowfile_core.flowfile.utils import (
    batch_generator,
    create_unique_id,
    generate_sha256_hash,
    create_directory_if_not_exists,
    snake_case_to_camel_case,
    json_default,
    json_dumps,
    get_hash,
)


class TestGenerateSha256Hash:
    """Test generate_sha256_hash function."""

    def test_basic_hash(self):
        result = generate_sha256_hash(b"hello")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex digest is 64 chars

    def test_deterministic(self):
        h1 = generate_sha256_hash(b"test data")
        h2 = generate_sha256_hash(b"test data")
        assert h1 == h2

    def test_different_inputs_different_hashes(self):
        h1 = generate_sha256_hash(b"data1")
        h2 = generate_sha256_hash(b"data2")
        assert h1 != h2

    def test_empty_input(self):
        result = generate_sha256_hash(b"")
        assert isinstance(result, str)
        assert len(result) == 64


class TestCreateDirectoryIfNotExists:
    """Test create_directory_if_not_exists function."""

    def test_creates_new_directory(self, tmp_path):
        new_dir = str(tmp_path / "new_directory")
        assert not os.path.exists(new_dir)
        create_directory_if_not_exists(new_dir)
        assert os.path.exists(new_dir)

    def test_existing_directory_no_error(self, tmp_path):
        existing_dir = str(tmp_path / "existing")
        os.mkdir(existing_dir)
        # Should not raise
        create_directory_if_not_exists(existing_dir)
        assert os.path.exists(existing_dir)


class TestSnakeCaseToCamelCase:
    """Test snake_case_to_camel_case function."""

    def test_simple(self):
        assert snake_case_to_camel_case("hello_world") == "HelloWorld"

    def test_single_word(self):
        assert snake_case_to_camel_case("hello") == "Hello"

    def test_multiple_underscores(self):
        assert snake_case_to_camel_case("one_two_three") == "OneTwoThree"

    def test_already_capitalized(self):
        assert snake_case_to_camel_case("Hello_World") == "HelloWorld"


class TestJsonDefault:
    """Test json_default function."""

    def test_datetime(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0, 123456)
        result = json_default(dt)
        assert "2024-01-15" in result
        assert isinstance(result, str)

    def test_date(self):
        d = datetime.date(2024, 1, 15)
        result = json_default(d)
        assert result == "2024-01-15"

    def test_time(self):
        t = datetime.time(10, 30, 0)
        result = json_default(t)
        assert "10:30" in result

    def test_decimal_integer(self):
        d = Decimal("42")
        result = json_default(d)
        assert result == 42
        assert isinstance(result, int)

    def test_decimal_float(self):
        d = Decimal("3.14")
        result = json_default(d)
        assert result == 3.14
        assert isinstance(result, float)

    def test_object_with_dict(self):
        class Obj:
            def __init__(self):
                self.x = 1
                self.y = 2

        result = json_default(Obj())
        assert result == {"x": 1, "y": 2}

    def test_non_serializable_raises(self):
        import pytest
        with pytest.raises(Exception, match="not serializable"):
            json_default(set())


class TestJsonDumps:
    """Test json_dumps function."""

    def test_simple_dict(self):
        result = json_dumps({"a": 1, "b": 2})
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == {"a": 1, "b": 2}

    def test_sorted_keys(self):
        result = json_dumps({"b": 1, "a": 2})
        assert result.index('"a"') < result.index('"b"')


class TestGetHash:
    """Test get_hash function."""

    def test_basic_string(self):
        result = get_hash("test")
        assert isinstance(result, str)
        assert len(result) == 64

    def test_deterministic(self):
        h1 = get_hash("test")
        h2 = get_hash("test")
        assert h1 == h2

    def test_different_inputs(self):
        h1 = get_hash("test1")
        h2 = get_hash("test2")
        assert h1 != h2

    def test_dict_input(self):
        result = get_hash({"key": "value"})
        assert isinstance(result, str)


class TestBatchGenerator:
    """Test batch_generator function."""

    def test_single_batch(self):
        items = list(range(5))
        batches = list(batch_generator(items, batch_size=10))
        assert len(batches) == 1
        assert batches[0] == items

    def test_multiple_batches(self):
        items = list(range(25))
        batches = list(batch_generator(items, batch_size=10))
        assert len(batches) == 3
        assert batches[0] == list(range(10))
        assert batches[1] == list(range(10, 20))
        assert batches[2] == list(range(20, 25))

    def test_exact_batch_size(self):
        items = list(range(10))
        batches = list(batch_generator(items, batch_size=10))
        assert len(batches) == 1
        assert batches[0] == items

    def test_empty_list(self):
        batches = list(batch_generator([], batch_size=10))
        assert len(batches) == 1
        assert batches[0] == []

    def test_batch_size_one(self):
        items = [1, 2, 3]
        batches = list(batch_generator(items, batch_size=1))
        assert len(batches) == 3


class TestCreateUniqueId:
    """Test create_unique_id function."""

    def test_returns_int(self):
        result = create_unique_id()
        assert isinstance(result, int)

    def test_fits_32_bits(self):
        result = create_unique_id()
        assert 0 <= result <= 0xFFFFFFFF

    def test_uniqueness(self):
        ids = {create_unique_id() for _ in range(100)}
        # Should have a high degree of uniqueness
        assert len(ids) > 90
