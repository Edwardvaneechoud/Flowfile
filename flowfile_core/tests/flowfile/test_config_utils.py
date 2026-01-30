"""Tests for configs/utils module."""

from flowfile_core.configs.utils import MutableBool


class TestMutableBool:
    """Test MutableBool dataclass."""

    def test_create_true(self):
        mb = MutableBool(value=True)
        assert mb.value is True

    def test_create_false(self):
        mb = MutableBool(value=False)
        assert mb.value is False

    def test_bool_evaluation_true(self):
        mb = MutableBool(value=True)
        assert bool(mb) is True

    def test_bool_evaluation_false(self):
        mb = MutableBool(value=False)
        assert bool(mb) is False

    def test_equality_with_bool_true(self):
        mb = MutableBool(value=True)
        assert mb == True  # noqa: E712

    def test_equality_with_bool_false(self):
        mb = MutableBool(value=False)
        assert mb == False  # noqa: E712

    def test_inequality_with_bool(self):
        mb = MutableBool(value=True)
        assert not (mb == False)  # noqa: E712

    def test_equality_with_mutable_bool(self):
        mb1 = MutableBool(value=True)
        mb2 = MutableBool(value=True)
        assert mb1 == mb2

    def test_inequality_with_mutable_bool(self):
        mb1 = MutableBool(value=True)
        mb2 = MutableBool(value=False)
        assert not (mb1 == mb2)

    def test_equality_with_other_type(self):
        mb = MutableBool(value=True)
        result = mb.__eq__("not_a_bool")
        assert result is NotImplemented

    def test_set_value_true(self):
        mb = MutableBool(value=False)
        result = mb.set(True)
        assert mb.value is True
        assert result is mb  # Returns self

    def test_set_value_false(self):
        mb = MutableBool(value=True)
        result = mb.set(False)
        assert mb.value is False
        assert result is mb

    def test_set_value_truthy(self):
        mb = MutableBool(value=False)
        mb.set(1)
        assert mb.value is True

    def test_set_value_falsy(self):
        mb = MutableBool(value=True)
        mb.set(0)
        assert mb.value is False

    def test_if_statement(self):
        mb_true = MutableBool(value=True)
        mb_false = MutableBool(value=False)
        assert mb_true
        assert not mb_false

    def test_mutability(self):
        mb = MutableBool(value=True)
        mb.value = False
        assert mb.value is False
        assert bool(mb) is False
