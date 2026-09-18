"""The independence predicate that decides whether formula entries can share one step."""

import os

import pytest

from flowfile_core.flowfile.formula_dependencies import entries_are_independent


def test_independent_entries_read_only_upstream_columns():
    assert entries_are_independent(
        [
            ("FullName", '[First] + " " + [Last]'),
            ("Upper", "uppercase([Email])"),
            ("Constant", "1"),
        ]
    )


def test_forward_reference_is_not_independent():
    assert not entries_are_independent([("A", "[B] + 1"), ("B", "[Value] * 2")])


def test_backward_reference_is_not_independent():
    assert not entries_are_independent([("A", "[Value] * 2"), ("B", "[A] + 1")])


def test_self_reference_is_independent():
    assert entries_are_independent([("Value", "[Value] * 10"), ("Other", "[Score] + 1")])


def test_duplicate_output_names_are_not_independent():
    assert not entries_are_independent([("Tag", '"first"'), ("Tag", '"second"')])


def test_unparseable_expression_is_not_independent():
    assert not entries_are_independent([("A", "[Value] +* "), ("B", "1")])


def test_string_literal_is_not_a_column_read():
    assert entries_are_independent([("a", '"[b]"'), ("b", "1")])


def test_blank_expressions_contribute_nothing():
    assert entries_are_independent([("A", "   "), ("B", "[Value] + 1")])
    # A blank entry may even share its name with a live one: it writes nothing.
    assert entries_are_independent([("B", ""), ("B", "[Value] + 1")])


def test_fewer_than_two_active_entries_is_trivially_independent():
    assert entries_are_independent([])
    assert entries_are_independent([("A", "[Nonsense +*")])


if __name__ == "__main__":
    pytest.main([os.path.abspath(__file__)])
