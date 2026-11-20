"""Tests for ``ai.json_payload.parse_json_payload``, the tolerant parser the JSON-mode surfaces share."""

from __future__ import annotations

import pytest

from flowfile_core.ai.json_payload import parse_json_payload

FENCED = '```json\n{"a": 1}\n```'


@pytest.mark.parametrize("content", [None, "", "  \n"])
def test_nothing_is_an_empty_response(content: str | None) -> None:
    assert parse_json_payload(content) == (None, "empty_response")


def test_strict_json_is_taken_as_written() -> None:
    noted: list[str] = []
    assert parse_json_payload(' {"a": 1} ', extract_object=True, on_extracted=lambda: noted.append("x")) == (
        {"a": 1},
        None,
    )
    assert noted == []


def test_a_fence_around_the_answer_is_stripped() -> None:
    noted: list[str] = []
    assert parse_json_payload(FENCED, on_extracted=lambda: noted.append("x")) == ({"a": 1}, None)
    assert noted == ["x"]


def test_prose_around_an_object_needs_extract_object() -> None:
    content = 'Here it is: {"a": 1}. Done.'
    assert parse_json_payload(content) == (None, "parse_error")
    noted: list[str] = []
    assert parse_json_payload(content, extract_object=True, on_extracted=lambda: noted.append("x")) == (
        {"a": 1},
        None,
    )
    assert noted == ["x"]


@pytest.mark.parametrize("depth", [201, 100_000])
def test_an_answer_nested_too_deeply_is_a_parse_error(depth: int) -> None:
    payload, error = parse_json_payload("[" * depth + "]" * depth)
    assert payload is None
    assert error == "parse_error"


def test_an_answer_nested_up_to_the_limit_parses() -> None:
    payload, error = parse_json_payload('{"a": ' * 199 + "[1]" + "}" * 199)
    assert error is None
    assert payload is not None


def test_what_cannot_be_parsed_is_a_parse_error() -> None:
    assert parse_json_payload("not json", extract_object=True) == (None, "parse_error")
