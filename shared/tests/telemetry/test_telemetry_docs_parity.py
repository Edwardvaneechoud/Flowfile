"""Docs/schema parity for ``docs/users/telemetry.md``.

The page promises "every event, every field, every value that can leave your
machine", which makes its tables part of the closed-schema contract rather than
commentary. These tests parse the page and assert it describes exactly the
schema in :mod:`shared.telemetry`, so a schema change that forgets the docs (or
a docs value the client would silently drop) fails CI.

Firing *semantics* — which user action produces which event — are prose and
cannot be machine-checked here; those stay pinned by the route-mapping tests in
``flowfile_core/tests/test_telemetry_events.py``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from shared import telemetry

DOCS = Path(__file__).resolve().parents[3] / "docs" / "users" / "telemetry.md"

EVENT_HEADER = ("Event", "When it fires")
PROP_HEADER = ("Prop", "Event", "Allowed values")

CLOSED_SET = re.compile(r"^`[^`]+`(?:\s*·\s*`[^`]+`)*$")

NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}


def _text() -> str:
    return DOCS.read_text(encoding="utf-8")


def _cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _rows(header: tuple[str, ...]) -> list[list[str]]:
    """Body cells of the one markdown table whose header row is *header*."""
    lines = _text().splitlines()
    for index, line in enumerate(lines):
        if line.startswith("|") and tuple(_cells(line)) == header:
            body = [_cells(row) for row in _takewhile_table(lines[index + 2 :])]
            assert body, f"the {header} table has no rows"
            return body
    raise AssertionError(f"no table with header {header} in {DOCS}")


def _takewhile_table(lines: list[str]) -> list[str]:
    kept: list[str] = []
    for line in lines:
        if not line.startswith("|"):
            break
        kept.append(line)
    return kept


def _code(cell: str) -> str:
    assert cell.startswith("`") and cell.endswith("`"), f"expected a code-formatted cell, got {cell!r}"
    return cell.strip("`")


def _values(cell: str) -> set[str] | None:
    """The cell's closed value set, or ``None`` when it describes an open set in prose."""
    if not CLOSED_SET.match(cell):
        return None
    return {token.strip().strip("`") for token in cell.split("·")}


def _number(pattern: str) -> int:
    match = re.search(pattern, _text())
    assert match is not None, f"the page no longer says {pattern!r}"
    word = match.group(1)
    return int(word) if word.isdigit() else NUMBER_WORDS[word]


@pytest.fixture(scope="module")
def prop_rows() -> dict[str, list[str]]:
    return {_code(row[0]): row for row in _rows(PROP_HEADER)}


def test_documented_event_names_match_the_client_schema() -> None:
    documented = {_code(row[0]) for row in _rows(EVENT_HEADER)}
    assert documented == set(telemetry.EVENTS)


def test_documented_props_match_the_client_schema(prop_rows) -> None:
    documented = {(_code(row[0]), _code(row[1])) for row in prop_rows.values()}
    expected = {(prop, event) for event, props in telemetry.EVENTS.items() for prop in props}
    assert documented == expected


def test_documented_closed_value_sets_match_the_client_schema(prop_rows) -> None:
    assert _values(prop_rows["node_count_bucket"][2]) == set(telemetry.NODE_COUNT_BUCKETS)
    assert _values(prop_rows["duration_bucket"][2]) == set(telemetry.DURATION_BUCKETS)
    assert _values(prop_rows["used_sample_data"][2]) == {"true", "false"}


def test_documented_export_targets_are_all_accepted_by_the_client(prop_rows) -> None:
    """A documented target the client would drop is a promise the code cannot keep."""
    documented = _values(prop_rows["target"][2])
    assert documented is not None, "the target row must stay a closed list of code-formatted values"
    assert documented <= set(telemetry.EXPORT_TARGETS)


def test_open_value_sets_stay_prose(prop_rows) -> None:
    """``node_types`` and ``error_class`` are allowlists too long to enumerate."""
    assert _values(prop_rows["node_types"][2]) is None
    assert _values(prop_rows["error_class"][2]) is None


def test_documented_node_type_cap_matches_the_client() -> None:
    assert _number(r"capped at (\d+) entries") == telemetry.MAX_NODE_TYPES


def test_documented_event_and_prop_counts_match_the_client_schema() -> None:
    assert _number(r"The schema is closed — (\w+) events") == len(telemetry.EVENTS)
    with_props = [event for event, props in telemetry.EVENTS.items() if props]
    assert _number(r"Only (\w+) events carry props at all") == len(with_props)


def test_documented_example_event_matches_a_client_built_envelope() -> None:
    match = re.search(r"```json\n(.*?)\n```", _text(), re.DOTALL)
    assert match is not None, "the page must keep its canonical example event"
    example = json.loads(match.group(1))
    built = telemetry._envelope(example["event"], example["props"], example["install_id"])
    assert set(example) == set(built)
    assert set(example["props"]) == telemetry.EVENTS[example["event"]]


def test_page_ships_no_unfilled_placeholders() -> None:
    """The page is in the published nav, so a placeholder renders to a reader deciding on consent."""
    assert "TODO" not in _text(), "docs/users/telemetry.md still contains a TODO placeholder"
