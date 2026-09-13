"""Drift guard: the scope registry must stay equal to the corpus taxonomy it was transcribed from.

`scope.py` carries Python literals because the taxonomy lives in a private learning folder the
public repo never ships. These tests only run where that folder is present; everywhere else the
registry is simply trusted, which is the point of transcribing it.
"""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from flowfile_core.flowfile.converters.alteryx.scope import (
    BUCKETS,
    NO_OP_TOOLS,
    OUT_OF_SCOPE,
    census_tool_name,
    classify,
)
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import AlteryxTool

LEARNING = Path(__file__).resolve().parents[5]
SCOPE_JSON = LEARNING / "tools" / "alteryx_scope.json"
CENSUS_JSON = LEARNING / "tools" / "census_out" / "census.json"

pytestmark = pytest.mark.skipif(
    not (SCOPE_JSON.exists() and CENSUS_JSON.exists()),
    reason=f"corpus taxonomy or census not present ({SCOPE_JSON}, {CENSUS_JSON})",
)

JSON_NO_OP = "noop"
NO_OP_BUCKET = "no_op"


def _taxonomy() -> dict[str, str]:
    """The JSON's `out_of_scope:<bucket>` / `noop` labels, normalised to this registry's bucket names."""
    raw = json.loads(SCOPE_JSON.read_text())
    return {
        name: (NO_OP_BUCKET if label == JSON_NO_OP else label.split(":", 1)[1]) for name, label in raw.items()
    }


def test_out_of_scope_table_equals_the_taxonomy():
    expected = {name: bucket for name, bucket in _taxonomy().items() if bucket != NO_OP_BUCKET}
    assert OUT_OF_SCOPE == expected


def test_no_op_table_equals_the_taxonomy():
    expected = {name for name, bucket in _taxonomy().items() if bucket == NO_OP_BUCKET}
    assert set(NO_OP_TOOLS) == expected


def test_every_bucket_in_the_taxonomy_has_a_sentence():
    assert set(BUCKETS) == set(_taxonomy().values())


def test_every_key_names_a_tool_the_corpus_actually_contains():
    census_tools = set(json.loads(CENSUS_JSON.read_text())["tools"])
    unknown = sorted(set(_taxonomy()) - census_tools)
    assert not unknown, f"scope keys that no corpus tool uses (typo?): {unknown}"


def _tool(tool_name: str = "", plugin: str = "") -> AlteryxTool:
    return AlteryxTool(tool_id=1, plugin=plugin, tool_name=tool_name, configuration=ET.Element("Configuration"))


def test_census_tool_name_uses_the_macro_basename_with_its_extension():
    assert census_tool_name(_tool(plugin="Precision Match\\Precision_Match.yxmc")) == "Precision_Match.yxmc"
    assert census_tool_name(_tool(tool_name="Filter", plugin="AlteryxBasePluginsGui.Filter.Filter")) == "Filter"
    assert census_tool_name(_tool()) == "<unknown>"


def test_macro_keys_classify_through_the_basename():
    macros = sorted(name for name in _taxonomy() if name.endswith(".yxmc"))
    assert len(macros) == 7
    for macro in macros:
        verdict = classify(census_tool_name(_tool(plugin=f"Some Folder\\{macro}")))
        assert verdict is not None, macro
        assert verdict.status == "out_of_scope"


def test_an_unlisted_tool_is_in_scope():
    assert classify("Filter") is None
