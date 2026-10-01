"""Per-type settings normalisation: which differences between a canvas node and its rebuilt twin are cosmetic.

A notebook push rebuilds every node from its cell and must not re-send a node whose settings only
differ in ways that never change what it computes. :func:`normalise` maps a node's ``setting_input``
dict (``model_dump(mode="json")`` or the save format) onto a canonical form; :func:`settings_equal`
compares two of them. The rules are a small table keyed by node type, on top of the fields every node drops.
"""

from __future__ import annotations

import copy
import re
import textwrap
from collections.abc import Callable
from pathlib import PurePath
from typing import Any

from flowfile_core.flowfile.param_types import FlowParameter

DROPPED_FIELDS = frozenset(
    {
        "flow_id",
        "user_id",
        "pos_x",
        "pos_y",
        "group_id",
        "is_setup",
        "description_is_auto_generated",
        "description",
        "node_reference",
        "cache_results",
    }
)
_SELECT_POSITION_FIELDS = ("position", "original_position", "is_altered", "is_available")
_OPERATORS = {
    "==": "equals",
    "=": "equals",
    "!=": "not_equals",
    ">": "greater_than",
    ">=": "greater_than_or_equals",
    "<": "less_than",
    "<=": "less_than_or_equals",
}
_PARAM_COMPARISON = re.compile(r"\[(?P<field>[^\[\]]+)\]\s*(?P<op>==|=|!=|>=|<=|>|<)\s*\$\{(?P<value>\w+)\}")
_PARAM_BETWEEN = re.compile(
    r"\(\s*\[(?P<field>[^\[\]]+)\]\s*>=\s*\$\{(?P<value>\w+)\}\s*\)\s*and\s*"
    r"\(\s*\[(?P<field2>[^\[\]]+)\]\s*<=\s*\$\{(?P<value2>\w+)\}\s*\)"
)


def normalise_formula(formula: str | None) -> str:
    """A formula through the translator both ways (formula to expression to formula), else stripped.

    Parenthesisation and spacing are cosmetic: ``[a] > 1`` and ``([a] > 1)`` both come back as the
    frame's formula spelling of the same expression. A formula the translator cannot express (or
    one holding ``${name}``) is compared as its stripped text.
    """
    text = (formula or "").strip()
    if not text or "${" in text:
        return text
    from flowfile_core.flowfile.code_generator.code_generator import (
        _eval_in_validation_namespace,
        _try_translate_to_ff_code,
    )

    code = _try_translate_to_ff_code(text)
    if not code:
        return text
    try:
        spelled = getattr(_eval_in_validation_namespace(code), "_ff_repr", None)
    except Exception:
        return text
    return spelled.strip() if isinstance(spelled, str) and spelled.strip() else text


def strip_outer_parens(formula: str) -> str:
    """``formula`` without parentheses that wrap all of it (quoted text and ``[column]`` names respected)."""
    text = formula.strip()
    while text.startswith("(") and text.endswith(")"):
        depth, quote, closes_at = 0, None, None
        for index, char in enumerate(text):
            if quote:
                quote = None if char == quote else quote
            elif char in "\"'[":
                quote = "]" if char == "[" else char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    closes_at = index
                    break
        if closes_at != len(text) - 1:
            break
        text = text[1:-1].strip()
    return text


def param_comparison_filter(formula: str | None) -> dict | None:
    """The basic ``filter_input`` an advanced ``[col] <op> ${name}`` (or a between of two refs) spells, or None.

    The frame stores ``ff.col("x") > PARAM`` as ``([x] > ${param})``; a canvas basic filter whose value
    is the whole-field ``${param}`` is the same comparison, since both substitute before they run.
    """
    text = strip_outer_parens(formula or "")
    match = _PARAM_COMPARISON.fullmatch(text)
    if match:
        basic = {"field": match["field"], "operator": _OPERATORS[match["op"]], "value": f"${{{match['value']}}}"}
    else:
        match = _PARAM_BETWEEN.fullmatch(text)
        if match is None or match["field"] != match["field2"]:
            return None
        basic = {
            "field": match["field"],
            "operator": "between",
            "value": f"${{{match['value']}}}",
            "value2": f"${{{match['value2']}}}",
        }
    return {"mode": "basic", "basic_filter": basic, "advanced_filter": ""}


def _operator_name(operator: str | None) -> str:
    return _OPERATORS.get(operator or "", operator or "equals")


def _formula(settings: dict) -> None:
    entries = settings.get("functions") or ([settings["function"]] if settings.get("function") else [])
    settings.pop("function", None)
    settings["functions"] = [
        {**entry, "function": normalise_formula(entry.get("function"))} for entry in entries if isinstance(entry, dict)
    ]


def _filter(settings: dict) -> None:
    """Basic and advanced spellings of one comparison compare equal; stale fields of the other mode drop."""
    from flowfile_core.flowfile.share.filter_translation import translate_advanced_filter

    filter_input = settings.get("filter_input") or {}
    filter_input.pop("filter_type", None)
    if filter_input.get("mode") == "advanced":
        advanced = filter_input.get("advanced_filter") or ""
        basic = param_comparison_filter(advanced) if "${" in advanced else translate_advanced_filter(advanced)
        if basic is not None:
            filter_input.clear()
            filter_input.update(basic)
        else:
            filter_input["advanced_filter"] = normalise_formula(filter_input.get("advanced_filter"))
            filter_input["basic_filter"] = None
    if filter_input.get("mode") == "basic":
        basic = filter_input.get("basic_filter") or {}
        filter_input["basic_filter"] = {
            "field": basic.get("field") or "",
            "operator": _operator_name(basic.get("operator")),
            "value": basic.get("value") or "",
            "value2": basic.get("value2") or None,
        }
        filter_input["advanced_filter"] = ""


def _select_entry(entry: dict) -> dict:
    """The ordered projection of a select entry; a dtype only counts when the entry changes it."""
    return {
        "old_name": entry.get("old_name"),
        "new_name": entry.get("new_name") or entry.get("old_name"),
        "data_type": entry.get("data_type") if entry.get("data_type_change") else None,
        "keep": entry.get("keep", True),
    }


def _select(settings: dict) -> None:
    """Without ``keep_missing`` an unlisted column is dropped, so a listed ``keep=False`` entry says nothing more."""
    entries = [_select_entry(entry) for entry in settings.get("select_input") or []]
    if not settings.get("keep_missing", True):
        entries = [entry for entry in entries if entry["keep"]]
    settings["select_input"] = entries


def _record_id(settings: dict) -> None:
    """Group-by columns only count while grouping is on."""
    record = settings.get("record_id_input") or {}
    if not record.get("group_by"):
        record["group_by"] = False
        record["group_by_columns"] = []


def _polars_code(settings: dict) -> None:
    """The code as it runs: dedented, stripped, and without a closing ``return output_df`` the runtime adds anyway."""
    code_input = settings.get("polars_code_input") or {}
    code = textwrap.dedent(code_input.get("polars_code") or "").strip()
    head, _, last = code.rpartition("\n")
    if last.strip() == "return output_df" and re.search(r"^output_df\s*=[^=]", head, re.M):
        code = head.rstrip()
    code_input["polars_code"] = code


def _output(settings: dict) -> None:
    """The written file is ``abs_file_path``; a directory spelled as the file path itself is the same target."""
    output = settings.get("output_settings") or {}
    path = output.get("abs_file_path")
    if path:
        output["directory"] = str(PurePath(path).parent)
        output["name"] = PurePath(path).name


def _read(settings: dict) -> None:
    """A read node's ``name`` is a display label (the file is ``abs_file_path``); UTF-8 spellings are one encoding."""
    received = settings.get("received_file") or {}
    if received.get("abs_file_path"):
        received.pop("name", None)
    table = received.get("table_settings") or {}
    encoding = table.get("encoding")
    if isinstance(encoding, str) and encoding.upper().replace("-", "") in ("UTF8", "UTF8LOSSY"):
        table["encoding"] = "utf8-lossy" if "LOSSY" in encoding.upper() else "utf8"


def _text_to_rows(settings: dict) -> None:
    """No output column means the split column; the unused split source of the other mode drops."""
    text = settings.get("text_to_rows_input") or {}
    text["output_column_name"] = text.get("output_column_name") or text.get("column_to_split")
    if text.get("split_by_fixed_value", True):
        text.pop("split_by_column", None)
    else:
        text.pop("split_fixed_value", None)


def _gate(settings: dict) -> None:
    """Only the fields of the active condition source count; the designer keeps the other's stale values."""
    gate_input = settings.get("gate_input") or {}
    if gate_input.get("condition_source") == "formula":
        gate_input.pop("parameter", None)
        gate_input.pop("operator", None)
        gate_input.pop("value", None)
        gate_input["formula"] = normalise_formula(gate_input.get("formula"))
    else:
        gate_input.pop("formula", None)


def _python_script(settings: dict) -> None:
    """Cells compare by code, never by their ids."""
    script = settings.get("python_script_input") or {}
    cells = script.get("cells")
    if cells is not None:
        script["cells"] = [cell.get("code", "") if isinstance(cell, dict) else cell for cell in cells]


def _drop_select_positions(value: Any) -> None:
    """Select entries nested anywhere (join and fuzzy-match column lists) lose their position fields."""
    if isinstance(value, dict):
        if "old_name" in value and "keep" in value:
            for key in _SELECT_POSITION_FIELDS:
                value.pop(key, None)
        for item in value.values():
            _drop_select_positions(item)
    elif isinstance(value, list):
        for item in value:
            _drop_select_positions(item)


RULES: dict[str, tuple[Callable[[dict], None], ...]] = {
    "formula": (_formula,),
    "filter": (_filter,),
    "select": (_select,),
    "output": (_output,),
    "read": (_read,),
    "text_to_rows": (_text_to_rows,),
    "gate": (_gate,),
    "python_script": (_python_script,),
    "record_id": (_record_id,),
    "polars_code": (_polars_code,),
}


def normalise(settings: dict | None, node_type: str) -> dict | None:
    """``settings`` (a node's ``setting_input`` dict) in canonical form for ``node_type``; the input is not mutated."""
    if settings is None:
        return None
    normalised = {key: value for key, value in copy.deepcopy(settings).items() if key not in DROPPED_FIELDS}
    for rule in RULES.get(node_type, ()):
        rule(normalised)
    _drop_select_positions(normalised)
    return normalised


def settings_equal(a: dict | None, b: dict | None, node_type: str) -> bool:
    """Whether two ``setting_input`` dicts of a ``node_type`` node differ only cosmetically."""
    return normalise(a, node_type) == normalise(b, node_type)


def parameters_equal(a: list[Any], b: list[Any]) -> bool:
    """Flow parameters compared as ``FlowParameter`` models (a dict and its model compare equal), in order."""

    def models(params: list[Any]) -> list[FlowParameter]:
        return [p if isinstance(p, FlowParameter) else FlowParameter.model_validate(p) for p in params or []]

    return models(a) == models(b)
