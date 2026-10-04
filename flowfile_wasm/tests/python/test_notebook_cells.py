"""Reading changed notebook cells back into the flow, on the browser's pinned versions.

A cell left as the render wrote it changes nothing, whatever its layout. A changed call becomes
the settings it describes, and laying those over the flow renders the cell the user wrote. A
change the browser cannot keep fails on its line, and no cell text is ever executed.
"""

import ast
import builtins
import copy
import json
from pathlib import Path

import pytest
from engine.notebook_cells import sync_notebook
from engine.notebook_render import render_notebook

GOLDEN = json.loads((Path(__file__).parent / "notebook_golden.json").read_text())
FLOWS = {flow["name"]: flow for flow in GOLDEN["flows"]}

SETTINGS = {
    "description": "",
    "execution_mode": "Development",
    "execution_location": "local",
    "auto_save": True,
    "show_detailed_progress": False,
}
SOURCE = {
    "raw_data_format": {
        "columns": [
            {"name": "product", "data_type": "String"},
            {"name": "revenue", "data_type": "Int64"},
            {"name": "sold", "data_type": "Date"},
        ],
        "data": [["Widget", "Gadget"], [100, 200], ["2024-01-01", "2024-02-01"]],
    }
}
SCHEMA = [
    {"name": "product", "data_type": "String"},
    {"name": "revenue", "data_type": "Int64"},
    {"name": "sold", "data_type": "Date"},
]


UNCHANGED = {"ok": True, "nodes": {}, "added": [], "inputs": {}, "warnings": []}


def changes(result: dict) -> dict:
    """A sync's answer without the list of which cell holds which nodes."""
    return {key: value for key, value in result.items() if key != "node_ids_by_cell"}


def node(node_id: int, node_type: str, inputs: list[int], settings: dict, **extra) -> dict:
    return {
        "id": node_id,
        "type": node_type,
        "is_start_node": not inputs,
        "description": "",
        "x_position": node_id * 200,
        "y_position": 0,
        "input_ids": inputs,
        "outputs": [],
        "setting_input": {"node_id": node_id, "is_setup": True, **settings},
        **extra,
    }


def flow_of(*nodes: dict) -> dict:
    return {
        "flowfile_version": "1.0.0",
        "flowfile_id": 1,
        "flowfile_name": "cells",
        "flowfile_settings": SETTINGS,
        "nodes": list(nodes),
    }


def schemas_of(flow: dict) -> dict:
    return {str(each["id"]): SCHEMA for each in flow["nodes"]}


def cells_of(flow: dict) -> dict[str, str]:
    return {cell["cell_id"]: cell["code"] for cell in render_notebook(copy.deepcopy(flow), schemas_of(flow))["cells"]}


def sync(flow: dict, drafts: dict[str, str], locked: dict | None = None) -> dict:
    return sync_notebook(copy.deepcopy(flow), schemas_of(flow), locked or {}, drafts)


def merged(base, patch):
    """Lay ``patch`` over ``base``: dicts merge, anything else is replaced."""
    if not isinstance(base, dict) or not isinstance(patch, dict):
        return copy.deepcopy(patch)
    return {**base, **{key: merged(base.get(key), value) for key, value in patch.items()}}


def applied(flow: dict, result: dict) -> dict:
    """The flow after a sync, the way the editor lands it."""
    assert result["ok"], result
    flow = copy.deepcopy(flow)
    for new in result["added"]:
        flow["nodes"].append(node(new["id"], new["type"], [], new["settings"], description=new["description"]))
        if new["node_reference"]:
            flow["nodes"][-1]["node_reference"] = new["node_reference"]
    by_id = {each["id"]: each for each in flow["nodes"]}
    for node_id, change in result["nodes"].items():
        target = by_id[int(node_id)]
        if "settings" in change:
            target["setting_input"] = merged(target["setting_input"], change["settings"])
        if "description" in change:
            target["description"] = change["description"]
        if "node_reference" in change:
            target["node_reference"] = change["node_reference"]
    for node_id, ports in result["inputs"].items():
        target = by_id[int(node_id)]
        target["input_ids"] = ports["main"]
        target["right_input_id"] = ports["right"]
    return flow


def edited(flow: dict, cell_id: str, old: str, new: str) -> tuple[dict, str]:
    """Change ``old`` to ``new`` in one cell; the sync's answer and the text that was sent."""
    code = cells_of(flow)[cell_id]
    assert old in code, code
    draft = code.replace(old, new)
    return sync(flow, {cell_id: draft}), draft


def chain(*steps: tuple[str, dict]) -> dict:
    """A manual input feeding one node per step, in a line."""
    nodes = [node(1, "manual_input", [], SOURCE)]
    for position, (node_type, settings) in enumerate(steps, start=2):
        nodes.append(node(position, node_type, [position - 1], settings))
    return flow_of(*nodes)


def named(*steps: tuple[str, dict]) -> dict:
    """The same line, with the source named ``sales`` so it keeps a cell of its own."""
    flow = chain(*steps)
    flow["nodes"][0]["node_reference"] = "sales"
    return flow


BASIC = {"mode": "basic", "basic_filter": {"field": "revenue", "operator": "greater_than", "value": "60"}}
FILTER = ("filter", {"filter_input": {**BASIC, "advanced_filter": ""}})
SORT = ("sort", {"sort_input": [{"column": "revenue", "how": "desc"}]})
SELECT = (
    "select",
    {
        "keep_missing": False,
        "select_input": [
            {"old_name": "product", "new_name": "item", "keep": True, "position": 0, "data_type": "String"},
            {"old_name": "revenue", "new_name": "revenue", "keep": True, "position": 1, "data_type": "Int64"},
            {"old_name": "sold", "new_name": "sold", "keep": False, "position": 2, "data_type": "Date"},
        ],
    },
)


# An unchanged notebook changes nothing


def _readable(cell: dict) -> bool:
    if cell["kind"] != "node" or cell["status"] != "code":
        return False
    try:
        ast.parse(cell["code"])
    except SyntaxError:
        return False
    return True


@pytest.mark.parametrize("name", list(FLOWS))
def test_cells_the_render_wrote_change_nothing_however_they_are_laid_out(name):
    """Every node of every golden flow is recognised in its own cell, with no handler reading it."""
    golden = FLOWS[name]
    touched = {cell["cell_id"]: cell["code"] + "\n# touched\n" for cell in golden["cells"] if _readable(cell)}
    reflowed = {cell_id: ast.unparse(ast.parse(code)) for cell_id, code in touched.items()}
    for drafts in ({}, touched, reflowed):
        result = sync_notebook(copy.deepcopy(golden["flow"]), golden["schemas"], {}, drafts)
        assert changes(result) == UNCHANGED, result
        if drafts:
            # Every cell that was read holds the nodes it held.
            rendered = {cell["cell_id"]: cell for cell in golden["cells"]}
            read = [cell_id for cell_id, text in drafts.items() if text != rendered[cell_id]["code"]]
            held = {cell_id: sum(lines, []) for cell_id, lines in result["node_ids_by_cell"].items()}
            assert held == {cell_id: rendered[cell_id]["node_ids"] for cell_id in read}


def test_a_draft_equal_to_the_render_is_not_read_at_all():
    flow = chain(FILTER, SORT)
    cells = cells_of(flow)
    assert sync(flow, dict(cells)) == {**UNCHANGED, "node_ids_by_cell": {}}


# A changed call becomes the settings it describes

EDITS = [
    (
        "sort: columns and directions",
        chain(SORT),
        'sort(["revenue"], descending=[True])',
        'sort(["product", "revenue"], descending=[False, True])',
        {2: {"sort_input": [{"column": "product", "how": "asc"}, {"column": "revenue", "how": "desc"}]}},
    ),
    (
        "head: the row count",
        chain(("sample", {"sample_size": 10, "sample_method": "first"})),
        "head(10)",
        "head(3)",
        {2: {"sample_size": 3}},
    ),
    (
        "unique: subset and strategy",
        chain(("unique", {"unique_input": {"columns": ["product"], "strategy": "first"}})),
        "unique(subset=['product'], keep='first')",
        "unique(subset=['product', 'sold'], keep='last')",
        {2: {"unique_input": {"columns": ["product", "sold"], "strategy": "last"}}},
    ),
    (
        "unique: every column",
        chain(("unique", {"unique_input": {"columns": ["product"], "strategy": "first"}})),
        "unique(subset=['product'], keep='first')",
        "unique(keep='any')",
        {2: {"unique_input": {"columns": None, "strategy": "any"}}},
    ),
    (
        "record id: name and offset",
        chain(("record_id", {"record_id_input": {"output_column_name": "record_id", "offset": 1}})),
        'with_row_index("record_id", offset=1)',
        'with_row_index("row", offset=100)',
        {2: {"record_id_input": {"output_column_name": "row", "offset": 100}}},
    ),
    (
        "unpivot: index and value columns",
        chain(("unpivot", {"unpivot_input": {"index_columns": ["product"], "value_columns": ["revenue"]}})),
        "index=['product']",
        "index=['product', 'sold']",
        {2: {"unpivot_input": {"index_columns": ["product", "sold"], "value_columns": ["revenue"]}}},
    ),
    (
        "dynamic rename: mode and columns",
        chain(("dynamic_rename", {"dynamic_rename_input": {"rename_mode": "prefix", "prefix": "a_"}})),
        "dynamic_rename(mode='prefix', prefix='a_')",
        "dynamic_rename(mode='suffix', suffix='_b', columns=['product'])",
        {
            2: {
                "dynamic_rename_input": {
                    "rename_mode": "suffix",
                    "prefix": "",
                    "suffix": "_b",
                    "selection_mode": "list",
                    "selected_columns": ["product"],
                }
            }
        },
    ),
    (
        "filter: another value",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("revenue") > 75.5',
        {2: {"filter_input": {"mode": "basic", "basic_filter": {"operator": "greater_than", "value": "75.5"}}}},
    ),
    (
        "filter: another column and operator",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("product") != "Gizmo"',
        {2: {"filter_input": {"basic_filter": {"field": "product", "operator": "not_equals", "value": "Gizmo"}}}},
    ),
    (
        "filter: a text match",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("product").str.starts_with("W")',
        {2: {"filter_input": {"basic_filter": {"field": "product", "operator": "starts_with", "value": "W"}}}},
    ),
    (
        "filter: a negated text match",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("product").str.contains("dg").not_()',
        {2: {"filter_input": {"basic_filter": {"field": "product", "operator": "not_contains", "value": "dg"}}}},
    ),
    (
        "filter: membership",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("revenue").is_in([100, 200])',
        {2: {"filter_input": {"basic_filter": {"field": "revenue", "operator": "in", "value": "100, 200"}}}},
    ),
    (
        "filter: outside a list",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("product").is_in(["Widget", "Gizmo"]).not_()',
        {2: {"filter_input": {"basic_filter": {"field": "product", "operator": "not_in", "value": "Widget, Gizmo"}}}},
    ),
    (
        "filter: a range",
        chain(FILTER),
        'ff.col("revenue") > 60',
        '(ff.col("revenue") >= 10) & (ff.col("revenue") <= 150)',
        {
            2: {
                "filter_input": {
                    "basic_filter": {"field": "revenue", "operator": "between", "value": "10", "value2": "150"}
                }
            }
        },
    ),
    (
        "filter: null check",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("product").is_not_null()',
        {2: {"filter_input": {"basic_filter": {"field": "product", "operator": "is_not_null"}}}},
    ),
    (
        "filter: a date",
        chain(FILTER),
        'ff.col("revenue") > 60',
        'ff.col("sold") >= datetime.date(2024, 1, 15)',
        {
            2: {
                "filter_input": {
                    "basic_filter": {"field": "sold", "operator": "greater_than_or_equals", "value": "2024-01-15"}
                }
            }
        },
    ),
    (
        "filter: formula text",
        chain(FILTER),
        'filter(ff.col("revenue") > 60)',
        "filter(flowfile_formula='[revenue] > 60 and length([product]) > 3 or [revenue] = 1')",
        {
            2: {
                "filter_input": {
                    "mode": "advanced",
                    "advanced_filter": "[revenue] > 60 and length([product]) > 3 or [revenue] = 1",
                }
            }
        },
    ),
    (
        "manual input: its data",
        chain(SORT),
        "[['Widget', 'Gadget'], [100, 200], ['2024-01-01', '2024-02-01']]",
        "[['Widget', 'Gadget', 'Gizmo'], [100, 200, 50], ['2024-01-01', '2024-02-01', '2024-03-01']]",
        {
            1: {
                "raw_data_format": {
                    "data": [
                        ["Widget", "Gadget", "Gizmo"],
                        [100, 200, 50],
                        ["2024-01-01", "2024-02-01", "2024-03-01"],
                    ]
                }
            }
        },
    ),
    (
        "csv output: separator",
        chain(
            (
                "output",
                {
                    "output_settings": {
                        "name": "out.csv",
                        "directory": "",
                        "file_type": "csv",
                        "write_mode": "overwrite",
                        "table_settings": {"file_type": "csv", "delimiter": ",", "encoding": "utf-8"},
                    }
                },
            )
        ),
        'write_csv("out.csv", separator=",")',
        'write_csv("report.csv", separator=";")',
        {2: {"output_settings": {"name": "report.csv", "table_settings": {"delimiter": ";"}}}},
    ),
]


def contains(actual, expected) -> bool:
    """Whether ``actual`` holds everything ``expected`` names, dict by dict."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(contains(actual.get(key), value) for key, value in expected.items())
    return actual == expected


@pytest.mark.parametrize(("label", "flow", "old", "new", "settings"), EDITS, ids=[edit[0] for edit in EDITS])
def test_a_changed_call_becomes_the_settings_it_describes(label, flow, old, new, settings):
    cell_id = next(cell_id for cell_id, code in cells_of(flow).items() if old in code)
    result, draft = edited(flow, cell_id, old, new)
    assert result["ok"], result
    assert set(result["nodes"]) == {str(node_id) for node_id in settings}
    after = applied(flow, result)
    for node_id, expected in settings.items():
        actual = next(each for each in after["nodes"] if each["id"] == node_id)["setting_input"]
        assert contains(actual, expected), actual
    assert result["inputs"] == {}
    # What the canvas now says is what was written (formula text may come back translated),
    # and reading it again changes nothing more.
    if "flowfile_formula=" not in new:
        assert ast.dump(ast.parse(cells_of(after)[cell_id])) == ast.dump(ast.parse(draft))
    assert sync(after, {cell_id: draft})["nodes"] == {}


def test_a_spelling_the_render_does_not_use_is_read_and_comes_back_as_the_render_writes_it():
    flow = chain(SORT)
    result, _ = edited(flow, "cell-1", 'sort(["revenue"], descending=[True])', 'sort("product", descending=False)')
    assert result["nodes"] == {"2": {"settings": {"sort_input": [{"column": "product", "how": "asc"}]}}}
    assert '.sort(["product"], descending=[False])' in cells_of(applied(flow, result))["cell-1"]


def test_only_the_changed_call_of_a_fused_cell_is_read():
    """The source and the filter are recognised as written; only the sort is turned into settings."""
    flow = chain(FILTER, ("formula", {"functions": [{"field": {"name": "double", "data_type": "Auto"}, "function": "[revenue] * 2"}]}), SORT)
    assert list(cells_of(flow)) == ["imports", "cell-1"]
    result, _ = edited(flow, "cell-1", "descending=[True]", "descending=[False]")
    assert result["nodes"] == {"4": {"settings": {"sort_input": [{"column": "revenue", "how": "asc"}]}}}
    assert result["inputs"] == {}


def test_a_node_keeps_what_its_code_does_not_say():
    flow = chain(("sort", {"sort_input": [{"column": "revenue", "how": "desc"}], "cache_results": True, "pos_x": 40}))
    result, _ = edited(flow, "cell-1", "descending=[True]", "descending=[False]")
    settings = next(each for each in applied(flow, result)["nodes"] if each["id"] == 2)["setting_input"]
    assert settings["cache_results"] is True and settings["pos_x"] == 40 and settings["node_id"] == 2


def test_a_formula_entry_is_changed_on_its_own():
    """An entry written as an expression stays as it was; the one in formula text is read."""
    entries = [
        {"field": {"name": "double", "data_type": "Auto"}, "function": "[revenue] * 2"},
        {"field": {"name": "label", "data_type": "String"}, "function": "[product]"},
    ]
    flow = chain(("formula", {"functions": entries}))
    result, draft = edited(flow, "cell-1", "['[product]']", "['uppercase([product])']")
    functions = result["nodes"]["2"]["settings"]["functions"]
    assert functions == [entries[0], {"field": {"name": "label", "data_type": "String"}, "function": "uppercase([product])"}]
    assert ast.dump(ast.parse(cells_of(applied(flow, result))["cell-1"])) == ast.dump(ast.parse(draft))


def test_a_description_rides_on_its_call():
    flow = chain(SORT)
    result, draft = edited(flow, "cell-1", "descending=[True])", 'descending=[False], description="Cheapest first")')
    assert result["nodes"]["2"]["description"] == "Cheapest first"
    described = applied(flow, result)
    assert 'descending=[False], description="Cheapest first")' in cells_of(described)["cell-1"]

    cleared, _ = edited(described, "cell-1", ', description="Cheapest first"', "")
    assert cleared["nodes"] == {"2": {"description": ""}}


def test_a_filter_that_was_not_set_takes_the_condition_written_on_it():
    flow = chain(("filter", {"filter_input": {"mode": "basic", "basic_filter": {"field": "", "operator": "equals", "value": ""}}}), SORT)
    assert "# No filter applied" in cells_of(flow)["cell-2"]
    untouched, _ = edited(flow, "cell-2", "descending=[True]", "descending=[False]")
    assert set(untouched["nodes"]) == {"3"} and untouched["inputs"] == {}

    result = sync(flow, {"cell-2": 'ordered_3 = source_1.filter(ff.col("revenue") > 5).sort(["revenue"], descending=[True])'})
    assert result["nodes"]["2"]["settings"]["filter_input"]["basic_filter"]["field"] == "revenue"
    assert result["inputs"] == {}


# A call the cell did not have is a new node


def test_a_call_added_to_a_chain_is_a_new_node_after_the_one_before_it():
    flow = chain(SORT)
    result, _ = edited(flow, "cell-1", "descending=[True])", "descending=[True]).head(5)")
    assert result["nodes"] == {}
    assert result["added"] == [
        {"id": 3, "type": "sample", "settings": {"sample_size": 5}, "description": "", "node_reference": None}
    ]
    assert result["inputs"] == {"3": {"main": [2], "right": None, "left": None}}
    after = cells_of(applied(flow, result))
    assert list(after) == ["imports", "cell-1"]
    assert after["cell-1"].startswith("sampled_3 = (") and ".sort([\"revenue\"], descending=[True])\n    .head(5)" in after["cell-1"]
    assert changes(sync(applied(flow, result), {"cell-1": after["cell-1"] + "\n"})) == UNCHANGED


def test_a_call_added_between_two_steps_is_read_by_the_one_after_it():
    flow = chain(FILTER, SORT)
    result, _ = edited(flow, "cell-1", ".sort(", ".head(3).sort(")
    assert [(new["id"], new["type"]) for new in result["added"]] == [(4, "sample")]
    assert {node_id: ports["main"] for node_id, ports in result["inputs"].items()} == {"3": [4], "4": [2]}
    code = cells_of(applied(flow, result))["cell-1"]
    assert code.index(".filter(") < code.index(".head(3)") < code.index(".sort(")


def test_a_new_line_is_a_new_node_under_the_name_it_is_given():
    flow = named(SORT)
    draft = cells_of(flow)["cell-2"] + '\ntop = ordered_2.select(["product", ff.col("revenue").alias("rev").cast(ff.Float64)])'
    result = sync(flow, {"cell-2": draft})
    (new,) = result["added"]
    assert (new["id"], new["type"], new["node_reference"]) == (3, "select", "top")
    assert new["settings"] == {
        "keep_missing": False,
        "select_input": [
            {"old_name": "product", "new_name": "product", "keep": True, "is_available": True, "position": 0, "data_type": None, "data_type_change": False, "is_altered": False},
            {"old_name": "revenue", "new_name": "rev", "keep": True, "is_available": True, "position": 1, "data_type": "Float64", "data_type_change": True, "is_altered": True},
            {"old_name": "sold", "new_name": "sold", "keep": False, "is_available": True, "position": 2, "data_type": None},
        ],
    }
    assert result["inputs"] == {"3": {"main": [2], "right": None, "left": None}}
    code = cells_of(applied(flow, result))["cell-2"]
    assert code.startswith("top = (") and 'ff.col("product"),' in code and 'ff.col("revenue").alias("rev").cast(ff.Float64),' in code


def test_new_nodes_of_every_kind_the_notebook_writes():
    flow = named(SORT)
    lines = [
        cells_of(flow)["cell-2"],
        'big = ordered_2.filter(ff.col("revenue") > 150)',
        "both = ff.concat([sales, big], how='diagonal_relaxed')",
        "counted = both.select(ff.len().alias('number_of_records'))",
        "labelled = big.with_columns(flowfile_formulas=['[revenue] * 2'], output_column_names=['double'])"
        ".with_columns(flowfile_formulas=['uppercase([product])'], output_column_names=['shout'], output_column_datatypes=['String'])",
        'big.write_csv("big.csv", separator=";")',
        "extra = ff.from_raw_data({'columns': [{'name': 'n', 'data_type': 'Int64'}], 'data': [[1, 2]]})",
    ]
    result = sync(flow, {"cell-2": "\n".join(lines)})
    assert result["ok"], result
    assert [(new["id"], new["type"], new["node_reference"]) for new in result["added"]] == [
        (3, "filter", "big"),
        (4, "union", "both"),
        (5, "record_count", "counted"),
        (6, "formula", "labelled"),
        (7, "output", None),
        (8, "manual_input", "extra"),
    ]
    assert {node_id: ports["main"] for node_id, ports in result["inputs"].items()} == {"3": [2], "4": [1, 3], "5": [4], "6": [3], "7": [3]}
    settings = {new["id"]: new["settings"] for new in result["added"]}
    assert [entry["field"]["name"] for entry in settings[6]["functions"]] == ["double", "shout"]
    assert settings[7]["output_settings"] == {
        "file_type": "csv",
        "write_mode": "overwrite",
        "polars_method": "sink_csv",
        "table_settings": {"file_type": "csv", "delimiter": ";", "encoding": "utf-8"},
        "directory": "",
        "name": "big.csv",
    }
    after = applied(flow, result)
    rendered = "\n".join(cells_of(after).values())
    for written in ('big.csv', "ff.concat([", ".select(ff.len().alias('number_of_records'))", "output_column_names=['shout']"):
        assert written in rendered
    # Reading the notebook it made again changes nothing.
    again = sync(after, {cell_id: code + "\n" for cell_id, code in cells_of(after).items() if cell_id != "imports"})
    assert changes(again) == UNCHANGED


def test_new_nodes_take_ids_from_the_one_given():
    flow = chain(SORT)
    draft = cells_of(flow)["cell-1"].replace("descending=[True])", "descending=[True]).head(5)")
    result = sync_notebook(copy.deepcopy(flow), schemas_of(flow), {}, {"cell-1": draft}, 40)
    assert [new["id"] for new in result["added"]] == [40]


def test_readers_of_a_name_move_to_the_node_added_under_it():
    flow = flow_of(
        node(1, "manual_input", [], SOURCE, node_reference="sales"),
        node(2, "sort", [1], SORT[1], node_reference="ranked"),
        node(3, "sample", [2], {"sample_size": 5, "sample_method": "first"}),
    )
    cells = cells_of(flow)
    result = sync(flow, {"cell-2": cells["cell-2"] + '.filter(ff.col("revenue") > 150)'})
    # The name moves to the new last step, and the cell that reads it follows.
    assert [(new["id"], new["type"], new["node_reference"]) for new in result["added"]] == [(4, "filter", "ranked")]
    assert result["nodes"] == {"2": {"node_reference": None}}
    assert {node_id: ports["main"] for node_id, ports in result["inputs"].items()} == {"3": [4], "4": [2]}
    after = cells_of(applied(flow, result))
    assert after["cell-2"].startswith("ranked = (") and "ranked.head(5)" in after["cell-3"]


def test_a_select_names_every_column_it_leaves_out_so_the_editor_does_not_keep_them():
    """The columns come through the new steps before it: a sort hands on what it reads, a formula adds one."""
    flow = named(SORT)
    draft = (
        cells_of(flow)["cell-2"]
        + "\npicked = ordered_2.head(3).with_columns(flowfile_formulas=['[revenue] * 2'], output_column_names=['double'])"
        + '.select(["double", "product"])'
    )
    result = sync(flow, {"cell-2": draft})
    rows = result["added"][-1]["settings"]["select_input"]
    assert [(row["old_name"], row["keep"]) for row in rows] == [
        ("double", True),
        ("product", True),
        ("revenue", False),
        ("sold", False),
    ]


def test_a_select_needs_to_know_the_columns_and_takes_only_ones_that_are_there():
    flow = named(SORT)
    draft = cells_of(flow)["cell-2"] + '\npicked = ordered_2.select(["product"])'
    unknown = sync_notebook(copy.deepcopy(flow), {}, {}, {"cell-2": draft})
    assert (unknown["ok"], unknown["kind"], unknown["line"]) == (False, "refused", 2)
    assert "not known yet" in unknown["message"]

    missing = sync(flow, {"cell-2": draft.replace('"product"', '"prodcut"')})
    assert (missing["ok"], missing["kind"], missing["line"]) == (False, "error", 2)
    assert "`prodcut` is not one of product, revenue, sold" in missing["message"]


# A cell the user wrote stays that cell


def laid_out(flow: dict, layout: list[list[int]]) -> dict[str, dict]:
    rendering = render_notebook(copy.deepcopy(flow), schemas_of(flow), {}, layout)
    return {cell["cell_id"]: cell for cell in rendering["cells"]}


def test_a_new_cell_is_read_where_it_stands_and_its_nodes_are_reported():
    flow = named(SORT)
    result = sync_notebook(
        copy.deepcopy(flow),
        schemas_of(flow),
        {},
        {},
        None,
        None,
        ["imports", "cell-1", "cell-2", "new-1"],
        {"new-1": 'top = ordered_2.head(3)\ntop.select(["product"])'},
    )
    assert [(new["id"], new["type"], new["node_reference"]) for new in result["added"]] == [
        (3, "sample", "top"),
        (4, "select", None),
    ]
    assert result["node_ids_by_cell"] == {"new-1": [[3], [4]]}
    assert {node_id: ports["main"] for node_id, ports in result["inputs"].items()} == {"3": [2], "4": [3]}


def test_a_new_cell_reads_only_the_names_of_the_cells_before_it():
    flow = named(SORT)
    early = sync_notebook(
        copy.deepcopy(flow), schemas_of(flow), {}, {}, None, None, ["imports", "cell-1", "new-1", "cell-2"],
        {"new-1": "top = ordered_2.head(3)"},
    )
    assert (early["ok"], early["cell_id"], early["kind"]) == (False, "new-1", "error")
    assert "NameError" in early["message"]


def test_the_render_keeps_the_nodes_of_a_written_cell_together_and_fuses_nothing_into_it():
    """Without a layout the sample and the select fuse into the sort's cell; with one they are the user's cell."""
    flow = flow_of(
        node(1, "manual_input", [], SOURCE),
        node(2, "sort", [1], SORT[1]),
        node(3, "sample", [2], {"sample_size": 3, "sample_method": "first"}, node_reference="top"),
        node(4, "select", [3], {"keep_missing": False, "select_input": [{"old_name": "product", "new_name": "product", "keep": True, "position": 0}]}),
    )
    assert list(cells_of(flow)) == ["imports", "cell-1", "cell-4"]

    cells = laid_out(flow, [[[3], [4]]])
    assert list(cells) == ["imports", "cell-1", "cell-3"]
    assert cells["cell-1"]["node_ids"] == [1, 2] and cells["cell-3"]["node_ids"] == [3, 4]
    assert cells["cell-3"]["code"].startswith("top = ordered_2.head(3)\nselected_4 = top.select([")
    assert cells["cell-3"]["uses"] == ["ff", "ordered_2"]

    # Two steps that do not read each other still share the cell they were written in.
    apart = laid_out(flow_of(*flow["nodes"][:3], node(4, "unique", [2], {"unique_input": {"columns": None, "strategy": "any"}})), [[[3], [4]]])
    assert apart["cell-3"]["node_ids"] == [3, 4]
    assert apart["cell-3"]["code"] == "top = ordered_2.head(3)\ndeduped_4 = ordered_2.unique(keep='any')"

    # Written on one line two steps are one statement; on two lines the second reads the first by its name.
    plain = flow_of(*flow["nodes"][:2], node(3, "sample", [2], {"sample_size": 3, "sample_method": "first"}), flow["nodes"][3])
    assert laid_out(plain, [[[3, 4]]])["cell-3"]["code"].startswith("selected_4 = (\n    ordered_2.head(3)\n    .select([")
    assert laid_out(plain, [[[3], [4]]])["cell-3"]["code"].startswith("sampled_3 = ordered_2.head(3)\nselected_4 = sampled_3.select([")


def test_a_written_cell_reads_back_as_itself():
    flow = named(SORT)
    first = sync_notebook(
        copy.deepcopy(flow), schemas_of(flow), {}, {}, None, None, ["imports", "cell-1", "cell-2", "new-1"],
        {"new-1": 'top = ordered_2.head(3)\npicked = top.select(["product"])'},
    )
    after = applied(flow, first)
    layout = [first["node_ids_by_cell"]["new-1"]]
    assert layout == [[[3], [4]]]
    cells = laid_out(after, layout)
    assert list(cells) == ["imports", "cell-1", "cell-2", "cell-3"]
    again = sync_notebook(copy.deepcopy(after), schemas_of(after), {}, {"cell-3": cells["cell-3"]["code"] + "\n"}, None, layout)
    assert changes(again) == UNCHANGED
    assert again["node_ids_by_cell"] == {"cell-3": [[3], [4]]}


def test_a_layout_naming_nodes_that_are_gone_or_locked_is_harmless():
    flow = chain(FILTER, SORT)
    assert laid_out(flow, [[[9, 10]], [], [[]]]).keys() == cells_of(flow).keys()
    locked = render_notebook(copy.deepcopy(flow), schemas_of(flow), {3: "locked"}, [[[2, 3]]])["cells"]
    assert [(cell["cell_id"], cell["status"]) for cell in locked] == [
        ("imports", "code"),
        ("cell-1", "code"),
        ("cell-2", "code"),
        ("cell-3", "placeholder"),
    ]


# An existing select


def test_a_select_keeps_what_is_listed_in_that_order_and_drops_the_rest():
    flow = chain(SELECT)
    code = cells_of(flow)["cell-1"]
    assert 'ff.col("product").alias("item").cast(ff.Utf8),' in code
    draft = code.replace(
        '        ff.col("product").alias("item").cast(ff.Utf8),\n        ff.col("revenue").cast(ff.Int64),\n',
        '        ff.col("sold"),\n        ff.col("revenue").alias("amount").cast(ff.Float64),\n',
    )
    assert draft != code
    result = sync(flow, {"cell-1": draft})
    rows = result["nodes"]["2"]["settings"]["select_input"]
    assert [(row["old_name"], row["new_name"], row["keep"], row["position"]) for row in rows] == [
        ("sold", "sold", True, 0),
        ("revenue", "amount", True, 1),
        ("product", "item", False, 2),
    ]
    assert (rows[0]["data_type"], rows[0]["data_type_change"]) == (None, False)
    assert (rows[1]["data_type"], rows[1]["data_type_change"]) == ("Float64", True)
    assert ast.dump(ast.parse(cells_of(applied(flow, result))["cell-1"])) == ast.dump(ast.parse(draft))


# Names and inputs


def branches() -> dict:
    """Two named sources; a described filter on the first, read by a sort, and a head on the second."""
    return flow_of(
        node(1, "manual_input", [], SOURCE, node_reference="sales"),
        node(2, "manual_input", [], SOURCE, node_reference="returns"),
        node(3, "filter", [1], FILTER[1], description="Big ones"),
        node(4, "sort", [3], SORT[1]),
        node(5, "sample", [2], {"sample_size": 5, "sample_method": "first"}),
    )


def test_a_call_reads_the_frame_it_is_written_on():
    flow = branches()
    cells = cells_of(flow)
    assert cells["cell-3"].startswith("filtered_3 = sales.filter(")
    result = sync(flow, {"cell-3": cells["cell-3"].replace("sales.filter", "returns.filter")})
    assert changes(result) == {**UNCHANGED, "inputs": {"3": {"main": [2], "right": None, "left": None}}}
    assert cells_of(applied(flow, result))["cell-3"].startswith("filtered_3 = returns.filter(")


def test_a_chosen_name_becomes_the_reference_and_its_readers_keep_reading_the_node():
    flow = branches()
    cells = cells_of(flow)
    result = sync(flow, {"cell-3": cells["cell-3"].replace("filtered_3 =", "big_ones =")})
    assert result["nodes"] == {"3": {"node_reference": "big_ones"}}
    assert result["inputs"] == {}
    renamed = applied(flow, result)
    after = cells_of(renamed)
    assert after["cell-3"].startswith("big_ones = sales.filter(")
    assert "big_ones.sort(" in after["cell-4"]

    back = sync(renamed, {"cell-3": after["cell-3"].replace("big_ones =", "filtered_3 =")})
    assert back["nodes"] == {"3": {"node_reference": None}}


def test_a_name_another_step_has_is_refused_and_an_unusable_one_is_left_with_a_warning():
    flow = branches()
    flow["nodes"][3]["node_reference"] = "sorted_rows"
    cells = cells_of(flow)
    taken = sync(flow, {"cell-3": cells["cell-3"].replace("filtered_3 =", "sorted_rows =")})
    assert (taken["ok"], taken["cell_id"], taken["line"], taken["kind"]) == (False, "cell-3", 1, "error")
    assert "sorted_rows" in taken["message"]

    odd = sync(flow, {"cell-5": cells["cell-5"].replace("sampled_5 =", "Top =")})
    assert odd["ok"] and odd["nodes"] == {}
    assert "`Top`" in odd["warnings"][0]


def test_readers_of_a_name_follow_it_to_the_node_it_names_now():
    """Swapping two calls of a fused cell moves the cell's name to the other node; its readers move too."""
    flow = flow_of(
        node(1, "manual_input", [], SOURCE),
        node(2, "filter", [1], FILTER[1]),
        node(3, "sort", [2], SORT[1]),
        node(4, "sample", [3], {"sample_size": 5, "sample_method": "first"}),
        node(5, "unique", [3], {"unique_input": {"columns": ["product"], "strategy": "first"}}),
    )
    cells = cells_of(flow)
    assert list(cells) == ["imports", "cell-1", "cell-4", "cell-5"]
    lines = cells["cell-1"].split("\n")
    filter_line = next(index for index, line in enumerate(lines) if ".filter(" in line)
    sort_line = next(index for index, line in enumerate(lines) if ".sort(" in line)
    lines[filter_line], lines[sort_line] = lines[sort_line], lines[filter_line]

    result = sync(flow, {"cell-1": "\n".join(lines)})

    assert result["nodes"] == {} and result["warnings"] == []
    assert {node_id: ports["main"] for node_id, ports in result["inputs"].items()} == {"2": [3], "3": [1], "4": [2], "5": [2]}
    after = cells_of(applied(flow, result))
    assert "filtered_2.head(5)" in after["cell-4"] and "filtered_2.unique(" in after["cell-5"]


def test_a_union_takes_the_frames_listed_in_their_order():
    flow = flow_of(
        node(1, "manual_input", [], SOURCE),
        node(2, "manual_input", [], SOURCE),
        node(3, "manual_input", [], SOURCE),
        node(4, "union", [1, 2], {"union_input": {"mode": "relaxed"}}),
    )
    cells = cells_of(flow)
    draft = cells["cell-4"].replace("    source_1,\n    source_2,\n", "    source_3,\n    source_1,\n")
    assert draft != cells["cell-4"]
    result = sync(flow, {"cell-4": draft})
    assert result["nodes"] == {}
    assert result["inputs"] == {"4": {"main": [3, 1], "right": None, "left": None}}
    assert ast.dump(ast.parse(cells_of(applied(flow, result))["cell-4"])) == ast.dump(ast.parse(draft))


# What the browser cannot keep fails on its line


def refusal(flow: dict, cell_id: str, old: str, new: str) -> tuple[str, int | None, str]:
    result, _ = edited(flow, cell_id, old, new)
    assert not result["ok"], result
    assert result["cell_id"] == cell_id
    return result["kind"], result["line"], result["message"]


REFUSALS = [
    ("a removed step", chain(FILTER, SORT), '.sort(["revenue"], descending=[True])', "", "refused", "removes a step"),
    ("a method with no handler yet", chain(SORT), '.sort(["revenue"], descending=[True])', '.drop(["revenue"])', "refused", "`drop`"),
    ("a computed column in a select", chain(SELECT), 'ff.col("revenue").cast(ff.Int64)', 'ff.col("revenue") * 2', "refused", "with_columns()"),
    ("a cast the select node cannot make", chain(SELECT), "cast(ff.Int64)", "cast(ff.Int16)", "refused", "casts to"),
    ("a column listed twice", chain(SELECT), 'ff.col("revenue").cast(ff.Int64)', 'ff.col("product")', "refused", "listed twice"),
    ("a setting the node has no place for", chain(SORT), "descending=[True]", "descending=[True], nulls_last=True", "refused", "nulls_last"),
    ("an expression where names go", chain(SORT), 'sort(["revenue"]', 'sort([ff.col("revenue")]', "refused", "column names"),
    ("a condition over two columns", chain(FILTER), 'ff.col("revenue") > 60', '(ff.col("revenue") > 60) | (ff.col("product") == "x")', "refused", "single comparison"),
    ("a formula as an expression", chain(("formula", {"functions": [{"field": {"name": "double", "data_type": "String"}, "function": "[revenue] * 2"}]})), "with_columns(flowfile_formulas=['[revenue] * 2'], output_column_names=['double'], output_column_datatypes=['String'])", 'with_columns((ff.col("revenue") * 3).alias("double"))', "refused", "expression"),
    ("a frame built inside a call", flow_of(node(1, "manual_input", [], SOURCE), node(2, "manual_input", [], SOURCE), node(3, "union", [1, 2], {"union_input": {"mode": "relaxed"}})), "    source_2,\n", "    source_2.head(1),\n", "refused", "line of its own"),
    ("a method outside the dialect", chain(SORT), '.sort(["revenue"], descending=[True])', ".collect()", "needs_kernel", "collect"),
    ("python that is not flow code", chain(SORT), "ordered_2 =", "print('x')\nordered_2 =", "needs_kernel", "browser runs no Python"),
    ("a name that is not bound", named(SORT), "sales.sort", "nothing.sort", "error", "NameError"),
    ("a wrong argument", chain(SORT), "descending=[True]", "descending=[True, False]", "error", "descending"),
    ("a syntax error", chain(SORT), "descending=[True])", "descending=[True]", "error", "SyntaxError"),
    ("a grouped row number", chain(("record_id", {"record_id_input": {"output_column_name": "id", "offset": 1}})), "offset=1", 'offset=1, group_by=["product"]', "refused", "per group"),
    ("another union mode", flow_of(node(1, "manual_input", [], SOURCE), node(2, "manual_input", [], SOURCE), node(3, "union", [1, 2], {"union_input": {"mode": "relaxed"}})), "how='diagonal_relaxed'", "how='vertical'", "refused", "diagonal_relaxed"),
]


@pytest.mark.parametrize(("label", "flow", "old", "new", "kind", "says"), REFUSALS, ids=[each[0] for each in REFUSALS])
def test_a_change_the_browser_cannot_keep_fails_and_changes_nothing(label, flow, old, new, kind, says):
    cell_id = next(cell_id for cell_id, code in cells_of(flow).items() if old in code)
    got_kind, line, message = refusal(flow, cell_id, old, new)
    assert got_kind == kind, message
    assert says in message, message
    assert line is None or line >= 1


def test_a_failure_names_the_line_it_happened_on():
    flow = chain(FILTER, SORT)
    code = cells_of(flow)["cell-1"]
    lines = code.split("\n")
    target = next(index for index, line in enumerate(lines, start=1) if ".sort(" in line)
    result = sync(flow, {"cell-1": code.replace("descending=[True]", "descending=[True], nulls_last=True")})
    assert (result["cell_id"], result["line"]) == ("cell-1", target)


def test_placeholder_cells_and_stale_cells_are_refused():
    flow = chain(SORT)
    locked = sync_notebook(copy.deepcopy(flow), schemas_of(flow), {2: "locked until trusted"}, {"cell-2": "x = 1"})
    assert (locked["ok"], locked["kind"]) == (False, "refused")
    assert "stays on the canvas" in locked["message"]

    gone = sync(flow, {"cell-9": "x = 1"})
    assert (gone["ok"], gone["cell_id"], gone["kind"]) == (False, "cell-9", "error")


def test_the_imports_cell_binds_only_the_modules_of_the_dialect():
    flow = chain(SORT)
    assert sync(flow, {"imports": "import flowfile as ff\nimport datetime"})["ok"]
    refused = sync(flow, {"imports": "import flowfile as ff\nimport os"})
    assert (refused["ok"], refused["cell_id"], refused["line"], refused["kind"]) == (False, "imports", 2, "needs_kernel")


# No cell text is ever executed

TAIL = '.sort(["revenue"], descending=[True])'
HOSTILE = [
    "ordered_3 = __import__('os').system('touch {canary}')",
    "import os\nos.system('touch {canary}')",
    "ordered_3 = sales.filter(open('{canary}', 'w').write('x'))" + TAIL,
    "ordered_3 = sales.filter(ff.col(str(exec(\"open('{canary}', 'w')\"))) > 1)" + TAIL,
    "def _polars_code_2(input_df):\n    open('{canary}', 'w')\n_polars_code_2(None)",
    "class A:\n    open('{canary}', 'w')",
    "ordered_3 = sales.filter(ff.col('revenue') > 60).sort(['revenue'], descending=[eval(\"open('{canary}', 'w')\")])",
    "ordered_3 = sales.filter(flowfile_formula=\"__import__('os').system('touch {canary}')\")" + TAIL,
    "[open('{canary}', 'w') for _ in (1,)]",
    "ordered_3 = (lambda: open('{canary}', 'w'))()",
    "ordered_3 = sales.filter(ff.col('revenue') > 60).__class__",
    "ordered_3 = sales.filter(ff.col('revenue') > 60).sort(**{{'by': open('{canary}', 'w')}})",
]


@pytest.mark.parametrize("code", HOSTILE)
def test_no_cell_text_reaches_exec_eval_or_compile(code, tmp_path, monkeypatch):
    """Whatever a cell says, reading it calls no ``exec`` or ``eval`` and compiles nothing past an AST."""
    flow = named(FILTER, SORT)
    canary = tmp_path / "canary"
    calls: list[str] = []
    originals = {name: getattr(builtins, name) for name in ("exec", "eval", "compile")}

    def exec_(source, *args, **kwargs):
        calls.append("exec")
        return originals["exec"](source, *args, **kwargs)

    def eval_(source, *args, **kwargs):
        calls.append("eval")
        return originals["eval"](source, *args, **kwargs)

    def compile_(source, filename, mode, flags=0, *args, **kwargs):
        if not flags & ast.PyCF_ONLY_AST:
            calls.append("compile")
        return originals["compile"](source, filename, mode, flags, *args, **kwargs)

    render_notebook(copy.deepcopy(flow), schemas_of(flow))
    monkeypatch.setattr(builtins, "exec", exec_)
    monkeypatch.setattr(builtins, "eval", eval_)
    monkeypatch.setattr(builtins, "compile", compile_)
    result = sync(flow, {"cell-2": code.format(canary=canary)})
    monkeypatch.undo()

    assert calls == []
    assert not canary.exists()
    if "flowfile_formula" in code:
        assert result["nodes"]["2"]["settings"]["filter_input"]["advanced_filter"].startswith("__import__")
    else:
        assert not result["ok"]


def test_the_module_names_no_way_to_run_code():
    source = (Path(__file__).parents[2] / "src/pyodide/engine/notebook_cells.py").read_text()
    names = {node.id for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Name)}
    assert not names & {"exec", "eval", "compile", "__import__", "globals", "locals", "importlib"}
