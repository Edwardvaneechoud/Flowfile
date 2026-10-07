"""The notebook render on the browser's pinned versions.

``notebook_golden.json`` holds what flowfile_core renders for each fixture flow and what it makes
of each formula; the vitest suite ``notebook-render-parity`` writes it and checks it against a real
flowfile_core. Replaying it here proves the engine gives the same cells on the Polars, pydantic
and formula package the browser runs.
"""

import ast
import builtins
import copy
import json
import sys
from pathlib import Path

import pytest
from engine.notebook_formulas import translate_to_ff_code
from engine.notebook_interpret import interprets_expression
from engine.notebook_render import render_notebook

GOLDEN = json.loads((Path(__file__).parent / "notebook_golden.json").read_text())
FLOWS = {flow["name"]: flow for flow in GOLDEN["flows"]}


def _render(name: str, locked: dict | None = None) -> list[dict]:
    flow = FLOWS[name]
    return render_notebook(copy.deepcopy(flow["flow"]), flow["schemas"], locked or {})["cells"]


@pytest.mark.parametrize("name", list(FLOWS))
def test_renders_the_cells_flowfile_core_renders(name):
    assert _render(name) == FLOWS[name]["cells"]


@pytest.mark.parametrize("formula", list(GOLDEN["formulas"]))
def test_reads_a_formula_the_way_flowfile_core_does(formula):
    assert translate_to_ff_code(formula) == GOLDEN["formulas"][formula]


LOCKED_FLOW = "notebook: polars code assigning output_df"


def test_a_locked_node_renders_as_a_placeholder_that_passes_its_frame_on():
    cells = {cell["cell_id"]: cell for cell in _render(LOCKED_FLOW, {2: "locked until trusted"})}
    assert cells["cell-2"]["status"] == "placeholder"
    assert cells["cell-2"]["reason"] == "locked until trusted"
    assert cells["cell-2"]["code"] == "transformed_2 = ff.canvas_node(2, source_1)  # Polars code: locked until trusted"
    assert cells["cell-3"] == FLOWS[LOCKED_FLOW]["cells"][3]


def test_a_reason_cannot_add_a_line_to_its_cell():
    cells = _render(LOCKED_FLOW, {2: "not supported\nimport os"})
    assert cells[2]["code"] == "transformed_2 = ff.canvas_node(2, source_1)  # Polars code: not supported import os"


def test_a_locked_node_never_shows_its_settings():
    flow = copy.deepcopy(FLOWS[LOCKED_FLOW])
    code_node = next(node for node in flow["flow"]["nodes"] if node["id"] == 2)
    code_node["setting_input"]["polars_code_input"]["polars_code"] = "output_df = CANARY_7c1e"
    rendering = render_notebook(flow["flow"], flow["schemas"], {2: "locked until trusted"})
    assert "CANARY_7c1e" not in json.dumps(rendering)


@pytest.mark.parametrize(
    "code",
    [
        'ff.col("a") + 1',
        '(ff.col("a") > 1) & (ff.col("b") < 2)',
        'ff.col("s").str.to_uppercase()',
        'ff.col("s").str.contains("W")',
        'ff.when(ff.col("a") > 30).then(ff.lit("S")).otherwise(ff.lit("J"))',
        'ff.col("a").cast(ff.Int64)',
        'ff.col("d").dt.year()',
    ],
)
def test_reads_an_expression_in_the_dialect(code):
    assert interprets_expression(code)


@pytest.mark.parametrize(
    "code",
    [
        '__import__("os").system("true")',
        'open("/etc/passwd")',
        'ff.col("a").map_elements(lambda value: value)',
        'ff.col("a").__class__',
        'ff.col("s").str.contains(ff.col("t"))',
        'ff.read_database("select 1")',
        'eval("1")',
        "os.getcwd()",
        'ff.col("a") if True else ff.col("b")',
        '[ff.col(name) for name in ("a", "b")]',
        'ff.col("a") ** 2',
        "1 +",
    ],
)
def test_refuses_an_expression_outside_the_dialect(code):
    assert not interprets_expression(code)


EVAL_SITE = ("polars_expr_transformer.process.polars_expr_transformer", "_validate_polars_code")
EVAL_NAMES = {"pl", "datetime", "hashlib"}


def test_rendering_executes_no_node_text(monkeypatch):
    """A render calls no ``exec`` and compiles only to an AST.

    The one ``eval`` is the formula package checking code it generated itself, after its own
    dialect check: each is a single expression whose free names are ``pl``, ``datetime`` and ``hashlib``.
    """
    for name in FLOWS:
        _render(name)
    translate_to_ff_code.cache_clear()

    calls: list[tuple[str, tuple[str, str], str]] = []
    originals = {name: getattr(builtins, name) for name in ("exec", "eval", "compile")}

    def record(name: str, source) -> None:
        caller = sys._getframe(2)
        calls.append((name, (caller.f_globals.get("__name__", ""), caller.f_code.co_name), str(source)))

    def exec_(source, *args, **kwargs):
        record("exec", source)
        return originals["exec"](source, *args, **kwargs)

    def eval_(source, *args, **kwargs):
        record("eval", source)
        return originals["eval"](source, *args, **kwargs)

    def compile_(source, filename, mode, flags=0, *args, **kwargs):
        if not flags & ast.PyCF_ONLY_AST:
            record("compile", source)
        return originals["compile"](source, filename, mode, flags, *args, **kwargs)

    monkeypatch.setattr(builtins, "exec", exec_)
    monkeypatch.setattr(builtins, "eval", eval_)
    monkeypatch.setattr(builtins, "compile", compile_)
    for name in FLOWS:
        assert _render(name) == FLOWS[name]["cells"]
    for formula, expected in GOLDEN["formulas"].items():
        assert translate_to_ff_code(formula) == expected
    monkeypatch.undo()

    assert calls, "the formula package no longer validates by eval: drop the pinned site"
    assert {(name, site) for name, site, _ in calls} == {("eval", EVAL_SITE)}
    for _, _, source in calls:
        nodes = list(ast.walk(ast.parse(source, mode="eval")))
        bound = {arg.arg for node in nodes if isinstance(node, ast.Lambda) for arg in node.args.args}
        assert {node.id for node in nodes if isinstance(node, ast.Name)} - bound <= EVAL_NAMES, source
        assert not any(isinstance(node, ast.Attribute) and node.attr.startswith("__") for node in nodes), source
