"""Source-text entry points: a ``def`` read from text gives the node code its function object gives.

A notebook cell interpreted in core has no function object, so ``_polars_code_text`` and
``PythonScriptFunction._from_source`` derive from the source text (AST, ``symtable``,
``inspect.getblock``) what ``_polars_code_source`` and ``_notebook_cells`` derive from the compiled
function. These tests pin that the two give byte-identical code, the same parameters and the same
errors, and which module names, in which order, a body's prelude is built from.
"""

from __future__ import annotations

import ast
import importlib
import linecache
import sys
import textwrap
import types

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_frame.flow_frame import _polars_code_source, _polars_code_text, _PolarsCodeText
from flowfile_frame.native import NativeNodeError
from flowfile_frame.python_script import (
    PythonScriptFunction,
    _function_source,
    _is_raw,
    _module_reads,
    _notebook_cells,
    _parameters,
    _source_cells,
)

_CELLS = iter(range(10**6))


def _define(code: str, namespace: dict | None = None) -> dict:
    """Run ``code`` as a notebook cell would (registered in ``linecache``) and return its namespace."""
    filename = f"<text-entry-{next(_CELLS)}>"
    linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)
    namespace = {"__name__": "__main__", "ff": ff, "pl": pl, **(namespace or {})}
    exec(compile(code, filename, "exec", dont_inherit=True), namespace)
    return namespace


POLARS_CODE_DEFS = [
    "def f(input_df):\n    return input_df.head(1)\n",
    "def f(input_df: ff.FlowFrame):\n    return input_df.with_columns(\n        \n        a=pl.lit(1),\n    )\n",
    "def f(input_df_1, input_df_2):\n    output_df = input_df_1.join(input_df_2, how='cross')\n    return output_df\n",
    "def f(input_df):\n    # keep the first rows\n    # of the frame\n    x = input_df.head(2)\n    return x\n",
    "def f(input_df): return input_df.head(3)\n",
    "def f():\n    output_df = pl.LazyFrame({'a': [1, 2]})\n    return output_df\n",
]


@pytest.mark.parametrize("code", POLARS_CODE_DEFS)
def test_a_polars_code_def_read_as_text_stores_what_the_function_stores(code):
    fn = _define(code)["f"]
    text = _polars_code_text(textwrap.dedent(code))
    assert text == _polars_code_source(fn)
    assert _polars_code_source(_PolarsCodeText(text)) == text


SCRIPT_CELLS = {
    "prelude_order": (
        "import json\nimport polars as pl\nLIMIT = 3\nLABEL = 'x'\n\n\n"
        "@ff.python_script(outputs=['main'])\n"
        "def script(orders):\n"
        '    """Two imports and two constants, used out of their written order."""\n'
        "    tag = LABEL * 2\n"
        "    rows = orders.collect().head(LIMIT)\n"
        "    # %% [markdown] Notes\n"
        "    # some words\n"
        "    # %% Build\n"
        "    return pl.DataFrame({'tag': [tag], 'n': [json.dumps(rows.height)]})\n"
    ),
    "one_line": "@ff.python_script\ndef script(orders): return orders\n",
    "dict_return": (
        "@ff.python_script(outputs=['a', 'b'])\n" "def script(left, right):\n" "    return {'a': left, 'b': right}\n"
    ),
    "comprehension_and_handler": (
        "import polars as pl\nimport json\nSCALE = 2\n\n\n"
        "@ff.python_script\n"
        "def script(orders):\n"
        "    try:\n"
        "        rows = [json.loads(v) for v in orders.collect()['a'].to_list() if v]\n"
        "    except ValueError:\n"
        "        rows = [SCALE]\n"
        "    return pl.DataFrame({'a': rows})\n"
    ),
    "inner_function_and_lambda": (
        "import polars as pl\nOFFSET = 1\n\n\n"
        "@ff.python_script\n"
        "def script(orders):\n"
        "    def shift(v):\n"
        "        return v + OFFSET\n"
        "    frame = orders.collect()\n"
        "    return frame.with_columns(pl.col('a').map_elements(lambda v: shift(v), return_dtype=pl.Int64))\n"
    ),
    "loop_exits_and_async_helper": (
        "@ff.python_script\n"
        "def script(orders):\n"
        "    for _ in range(2):\n"
        "        if orders is None:\n"
        "            continue\n"
        "        break\n"
        "    else:\n"
        "        orders = orders\n"
        "    async def fetch():\n"
        "        return [row async for row in fetch()] + [await fetch()]\n"
        "    return orders\n"
    ),
    "script_as_written": (
        "import polars as pl\nLIMIT = 3\n\n\n"
        "@ff.python_script(outputs=['kept', 'rest'])\n"
        "def script():\n"
        '    """Reads and publishes itself."""\n'
        "    rows = flowfile_ctx.read_input().collect()\n"
        "    # %% Publish\n"
        "    flowfile_ctx.publish_output(rows.head(LIMIT), 'kept')\n"
        "    flowfile_ctx.publish_output(pl.DataFrame(), 'rest')\n"
    ),
    "script_publishing_in_a_helper": (
        "@ff.python_script\n"
        "def script():\n"
        "    def publish(df):\n"
        "        return flowfile_ctx.publish_output(df)\n"
        "    publish(flowfile_ctx.read_input())\n"
    ),
}

SCRIPT_ERRORS = {
    "default": "@ff.python_script\ndef script(orders=None):\n    return orders\n",
    "varargs": "@ff.python_script\ndef script(*frames):\n    return frames[0]\n",
    "keyword_only": "@ff.python_script\ndef script(a, *, b):\n    return a\n",
    "kernel_name": "@ff.python_script\ndef script(flowfile_ctx):\n    return flowfile_ctx\n",
    "generator": "@ff.python_script\ndef script(orders):\n    yield orders\n    return orders\n",
    "no_return": "@ff.python_script\ndef script(orders):\n    x = orders\n",
    "nested_return": "@ff.python_script\ndef script(orders):\n    if orders:\n        return orders\n    return orders\n",
    "marker_comment": "@ff.python_script\ndef script(orders):\n    # flowfile: inputs\n    return orders\n",
    "marker_in_block": "@ff.python_script\ndef script(orders):\n    if orders:\n        # %%\n        x = 1\n    return orders\n",
    "docstring_line": '@ff.python_script\ndef script(orders):\n    """Doc."""; x = 1\n    return orders\n',
    "file": "@ff.python_script\ndef script(orders):\n    print(__file__)\n    return orders\n",
    "undefined": "@ff.python_script\ndef script(orders):\n    return orders.head(NOT_DEFINED)\n",
    "outside_value": "THING = object()\n\n\n@ff.python_script\ndef script(orders):\n    return orders.head(THING)\n",
    "flowfile_package": "@ff.python_script\ndef script(orders):\n    return orders.filter(ff.col('a') > 1)\n",
    "dict_without_outputs": "@ff.python_script\ndef script(orders):\n    return {'a': orders}\n",
    "script_with_parameters": "@ff.python_script\ndef script(orders):\n    flowfile_ctx.publish_output(orders)\n",
    "script_without_a_publish": "@ff.python_script\ndef script():\n    x = 1\n",
}


COMPILE_ONLY_ERRORS = {
    "break": ("    if orders is None:\n        break\n", "'break' outside loop (break)"),
    "continue": ("    continue\n", "'continue' not properly in loop (continue)"),
    "break_in_loop_else": ("    for _ in range(2):\n        pass\n    else:\n        break\n", "'break' outside loop"),
    "break_in_nested_def": (
        "    for _ in range(2):\n        def inner():\n            break\n",
        "'break' outside loop",
    ),
    "await": ("    await orders\n", "is async or a generator"),
    "async_for": ("    async for row in orders:\n        pass\n", "is async or a generator"),
    "async_with": ("    async with orders:\n        pass\n", "is async or a generator"),
    "async_comprehension": ("    rows = [row async for row in orders]\n", "is async or a generator"),
    "await_in_nested_def": ("    def inner():\n        return await orders\n", "is async or a generator"),
}


@pytest.mark.parametrize("name", sorted(COMPILE_ONLY_ERRORS))
def test_a_script_def_read_as_text_refuses_a_body_its_function_could_not_have(name):
    body, message = COMPILE_ONLY_ERRORS[name]
    code = f"@ff.python_script\ndef script(orders):\n{body}    return orders\n"
    with pytest.raises(SyntaxError):
        compile(code, "<cell>", "exec", dont_inherit=True)
    with pytest.raises(NativeNodeError) as refused:
        _source_cells(code, 1, {}, None)
    assert message in str(refused.value)


def _script(code: str) -> tuple[types.FunctionType, int, dict]:
    namespace = _define(code.replace("@ff.python_script", "@_capture"), {"_capture": _capture})
    fn = next(value for value in namespace.values() if getattr(value, "_captured", False))
    return fn, fn.__code__.co_firstlineno, namespace


def _capture(fn=None, /, **options):
    def mark(function):
        function._captured = True
        function._options = options
        return function

    return mark if fn is None else mark(fn)


@pytest.mark.parametrize("name", sorted(SCRIPT_CELLS))
def test_a_script_def_read_as_text_gives_the_cells_of_its_function(name):
    code = SCRIPT_CELLS[name]
    fn, first_line, namespace = _script(code)
    outputs = fn._options.get("outputs")
    assert _source_cells(code, first_line, namespace, outputs) == _function_parts(fn, outputs)


@pytest.mark.parametrize("name", sorted(SCRIPT_ERRORS))
def test_a_script_def_read_as_text_fails_as_its_function_does(name):
    code = SCRIPT_ERRORS[name]
    fn, first_line, namespace = _script(code)
    with pytest.raises(NativeNodeError) as from_function:
        _parameters(fn, fn.__name__)
        _notebook_cells(fn, fn._options.get("outputs"))
    with pytest.raises(NativeNodeError) as from_text:
        _source_cells(code, first_line, namespace, fn._options.get("outputs"))
    assert str(from_text.value) == str(from_function.value)


def _module_scripts(module_name: str) -> list[tuple[str, PythonScriptFunction]]:
    module = importlib.import_module(module_name)
    return [(name, value) for name, value in vars(module).items() if isinstance(value, PythonScriptFunction)]


@pytest.mark.parametrize("module_name", [f"{__package__}.test_native_python_script"])
def test_every_decorated_test_fixture_reads_the_same_from_its_file(module_name):
    scripts = _module_scripts(module_name)
    assert len(scripts) >= 10
    source = "".join(linecache.getlines(sys.modules[module_name].__file__))
    for name, script in scripts:
        fn = script.fn
        for outputs in (None, script._outputs):
            expected = _outcome(_function_parts, fn, outputs)
            got = _outcome(_source_cells, source, fn.__code__.co_firstlineno, fn.__globals__, outputs)
            assert got == expected, name


def _function_parts(fn, outputs):
    cells = _notebook_cells(fn, outputs)
    return fn.__name__, _parameters(fn, fn.__name__), cells, _is_raw(_function_source(fn, fn.__name__)[1])


def _outcome(build, *args):
    try:
        return build(*args)
    except NativeNodeError as exc:
        return str(exc)


def test_from_source_builds_the_function_s_node_settings():
    code = SCRIPT_CELLS["prelude_order"]
    fn, first_line, namespace = _script(code)
    by_function = PythonScriptFunction(fn, outputs=["main"], kernel="k", description="d")
    by_text = PythonScriptFunction._from_source(
        code, first_line, namespace, outputs=["main"], kernel="k", description="d"
    )
    assert by_text.fn is None and by_text.__name__ == "script"
    assert by_text.cells == by_function.cells
    frame = ff.from_dict({"a": [1]})
    outputs = [script(frame) for script in (by_function, by_text)]
    placed = [out.flow_graph.get_node(out.node_id).setting_input for out in outputs]
    dumped = [p.python_script_input.model_dump(exclude={"cells"}) for p in placed]
    assert dumped[0] == dumped[1]
    assert [c.code for c in placed[0].python_script_input.cells] == [
        c.code for c in placed[1].python_script_input.cells
    ]


def test_from_source_places_a_script_like_its_function():
    """A ``def`` without a ``return`` takes its frames in the call, from text as from the function."""
    code = SCRIPT_CELLS["script_as_written"]
    fn, first_line, namespace = _script(code)
    options = {"outputs": ["kept", "rest"], "kernel": "k"}
    by_function = PythonScriptFunction(fn, **options)
    by_text = PythonScriptFunction._from_source(code, first_line, namespace, **options)
    assert by_text._raw is by_function._raw is True and by_text._parameters == []
    placed = []
    for script in (by_function, by_text):
        left, right = ff.from_dict({"a": [1]}), ff.from_dict({"b": [2]})
        node = script.node(left, right).node
        assert [source.node_id for source in node.node_inputs.main_inputs] == [left.node_id, right.node_id]
        placed.append(node.setting_input)
    assert placed[0].output_names == placed[1].output_names == ["kept", "rest"]
    cells = [[cell.code for cell in settings.python_script_input.cells] for settings in placed]
    assert cells[0] == cells[1] and cells[0][0] == "import polars as pl\nLIMIT = 3"
    dumped = [settings.python_script_input.model_dump(exclude={"cells"}) for settings in placed]
    assert dumped[0] == dumped[1]


MODULE_READS = [
    ("rows = [g(x) for x in it if h(x)]\nreturn len(rows)", ["g", "it", "h", "len"]),
    (
        "try:\n    a()\nexcept E as e:\n    b(e)\nelse:\n    d()\nfinally:\n    h()\nreturn c",
        ["a", "E", "b", "d", "h", "c"],
    ),
    ("cfg[k] = helper(v)\nobj.attr = w\nreturn cfg", ["cfg", "k", "helper", "v", "obj", "w"]),
    (
        "@deco(p)\ndef inner(x: T = dflt) -> R:\n    return w\nreturn inner(o)",
        ["deco", "p", "T", "dflt", "R", "w", "o"],
    ),
    ("g = lambda q=d: q + k\nreturn g", ["d", "k"]),
    ("import numpy as np\nn = 1\nreturn pl.DataFrame({'x': np.arange(LIMIT + n + p1)})", ["pl", "LIMIT"]),
    ("class C(B):\n    x = y\n    z = x\n    def m(self):\n        return q\nreturn C", ["B", "y", "q"]),
    ("value = 1\ndef g():\n    return value + OTHER\nreturn g", ["OTHER"]),
    ("x: T = v\ny: U\nreturn x", ["T", "v", "U"]),
    ("global G\nG += 1\nreturn H", ["G", "H"]),
    ("if False:\n    a()\nreturn b", ["a", "b"]),
]


@pytest.mark.parametrize("body, expected", MODULE_READS)
def test_a_body_reads_its_module_names_in_the_order_they_first_appear(body, expected):
    source = "def f(p1, p2):\n" + textwrap.indent(body, "    ") + "\n"
    assert _module_reads(source, ast.parse(source).body[0]) == expected


def test_a_function_and_its_text_order_the_prelude_as_the_names_first_appear():
    code = (
        "import json\nimport math\n\n\n@ff.python_script\ndef script(orders):\n"
        "    parts = [json.dumps(v) for v in math.modf(2.5)]\n    return orders.head(len(parts))\n"
    )
    fn, first_line, namespace = _script(code)
    assert _notebook_cells(fn)[0] == "import json\nimport math"
    assert _source_cells(code, first_line, namespace, None)[2][0] == "import json\nimport math"
