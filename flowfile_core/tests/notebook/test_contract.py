"""No cell reaches ``exec``, ``eval`` or ``compile`` in core, and the call sites of those builtins do not grow.

Every corpus flow is rendered, then planned with every cell edited (fingerprint, snapshot, the installed
runner's clean run, refusals, reconcile: what ``POST /notebook/plan`` does), one of them with a cell added that
calls the input-only frame methods and ``ff.LazyFrame`` / ``ff.DataFrame``, twice: a warm-up that loads every
lazy import and cache, then again with ``builtins.exec``, ``eval`` and ``compile`` replaced by recorders that
delegate to the originals. Parsing (``compile`` with ``ast.PyCF_ONLY_AST``, which is how ``ast.parse`` reads a
cell) is allowed; any other call on the planning thread must come from a pinned site, none may come from the
notebook package or the frame's ``exec`` executor, untyped formula or function-object Python Script paths, and
no call on any thread may receive cell text (a line of a cell, an AST unparsing to one, or a code object
compiled from a cell). The static tests pin that the notebook package calls none of those builtins and never names
the ``exec`` executor, and count every ``exec``/``eval``/``compile`` call site in core, the frame and ``shared``.
"""

import ast
import builtins
import sys
import threading
import types
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

import flowfile_core.notebook
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook import bridge
from flowfile_core.notebook.interpret import CellInterpreter
from flowfile_core.notebook.push import NotebookPushRequest, plan_push, plan_response
from flowfile_core.notebook.render import render
from flowfile_core.notebook.runner import NotebookRunner, install_notebook_runner
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, cell_provenance, masked_payload
from tests.notebook.test_input_only import EDITED, FRAMES, SOURCE, edited_cell

REPO = Path(__file__).resolve().parents[3]
BUILTINS = ("exec", "eval", "compile")
PINNED = {
    ("flowfile_core.flowfile.code_generator.code_generator", "_eval_in_validation_namespace"): (
        "reconcile compares formulas through the translator; evaluates its generated code with no builtins"
    ),
    ("polars_expr_transformer.process.polars_expr_transformer", "_validate_polars_code"): (
        "the formula translator checks its own generated code, after its dialect check"
    ),
    ("shared.node_designer.loading", "load_node_module"): "loads an installed custom node file, never cell text",
}
NEVER = {
    ("flowfile_frame.notebook_cells", "_compile"): "the exec executor's compile",
    ("flowfile_frame.notebook_cells", "exec_cell"): "the exec executor",
    ("flowfile_frame.python_script", "_notebook_cells"): "a Python Script built from a function object",
    ("flowfile_frame.flow_frame", "_try_translate_flowfile_formulas"): "untyped formulas the frame translates",
    ("flowfile_frame.selectors", "Selector.expr"): "a selector's repr evaluated",
}
NOTEBOOK_PACKAGE = "flowfile_core.notebook"
MIN_LINE = 16


@dataclass
class Call:
    builtin: str
    source: Any
    filename: Any
    ast_only: bool
    scope: Any
    caller: tuple[str, str]
    stack: tuple[tuple[str, str], ...]
    thread: int


def _defined_qualname(code: types.CodeType, module_globals: dict[str, Any]) -> str:
    """The qualname of the module function or class member whose code is ``code``, else its bare name.

    Python 3.10 code objects have no ``co_qualname``, so the function is found in its module instead,
    looking only at functions, properties and static or class methods (never an arbitrary ``getattr``).
    """
    for value in list(module_globals.values()):
        for candidate in (value, *(vars(value).values() if isinstance(value, type) else ())):
            if isinstance(candidate, property):
                candidate = candidate.fget
            elif isinstance(candidate, staticmethod | classmethod):
                candidate = candidate.__func__
            if isinstance(candidate, types.FunctionType) and candidate.__code__ is code:
                return candidate.__qualname__
    return code.co_name


def _qualname(frame) -> str:
    qualname = getattr(frame.f_code, "co_qualname", None)
    return qualname if qualname is not None else _defined_qualname(frame.f_code, frame.f_globals)


def _frames(frame) -> tuple[tuple[str, str], ...]:
    stack = []
    while frame is not None:
        stack.append((frame.f_globals.get("__name__", "?"), _qualname(frame)))
        frame = frame.f_back
    return tuple(stack)


@contextmanager
def recording_builtins() -> Iterator[list[Call]]:
    """Replace ``exec``, ``eval`` and ``compile`` with recorders that delegate to the originals."""
    calls: list[Call] = []
    originals = {name: getattr(builtins, name) for name in BUILTINS}

    def record(name: str, source: Any, filename: Any, flags: int, scope: Any = None) -> None:
        stack = _frames(sys._getframe(2))
        ast_only = name == "compile" and bool(flags & ast.PyCF_ONLY_AST)
        calls.append(Call(name, source, filename, ast_only, scope, stack[0], stack, threading.get_ident()))

    def scope(globals_, locals_):
        if globals_ is None:
            caller = sys._getframe(2)
            return caller.f_globals, caller.f_locals if locals_ is None else locals_
        return globals_, locals_

    def exec_(source, globals_=None, locals_=None, /, **kwargs):
        record("exec", source, None, 0, globals_)
        return originals["exec"](source, *scope(globals_, locals_), **kwargs)

    def eval_(source, globals_=None, locals_=None, /):
        record("eval", source, None, 0, globals_)
        return originals["eval"](source, *scope(globals_, locals_))

    def compile_(source, filename, mode, flags=0, dont_inherit=False, optimize=-1, **kwargs):
        record("compile", source, filename, flags)
        return originals["compile"](source, filename, mode, flags, dont_inherit, optimize, **kwargs)

    replacements = {"exec": exec_, "eval": eval_, "compile": compile_}
    for name in BUILTINS:
        setattr(builtins, name, replacements[name])
    try:
        yield calls
    finally:
        for name, original in originals.items():
            setattr(builtins, name, original)


def _cell_lines(cells: list[tuple[str, str]]) -> set[str]:
    """Every stripped cell line with at least ``MIN_LINE`` non-space characters (shorter ones are too generic)."""
    return {line.strip() for _, code in cells for line in code.splitlines() if len("".join(line.split())) >= MIN_LINE}


def _from_a_cell(call: Call, lines: set[str]) -> bool:
    """Whether ``call`` received cell text: a cell line, an AST unparsing to one, or code compiled from a cell."""
    if isinstance(call.filename, str) and call.filename.startswith("<cell-"):
        return True
    source = call.source
    if isinstance(source, types.CodeType):
        return source.co_filename.startswith("<cell-")
    if isinstance(source, ast.AST):
        source = ast.unparse(source)
    if isinstance(source, bytes | bytearray):
        source = bytes(source).decode("utf-8", "replace")
    return isinstance(source, str) and any(line in source for line in lines)


def _requests(notebook_corpus) -> list[tuple[str, Any, NotebookPushRequest]]:
    """Every corpus flow's rendered cells as a plan request with every cell edited (rendering is not the sync);
    the first flow's again with a cell added that calls every input-only frame method needing no connection and
    builds a frame from data with ``ff.LazyFrame`` / ``ff.DataFrame``."""
    requests = []
    frames = "\n".join(f"frame_{name} = {call}" for name, (call, _, _) in FRAMES.items())
    edited = edited_cell(name for name in EDITED if name != "write_database")
    added = ("cell-input-only", f"{SOURCE}\n{edited}\n{frames}")
    targets = [(name, graph, []) for name, graph in notebook_corpus]
    targets.append((f"{notebook_corpus[0][0]}+input_only", notebook_corpus[0][1], [added]))
    for name, graph, extra in targets:
        rendering = render(graph)
        cells = [(cell.cell_id, cell.code) for cell in rendering.cells] + extra
        request = NotebookPushRequest(
            flow_id=graph.flow_id,
            cells=cells,
            changed_cell_ids=[cell_id for cell_id, _ in cells],
            provenance=cell_provenance(graph, rendering),
            code_fingerprint=rendering.code_fingerprint,
            client_max_node_id=max((node.node_id for node in graph.nodes), default=0),
        )
        requests.append((name, graph, request))
    return requests


def _plan_all(requests) -> dict[str, Any]:
    """Each request planned as the plan route plans it: the plan and payload, or the refusal."""
    owner = PydanticUser(username="nb_contract", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    outcomes = {}
    for name, graph, request in requests:
        try:
            plan, result = plan_push(graph, owner, request)
        except HTTPException as exc:
            outcomes[name] = (exc.status_code, exc.detail)
            continue
        response = plan_response(plan, result).model_dump(mode="json")
        outcomes[name] = (200, masked_payload(response), masked_payload(result.flowfile_data))
    return outcomes


@pytest.fixture(scope="module")
def recorded_plans(notebook_corpus):
    """The corpus planned twice through the runner ``main.py`` installs; the second pass under the recorders."""
    before = bridge._runner
    install_notebook_runner()
    try:
        runner = bridge.get_clean_runner()
        assert type(runner) is NotebookRunner and NotebookRunner.executor is CellInterpreter
        requests = _requests(notebook_corpus)
        warm = _plan_all(requests)
        with recording_builtins() as calls:
            again = _plan_all(requests)
    finally:
        bridge.set_clean_runner(before)
    sent = [cell for _, _, request in requests for cell in request.cells]
    return warm, again, sent, calls, threading.get_ident()


def test_a_plan_hands_no_cell_text_to_exec_eval_or_compile(recorded_plans):
    warm, again, sent, calls, thread = recorded_plans
    refused = {name: outcome for name, outcome in again.items() if outcome[0] != 200}
    assert not refused, refused
    assert again == warm
    parsed = [c for c in calls if c.ast_only and isinstance(c.filename, str) and c.filename.startswith("<cell-")]
    assert parsed, "the recorders saw no cell being parsed, so they did not see the sync"
    lines = _cell_lines(sent)
    from_cells = [(c.builtin, c.caller, c.stack[:8]) for c in calls if not c.ast_only and _from_a_cell(c, lines)]
    assert not from_cells, from_cells
    running = [c for c in calls if not c.ast_only and c.thread == thread]
    from_the_notebook = [(c.builtin, c.caller) for c in running if c.caller[0].startswith(NOTEBOOK_PACKAGE)]
    assert not from_the_notebook, from_the_notebook
    never = [(c.builtin, c.caller, NEVER[c.caller]) for c in running if c.caller in NEVER]
    assert not never, never
    unpinned: dict[tuple[str, tuple[str, str]], tuple] = {}
    for call in running:
        if call.caller not in PINNED:
            unpinned.setdefault((call.builtin, call.caller), call.stack[:8])
    assert not unpinned, unpinned
    formula_site = ("flowfile_core.flowfile.code_generator.code_generator", "_eval_in_validation_namespace")
    assert formula_site in {c.caller for c in running}


def test_a_pinned_eval_sees_one_generated_expression_without_builtins(recorded_plans):
    _, _, _, calls, _ = recorded_plans
    evaluated = [c for c in calls if c.builtin == "eval" and c.caller in PINNED]
    assert evaluated
    for call in evaluated:
        nodes = list(ast.walk(ast.parse(call.source, mode="eval")))
        names = [n.attr for n in nodes if isinstance(n, ast.Attribute)] + [
            n.id for n in nodes if isinstance(n, ast.Name)
        ]
        assert not [name for name in names if name.startswith("__")], call.source
        if call.caller[1] == "_eval_in_validation_namespace":
            assert call.scope == {"__builtins__": {}}, call.source


def _name_calls(path: Path, names: tuple[str, ...]) -> list[tuple[int, str, str]]:
    """``(line, name, enclosing qualname)`` of every call to a bare ``name``, never an attribute (``re.compile``)."""
    found: list[tuple[int, str, str]] = []

    def visit(node: ast.AST, scope: list[str]) -> None:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in names:
            found.append((node.lineno, node.func.id, ".".join(scope) or "<module>"))
        inner = (
            scope + [node.name] if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) else scope
        )
        for child in ast.iter_child_nodes(node):
            visit(child, inner)

    visit(ast.parse(path.read_text(encoding="utf-8"), str(path)), [])
    return found


def _package_sites(package: str) -> list[tuple[str, int, str, str]]:
    base = REPO / package
    rows = []
    for path in sorted(base.rglob("*.py")):
        if {"tests", "__pycache__"} & set(path.relative_to(base).parts[:-1]):
            continue
        rows.extend((str(path.relative_to(REPO)), *site) for site in _name_calls(path, BUILTINS))
    return rows


RATCHET = {"flowfile_core/flowfile_core": 6, "flowfile_frame/flowfile_frame": 10, "shared": 2}


def test_exec_eval_compile_call_sites_do_not_grow():
    """Calls to the bare names ``exec``, ``eval`` and ``compile`` outside tests; each package may only lose some."""
    sites = {package: _package_sites(package) for package in RATCHET}
    grown = {package: rows for package, rows in sites.items() if len(rows) > RATCHET[package]}
    assert not grown, grown
    assert sum(len(rows) for rows in sites.values()) <= sum(RATCHET.values())


def _module_of(file: str) -> str:
    parts = Path(file).with_suffix("").parts
    return ".".join(parts[1:] if parts[0] in ("flowfile_core", "flowfile_frame") else parts)


def test_pinned_and_never_sites_are_real_call_sites():
    real = {(_module_of(file), where) for package in RATCHET for file, _, _, where in _package_sites(package)}
    repo_pinned = {site for site in PINNED if not site[0].startswith("polars_expr_transformer")}
    assert repo_pinned <= real, repo_pinned - real
    assert set(NEVER) <= real, set(NEVER) - real
    assert not set(NEVER) & set(PINNED)


def _notebook_modules() -> list[Path]:
    return sorted(Path(flowfile_core.notebook.__file__).parent.glob("*.py"))


CODE_RUNNING_MODULES = {"importlib", "runpy", "code", "codeop"}


def test_the_notebook_package_calls_no_exec_eval_or_compile():
    """No call to those builtins, ``__import__`` or ``breakpoint``; ``builtins`` only as a name list; ``importlib``
    only for ``importlib.util.find_spec``; nothing from the modules that run code."""
    forbidden_calls = (*BUILTINS, "__import__", "breakpoint")
    for path in _notebook_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        assert not _name_calls(path, forbidden_calls), path.name
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                assert node.value.id != "builtins", (path.name, node.lineno, node.attr)
                if node.value.id == "importlib":
                    assert node.attr == "util", (path.name, node.lineno)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute):
                if isinstance(node.value.value, ast.Name) and node.value.value.id == "importlib":
                    assert node.attr == "find_spec", (path.name, node.lineno)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert alias.name == "importlib.util" or top not in CODE_RUNNING_MODULES, (path.name, alias.name)
            if isinstance(node, ast.ImportFrom):
                top = (node.module or "").split(".")[0]
                assert top not in CODE_RUNNING_MODULES | {"builtins"}, (path.name, node.module)


EXEC_EXECUTOR_NAMES = {"exec_cell", "execute_cell", "seed_session", "_compile", "EXEC_CELLS"}


def test_core_never_names_the_exec_executor():
    """Core imports from ``notebook_cells`` only the snapshot session and ``clean_run`` (in the runner, with the
    runner's own executor), and never the ``exec`` executor, ``execute_cell`` or a canvas seed."""
    clean_run_calls = []
    for path in sorted((REPO / "flowfile_core" / "flowfile_core").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert "flowfile_frame.notebook_cells" not in {alias.name for alias in node.names}, path
            if isinstance(node, ast.ImportFrom) and node.module == "flowfile_frame.notebook_cells":
                names = {alias.name for alias in node.names}
                assert not names & EXEC_EXECUTOR_NAMES, (path, names)
                if "clean_run" in names:
                    assert path.name == "runner.py" and path.parent.name == "notebook", path
            if isinstance(node, ast.ImportFrom) and node.module == "flowfile_frame":
                assert not {alias.name for alias in node.names} & {"notebook_cells"}, path
            if isinstance(node, ast.Attribute) and node.attr in EXEC_EXECUTOR_NAMES:
                pytest.fail(f"{path}:{node.lineno} names {node.attr}")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "clean_run":
                clean_run_calls.append((path, node))
    assert clean_run_calls
    for path, call in clean_run_calls:
        executor = {keyword.arg: keyword.value for keyword in call.keywords}.get("executor")
        assert executor is not None, path
        assert ast.unparse(executor) == "self.executor()", (path, ast.unparse(executor))
    assert NotebookRunner.executor is CellInterpreter


EXEC_CELL_SITES = {
    "flowfile_frame/flowfile_frame/notebook_cells.py",
    "flowfile_frame/flowfile_frame/notebook_kernel.py",
}


def _names(tree: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.alias):
            names.add(node.name)
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            names.add(node.name)
    return names


def test_only_the_kernel_session_names_the_exec_executor():
    """Outside tests, ``exec_cell`` is named only where it is defined and by the frame's kernel session module."""
    naming = set()
    for package in (*RATCHET, "flowfile/flowfile"):
        base = REPO / package
        for path in sorted(base.rglob("*.py")):
            if {"tests", "__pycache__"} & set(path.relative_to(base).parts[:-1]):
                continue
            if "exec_cell" in _names(ast.parse(path.read_text(encoding="utf-8"), str(path))):
                naming.add(path.relative_to(REPO).as_posix())
    assert naming == EXEC_CELL_SITES, naming
