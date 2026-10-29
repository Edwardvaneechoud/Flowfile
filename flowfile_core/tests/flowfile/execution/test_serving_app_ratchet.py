"""The serving app collects no leaf a backend answers.

A process that serves (the desktop sidecar, the Docker image, the pip server) computes nothing itself: it reads a
result's rows through ``sample``, bounded by the backend, and composes on ``plan_for``'s leaf only to send the plan back
to the backend that holds it. A ``.collect(`` over a ``plan_for`` result would read the held result in the serving
process. Only a process that does not serve, which is the holder, collects that way (a frame's ``collect()`` in
``flowfile_frame``), so the serving app's modules, ``flowfile_core`` and the single-file app in ``flowfile/web``, are
walked for one and the count is asserted to be zero; the failure lists the sites.

A ``.collect(`` is counted when its receiver is a ``plan_for(...)`` call, a chain of calls, attributes or subscripts
over one, or a name bound to such an expression in its scope or an enclosing one. That the serving app resolves no
``local`` is the registry's and placement's: the default backend of a serving process is never ``local``
(``test_registry.py``) and a run placed there is refused before the registry is asked for it (``test_placement.py``).

The check is static: source files are parsed with ``ast`` and nothing from flowfile_core is imported. Run with ``-s``
to print the sites.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.core

_REPO_ROOT = Path(__file__).resolve().parents[4]
_APP_DIRS = (Path("flowfile_core", "flowfile_core"), Path("flowfile", "flowfile", "web"))
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


def _own_nodes(scope: ast.AST):
    """Every node under ``scope`` but those inside a nested scope, which is yielded itself."""
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, _SCOPES):
            stack.extend(ast.iter_child_nodes(node))


def _over_plan_for(node: ast.AST, bound: set[str]) -> bool:
    """Whether ``node`` is a ``plan_for(...)`` call, a chain over one, or a name in ``bound``."""
    while True:
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Attribute):
                return isinstance(node.func, ast.Name) and node.func.id == "plan_for"
            if node.func.attr == "plan_for":
                return True
            node = node.func.value
        elif isinstance(node, ast.Attribute | ast.Subscript):
            node = node.value
        else:
            return isinstance(node, ast.Name) and node.id in bound


def _leaf_collects(tree: ast.AST) -> list[int]:
    """The lines of each ``.collect(`` over a ``plan_for`` result in ``tree``."""
    lines: list[int] = []

    def visit(scope: ast.AST, inherited: set[str]) -> None:
        nodes = list(_own_nodes(scope))
        bound = set(inherited)
        grew = True
        while grew:
            grew = False
            for node in nodes:
                if not isinstance(node, ast.Assign | ast.AnnAssign | ast.NamedExpr) or node.value is None:
                    continue
                if not _over_plan_for(node.value, bound):
                    continue
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and target.id not in bound:
                        bound.add(target.id)
                        grew = True
        for node in nodes:
            func = getattr(node, "func", None)
            if isinstance(node, ast.Call) and isinstance(func, ast.Attribute) and func.attr == "collect":
                if _over_plan_for(func.value, bound):
                    lines.append(node.lineno)
            if isinstance(node, _SCOPES):
                visit(node, bound)

    visit(tree, set())
    return sorted(lines)


def _app_leaf_collects() -> list[tuple[str, int, str]]:
    sites = []
    for directory in _APP_DIRS:
        for path in sorted((_REPO_ROOT / directory).rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            lines = source.splitlines()
            for line in _leaf_collects(ast.parse(source, filename=str(path))):
                sites.append((path.relative_to(_REPO_ROOT).as_posix(), line, lines[line - 1].strip()))
    return sites


@pytest.mark.parametrize(
    ("source", "lines"),
    [
        ("backend.plan_for(ref).collect()", [1]),
        ("plan = backend.plan_for(ref).head(5)\nrows = plan\nrows.collect(engine='streaming')", [3]),
        ("def read(backend, ref):\n    plan = backend.plan_for(ref)\n    return lambda: plan[0].collect()", [3]),
        ("plan = pl.scan_ipc(path)\nplan.collect()", []),
    ],
)
def test_the_walk_counts_a_collect_over_a_plan_for_result(source, lines):
    assert _leaf_collects(ast.parse(source)) == lines


def test_the_serving_app_collects_no_leaf():
    sites = _app_leaf_collects()
    table = "\n".join([f"collects over a plan_for result: {len(sites)}", *(f"  {f}:{n}  {s}" for f, n, s in sites)])
    print("\n" + table)
    message = (
        "The serving app collects a backend's leaf; read a result's rows through sample(ref, n) on its backend, "
        f"or compose on the leaf and submit the plan to that backend.\n{table}"
    )
    assert len(sites) == 0, message
