"""Ratchet on inline execution-location branches and worker env-var reads in flowfile_core.

The flow graph, its node builders, the flow node and the data engine decide where work runs by comparing
``execution_location`` or ``run_location`` inline, and a number of core modules read ``OFFLOAD_TO_WORKER`` or
``WORKER_URL`` on their own. Every such site is a second copy of the same decision, so both counts are pinned
at the value measured when this test landed and may only fall: a new inline branch or a new module reading
the worker settings fails here. When a count drops, lower its pin to the new value.

Compares between an old and a new ``execution_location`` are not counted: they detect a settings change, not
where to run, and stay regardless of how execution is dispatched.

``configs/settings.py`` and ``flowfile/execution/registry.py`` are exempt from the reader count: settings defines
the worker settings, and the registry is the one place that turns them into the default compute backend.

The check is static: source files are parsed with ``ast`` or scanned as text, and nothing from flowfile_core is
imported. Run with ``-s`` to print the per-file tables.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.core

PINNED_INLINE_LOCATION_BRANCHES = 7
PINNED_ENV_VAR_READER_FILES = 9

_PACKAGE_ROOT = Path(__file__).resolve().parents[3] / "flowfile_core"
_FLOWFILE_DIR = _PACKAGE_ROOT / "flowfile"
_LOCATION_NAMES = frozenset({"execution_location", "run_location"})
_ENV_VAR_NAMES = ("OFFLOAD_TO_WORKER", "WORKER_URL")
_EXEMPT_READERS = (Path("configs", "settings.py"), Path("flowfile", "execution", "registry.py"))


def _rel(path: Path) -> str:
    return path.relative_to(_PACKAGE_ROOT).as_posix()


def _branch_files() -> list[Path]:
    files = sorted((_FLOWFILE_DIR / "flow_graph").glob("*.py"))
    for sub in ("flow_graph/builders", "flow_node", "flow_data_engine"):
        files += sorted((_FLOWFILE_DIR / sub).rglob("*.py"))
    return files


def _location_ref(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name) and node.id in _LOCATION_NAMES:
        return node.id
    if isinstance(node, ast.Attribute) and node.attr in _LOCATION_NAMES:
        return node.attr
    return None


def _is_settings_change(operands: list[ast.expr]) -> bool:
    return len(operands) == 2 and all(_location_ref(op) == "execution_location" for op in operands)


def _inline_location_branches() -> list[tuple[str, int, str]]:
    sites = []
    for path in _branch_files():
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        for node in ast.walk(ast.parse(source, filename=str(path))):
            if not isinstance(node, ast.Compare):
                continue
            operands = [node.left, *node.comparators]
            if any(_location_ref(op) for op in operands) and not _is_settings_change(operands):
                sites.append((_rel(path), node.lineno, lines[node.lineno - 1].strip()))
    return sorted(sites)


def _env_var_reader_files() -> dict[str, int]:
    readers = {}
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        if path.relative_to(_PACKAGE_ROOT) in _EXEMPT_READERS:
            continue
        code_lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.lstrip().startswith("#")]
        refs = sum(ln.count(name) for ln in code_lines for name in _ENV_VAR_NAMES)
        if refs:
            readers[_rel(path)] = refs
    return readers


def _table(title: str, rows: list[tuple[str, ...]]) -> str:
    widths = [max(len(cell) for cell in column) for column in zip(*rows, strict=True)]
    body = ["  " + "  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)).rstrip() for row in rows]
    return "\n".join([title, *body])


def test_inline_location_branches_do_not_grow():
    sites = _inline_location_branches()
    table = _table(
        f"inline location branches: {len(sites)} (pinned {PINNED_INLINE_LOCATION_BRANCHES})",
        [(file, str(line), src) for file, line, src in sites],
    )
    message = f"A new inline execution_location/run_location compare was added; this count may only fall.\n{table}"
    assert len(sites) <= PINNED_INLINE_LOCATION_BRANCHES, message
    print("\n" + table)


def test_env_var_reader_files_do_not_grow():
    readers = _env_var_reader_files()
    table = _table(
        f"files reading OFFLOAD_TO_WORKER / WORKER_URL: {len(readers)} (pinned {PINNED_ENV_VAR_READER_FILES})",
        [(file, str(refs)) for file, refs in readers.items()],
    )
    message = f"A module outside configs/settings.py newly reads a worker setting; this count may only fall.\n{table}"
    assert len(readers) <= PINNED_ENV_VAR_READER_FILES, message
    print("\n" + table)
