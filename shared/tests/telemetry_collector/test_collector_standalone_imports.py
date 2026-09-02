"""The collector must stay deployable from its own directory.

Its Dockerfile copies only ``app.py`` and ``funnel.py`` and installs only
``requirements.txt``, so an import of anything else — the monorepo above all —
is an ImportError at container start that no test here would otherwise catch.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

COLLECTOR = Path(__file__).resolve().parents[3] / "tools" / "telemetry_collector"
DEPLOYED = ("app.py", "funnel.py")
ALLOWED = set(sys.stdlib_module_names) | {"fastapi", "uvicorn", "pydantic"}


def _imported_roots(source: str) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "a relative import cannot resolve from the deployed directory"
            roots.add((node.module or "").split(".")[0])
    return roots


@pytest.mark.parametrize("name", DEPLOYED)
def test_deployed_module_imports_nothing_the_image_lacks(name: str) -> None:
    roots = _imported_roots((COLLECTOR / name).read_text(encoding="utf-8"))
    assert roots <= ALLOWED, f"{name} imports {sorted(roots - ALLOWED)}"
