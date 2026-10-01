"""Each project's polars floor must satisfy every package in its lock that itself requires polars.

A polars plugin (e.g. polars-grouper) with a higher polars floor than the declared pin
makes the constraint a lie: pip resolves it, but a plugin/Polars ABI mismatch only
surfaces when the plugin expression runs. The root and kernel_runtime floors differ on
purpose (the kernel ships none of the root's plugins), so each is checked against its own lock.
"""

import re
import sys
from pathlib import Path

import pytest
from packaging.version import Version

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[2]


def _floor(spec: str) -> Version | None:
    """Lowest version a Poetry constraint admits (``||`` alternatives take the loosest)."""
    floors = []
    for alternative in spec.split("||"):
        bounds = [Version(v) for v in re.findall(r"(?:>=?|==|~=|\^|~)\s*([0-9][0-9.]*)", alternative)]
        if not bounds:
            return None
        floors.append(max(bounds))
    return min(floors, default=None)


def _polars_specs(dep) -> list[str]:
    items = dep if isinstance(dep, list) else [dep]
    return [item if isinstance(item, str) else item.get("version", "") for item in items]


@pytest.mark.parametrize("project", [".", "kernel_runtime"], ids=["root", "kernel_runtime"])
def test_polars_floor_covers_locked_dependants(project):
    project_dir = ROOT / project
    pyproject = tomllib.loads((project_dir / "pyproject.toml").read_text(encoding="utf-8"))
    pinned_floor = _floor(pyproject["tool"]["poetry"]["dependencies"]["polars"])
    assert pinned_floor is not None, f"{project} polars pin has no lower bound"

    lock = tomllib.loads((project_dir / "poetry.lock").read_text(encoding="utf-8"))
    too_high = {}
    for package in lock["package"]:
        dep = package.get("dependencies", {}).get("polars")
        if dep is None:
            continue
        floors = [_floor(spec) for spec in _polars_specs(dep)]
        floor = min((f for f in floors if f is not None), default=None)
        if floor is not None and floor > pinned_floor:
            too_high[f"{package['name']}=={package['version']}"] = str(floor)

    assert not too_high, (
        f"{project}/pyproject.toml pins polars>={pinned_floor}, but these locked packages need more: {too_high}. "
        "Raise that project's polars floor."
    )
