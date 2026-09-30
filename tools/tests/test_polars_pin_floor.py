"""The root polars floor must satisfy every locked package that itself requires polars.

A polars plugin (e.g. polars-grouper) with a higher polars floor than the root pin
makes the declared constraint a lie: pip resolves it, but a plugin/Polars ABI
mismatch only surfaces when the plugin expression runs.
"""

import re
import sys
from pathlib import Path

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


def test_root_polars_floor_covers_locked_dependants():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    root_floor = _floor(pyproject["tool"]["poetry"]["dependencies"]["polars"])
    assert root_floor is not None, "root polars pin has no lower bound"

    lock = tomllib.loads((ROOT / "poetry.lock").read_text(encoding="utf-8"))
    too_high = {}
    for package in lock["package"]:
        dep = package.get("dependencies", {}).get("polars")
        if dep is None:
            continue
        floors = [_floor(spec) for spec in _polars_specs(dep)]
        floor = min((f for f in floors if f is not None), default=None)
        if floor is not None and floor > root_floor:
            too_high[f"{package['name']}=={package['version']}"] = str(floor)

    assert not too_high, (
        f"root pyproject pins polars>={root_floor}, but these locked packages need more: {too_high}. "
        "Raise the root polars floor (and keep kernel_runtime compatible)."
    )
