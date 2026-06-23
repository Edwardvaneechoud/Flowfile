"""Single source of truth for the Flowfile version at runtime.

The canonical value lives in the root ``pyproject.toml`` ``[tool.poetry]`` table;
``make bump-version`` keeps the frontend/Tauri/Cargo mirrors in sync.
"""

import re
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

_DISTRIBUTION = "Flowfile"
_UNKNOWN = "0.0.0+unknown"


@lru_cache(maxsize=1)
def get_version() -> str:
    """Resolve the version across installed, source-checkout, and frozen contexts.

    Order: installed distribution metadata (also bundled into the PyInstaller
    sidecars via ``copy_metadata('Flowfile')``) → repo-root ``pyproject.toml`` →
    a sentinel. Never returns ``None``.
    """
    try:
        return version(_DISTRIBUTION)
    except PackageNotFoundError:
        return _version_from_pyproject() or _UNKNOWN


def _version_from_pyproject() -> str | None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    try:
        text = pyproject.read_text(encoding="utf-8")
    except OSError:
        return None
    section = re.search(r"^\[tool\.poetry\]\s*$(.*?)(?=^\[|\Z)", text, re.MULTILINE | re.DOTALL)
    scope = section.group(1) if section else text
    match = re.search(r"""^\s*version\s*=\s*["']([^"']+)["']""", scope, re.MULTILINE)
    return match.group(1) if match else None
