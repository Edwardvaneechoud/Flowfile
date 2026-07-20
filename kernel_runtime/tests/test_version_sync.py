"""The runtime __version__ (reported by /health, shown in the Kernel Manager)
must match the image/package version in kernel_runtime/pyproject.toml. Bump both
together with `make bump-version-kernel VERSION=X.Y.Z`.
"""
import re
from pathlib import Path

import kernel_runtime

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _pyproject_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    section = re.search(r"^\[tool\.poetry\](.*?)(?=^\[|\Z)", text, re.MULTILINE | re.DOTALL)
    assert section, "no [tool.poetry] section in kernel_runtime/pyproject.toml"
    match = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.MULTILINE)
    assert match, "no version in [tool.poetry]"
    return match.group(1)


def test_runtime_version_matches_pyproject():
    assert kernel_runtime.__version__ == _pyproject_version(), (
        "kernel_runtime.__version__ is out of sync with kernel_runtime/pyproject.toml — "
        "run `make bump-version-kernel VERSION=X.Y.Z`."
    )
