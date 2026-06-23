"""Unit tests for :mod:`shared._version`.

The version resolver underpins every ``__version__`` in the app and the
``db_info.app_version`` row, so it must always return a real, non-empty string.
"""

import re
from pathlib import Path

from shared._version import _UNKNOWN, _version_from_pyproject, get_version

ROOT = Path(__file__).resolve().parents[2]


def test_get_version_returns_non_empty_string():
    version = get_version()
    assert isinstance(version, str)
    assert version
    assert version != _UNKNOWN  # installed metadata / pyproject must resolve in a checkout


def test_get_version_matches_pyproject():
    # Installed distribution metadata and the source pyproject agree in a checkout.
    assert get_version() == _version_from_pyproject()


def test_version_from_pyproject_parses_tool_poetry():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    expected = re.search(r'^\s*version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    assert expected is not None
    assert _version_from_pyproject() == expected.group(1)
