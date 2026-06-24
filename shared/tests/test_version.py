"""Tests for shared._version."""

import re
from pathlib import Path

from shared._version import __version__, get_version

ROOT = Path(__file__).resolve().parents[2]


def _pyproject_version() -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^\s*version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_get_version_returns_literal():
    assert get_version() == __version__
    assert isinstance(__version__, str)
    assert __version__


def test_version_matches_pyproject():
    assert __version__ == _pyproject_version()
