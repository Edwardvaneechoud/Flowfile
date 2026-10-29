"""Flowfile version, kept in sync with pyproject by tools/bump_version.py (CI-guarded)."""

__version__ = "0.24.0-rc.1"


def get_version() -> str:
    return __version__
