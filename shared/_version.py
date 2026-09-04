"""Flowfile version, kept in sync with pyproject by tools/bump_version.py (CI-guarded)."""

__version__ = "0.17.1"


def get_version() -> str:
    return __version__
