"""Flowfile version, kept in sync with pyproject by tools/bump_version.py (CI-guarded)."""

__version__ = "0.14.2"


def get_version() -> str:
    return __version__
