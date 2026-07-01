"""Flowfile version, kept in sync with pyproject by tools/bump_version.py (CI-guarded)."""

__version__ = "0.12.7"


def get_version() -> str:
    return __version__
