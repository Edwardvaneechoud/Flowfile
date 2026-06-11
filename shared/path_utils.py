"""Lightweight path helpers shared across Flowfile services.

Dependency-free on purpose: imported by hot core/worker schema modules.
"""

from __future__ import annotations


def is_url(path: str | None) -> bool:
    """Return True if ``path`` is a remote HTTP(S) URL rather than a local filesystem path.

    Polars reads HTTP(S) sources natively (``scan_csv``/``read_csv``/``scan_parquet``),
    so callers use this to skip local-path resolution and ``os.path.getsize`` checks.
    """
    return isinstance(path, str) and path.startswith(("http://", "https://"))
