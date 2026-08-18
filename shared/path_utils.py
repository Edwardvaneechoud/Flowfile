"""Lightweight path helpers shared across Flowfile services.

Dependency-free on purpose: imported by hot core/worker schema modules.
"""

from __future__ import annotations

import glob
import os
import re


class DirectoryScanUnsupportedError(ValueError):
    """Raised when directory/glob scanning is requested for a file type that cannot support it."""


class NoFilesMatchedError(FileNotFoundError):
    """Raised when a directory/glob pattern expands to zero files."""


DIRECTORY_SCAN_FILE_TYPES = frozenset({"csv", "parquet", "ipc"})

_UTF8_ENCODINGS = frozenset({"UTF-8", "UTF8", "UTF8-LOSSY", "UTF-8-LOSSY"})

# Duplicated from flowfile_core.flowfile.parameter_resolver._PARAM_PATTERN; shared cannot import core.
_PARAM_PATTERN = re.compile(r"\$\{[a-zA-Z_][a-zA-Z0-9_]*\}")

_DEFAULT_SCAN_EXTENSIONS = {
    "csv": "csv",
    "parquet": "parquet",
    "ipc": "arrow",
    "excel": "xlsx",
    "json": "json",
    "ndjson": "ndjson",
    "avro": "avro",
}


def is_url(path: str | None) -> bool:
    """Return True if ``path`` is a remote HTTP(S) URL rather than a local filesystem path.

    Polars reads HTTP(S) sources natively (``scan_csv``/``read_csv``/``scan_parquet``),
    so callers use this to skip local-path resolution and ``os.path.getsize`` checks.
    """
    return isinstance(path, str) and path.startswith(("http://", "https://"))


def is_glob_pattern(path: str) -> bool:
    """Return True if ``path`` contains glob metacharacters outside of ``${param}`` references.

    A parameter reference like ``${dir}`` carries a ``{``/``}`` pair but no glob meaning, and
    the substituted value decides globness later, so those spans are masked out first.
    """
    if not isinstance(path, str):
        return False
    masked = _PARAM_PATTERN.sub("\x00", path)
    return any(char in masked for char in ("*", "?", "["))


def is_utf8_encoding(encoding: str | None) -> bool:
    """True when ``encoding`` names the UTF-8 family (any casing), which polars scans natively.

    Everything else (UTF-16, latin-1, ...) routes to the worker's separate csv reader, so
    callers deciding local-vs-worker and directory-scan eligibility must agree on this test.
    """
    return encoding is not None and encoding.upper() in _UTF8_ENCODINGS


def default_scan_extension(file_type: str) -> str:
    """Return the on-disk extension used when synthesising a glob for ``file_type``.

    Total over every supported file type (not just the directory-capable ones) so callers
    building a pattern before validation can never hit a KeyError.
    """
    return _DEFAULT_SCAN_EXTENSIONS.get(file_type, file_type)


def _case_insensitive_ext(ext: str) -> str:
    """Render ``ext`` as a glob matching any casing: fnmatch only folds case on Windows, and a
    scan must read the same files on every platform."""
    return "".join(f"[{char.lower()}{char.upper()}]" if char.isalpha() else glob.escape(char) for char in ext)


def ensure_glob_pattern(path: str, ext: str) -> str:
    """Turn a directory ``path`` into a recursive glob for ``ext``, leaving real patterns alone.

    What exists on disk is checked before the path is sniffed for glob metacharacters, so a real
    directory called ``[archive]`` is a directory rather than a character class. An explicit
    pattern is honoured verbatim, and a concrete existing file reads as itself (escaped, so a
    ``report[1].csv`` still expands to that one file) — a directory-mode node pointed at a
    single file works. Idempotent: every returned pattern round-trips through the glob branch.
    """
    if os.path.isfile(path):
        return glob.escape(path)
    if os.path.isdir(path):
        # glob.escape so metacharacters in the real directory name stay literal.
        return os.path.join(glob.escape(path), "**", f"*.{_case_insensitive_ext(ext)}")
    if is_glob_pattern(path):
        return path
    return os.path.join(path, "**", f"*.{_case_insensitive_ext(ext)}")


def expand_glob_pattern(pattern: str) -> list[str]:
    """Return the sorted list of existing files matching ``pattern`` (recursive, files only).

    This expansion is the single source of truth shared by the scan itself, the
    change-detection fingerprint, the zero-match check, and the schema probe — they must all
    agree on which files a node reads. Python's glob skips dotfiles by design, which keeps
    macOS AppleDouble junk (``._data.csv``) out of the result.
    """
    return sorted(p for p in glob.glob(pattern, recursive=True) if os.path.isfile(p))


def assert_directory_scan_supported(file_type: str, encoding: str | None = None, path: str | None = None) -> None:
    """Raise ``DirectoryScanUnsupportedError`` unless ``file_type`` can be scanned natively.

    Only formats polars can scan from a file list qualify. Non-UTF-8 CSV is excluded because it
    routes to the worker's separate reader, which directory mode must never reach. A remote URL
    is refused outright when ``path`` is supplied: there is no filesystem to expand a glob over.
    """
    if path is not None and is_url(path):
        raise DirectoryScanUnsupportedError("Directory scan mode is not supported for URLs.")
    if file_type not in DIRECTORY_SCAN_FILE_TYPES:
        supported = ", ".join(sorted(DIRECTORY_SCAN_FILE_TYPES))
        raise DirectoryScanUnsupportedError(
            f"Directory scanning is not supported for file type '{file_type}'. Supported types: {supported}."
        )
    if file_type == "csv" and encoding is not None and not is_utf8_encoding(encoding):
        raise DirectoryScanUnsupportedError(
            f"Directory scanning of csv files requires a UTF-8 encoding, got '{encoding}'."
        )
