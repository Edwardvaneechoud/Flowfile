"""On-disk spool backing ``shared.telemetry``'s failed deliveries.

One compact-JSON envelope per line, appended when a send fails transiently and
drained oldest-first the next time the daemon thread starts. Stdlib only, and
every function swallows ``OSError``: an unreadable or unwritable spool must
degrade to exactly the pre-spool behaviour (the events drop) rather than raise
into a caller. The file path is supplied by the caller — ``telemetry._spool_file``
is the seam tests redirect — so this module holds no state of its own.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def encode(envelope: dict[str, Any]) -> str:
    return json.dumps(envelope, separators=(",", ":"))


def decode(line: str) -> dict[str, Any] | None:
    """Parse one spooled line, or ``None`` when it is corrupt."""
    try:
        envelope = json.loads(line)
    except ValueError:
        return None
    return envelope if isinstance(envelope, dict) else None


def load(path: Path) -> list[str]:
    """Every non-empty line, oldest first; empty when the spool is missing or unreadable."""
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    return [line for line in content.splitlines() if line.strip()]


def rewrite(path: Path, lines: list[str]) -> bool:
    """Replace the spool with *lines*, deleting the file when nothing is left.

    Staged through a sibling ``.tmp`` and ``os.replace`` for the same reason the
    consent file is: a torn spool is worse than a lost one. The temp file is
    unlinked even when the replace fails, so a failed compaction leaves no
    orphan holding a copy of the events.
    """
    if not lines:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            return False
        return True
    tmp = path.with_suffix(".jsonl.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
        os.replace(tmp, path)
        return True
    except OSError:
        return False
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def compact(path: Path, max_bytes: int) -> bool:
    """Drop corrupt lines and then the oldest lines until the spool fits *max_bytes*."""
    kept = [line for line in load(path) if decode(line) is not None]
    size = sum(len(line.encode("utf-8")) + 1 for line in kept)
    index = 0
    while index < len(kept) and size > max_bytes:
        size -= len(kept[index].encode("utf-8")) + 1
        index += 1
    return rewrite(path, kept[index:])


def append(path: Path, envelopes: list[dict[str, Any]], max_bytes: int) -> bool:
    """Append envelopes, FIFO-compacting when the file outgrows *max_bytes*.

    Returns True when a compaction rewrote the file — that invalidates the line
    offsets a concurrent drain is holding, so the caller must notice.
    """
    lines = [encode(envelope) for envelope in envelopes]
    if not lines:
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("".join(line + "\n" for line in lines))
        if path.stat().st_size <= max_bytes:
            return False
    except OSError:
        return False
    return compact(path, max_bytes)


def purge(path: Path) -> None:
    """Delete the spool outright — used when consent is revoked."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
