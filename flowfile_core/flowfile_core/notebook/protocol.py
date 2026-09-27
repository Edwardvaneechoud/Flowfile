"""The wire format between core and a notebook session process.

A message is a JSON object with a ``type``, framed as a 4-byte big-endian length followed by that many
bytes of UTF-8 JSON, written in binary mode and flushed per message. A body above ``SPILL_THRESHOLD``
travels as a temp file instead: the frame carries ``{"type": ..., "$spill": <path>}`` and the reader loads
and deletes the file, so a multi-MB seed or graph never sits in a pipe buffer (V3 F3).

Core to session: ``seed``, ``execute``, ``interrupt``, ``reset``, ``clean_run``, ``schemas``, ``shutdown``.
Session to core: ``ready``, ``stream``, ``display``, ``done``, ``graph``, ``schemas``.

The module is stdlib-only: the session imports it right after its fd swap.
"""

from __future__ import annotations

import json
import logging
import os
import struct
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import IO, Any

HEADER = struct.Struct(">I")
SPILL_THRESHOLD = 1_000_000
SPILL_KEY = "$spill"
MAX_FRAME = 256 * 1024 * 1024

CORE_TO_SESSION = frozenset({"seed", "execute", "interrupt", "reset", "clean_run", "schemas", "shutdown"})
SESSION_TO_CORE = frozenset({"ready", "stream", "display", "done", "graph", "schemas"})

logger = logging.getLogger("flowfile.notebook.protocol")


def encode(message: dict[str, Any], spill_dir: Path | str | None = None) -> bytes:
    """One framed message; the body is spilled to a file under ``spill_dir`` when it is over the threshold."""
    body = json.dumps(message, ensure_ascii=False, default=str).encode("utf-8")
    if spill_dir is not None and len(body) > SPILL_THRESHOLD:
        directory = Path(spill_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"nb_{uuid.uuid4().hex}.json"
        path.write_bytes(body)
        body = json.dumps({"type": message.get("type"), SPILL_KEY: str(path)}).encode("utf-8")
    return HEADER.pack(len(body)) + body


def write_message(
    stream: IO[bytes], message: dict[str, Any], lock: threading.Lock | None = None, spill_dir: Path | str | None = None
) -> None:
    """Frame and write ``message`` to ``stream`` under ``lock``, flushing after the write."""
    frame = encode(message, spill_dir)
    if lock is None:
        stream.write(frame)
        stream.flush()
        return
    with lock:
        stream.write(frame)
        stream.flush()


def _read_exact(stream: IO[bytes], size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = stream.read(size - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def _load_spill(message: dict[str, Any]) -> dict[str, Any]:
    path = Path(message[SPILL_KEY])
    try:
        return json.loads(path.read_bytes().decode("utf-8"))
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def read_message(stream: IO[bytes], on_garbage: Callable[[bytes], None] | None = None) -> dict[str, Any] | None:
    """The next message on ``stream``, or ``None`` at EOF.

    Anything that is not a framed JSON object (an absurd length, invalid UTF-8 or JSON, a non-object)
    is handed to ``on_garbage`` (logged by default) and skipped; reading continues with the next frame.
    """
    report = on_garbage or (lambda data: logger.warning("notebook session sent unframed bytes: %r", data[:200]))
    while True:
        header = _read_exact(stream, HEADER.size)
        if header is None:
            return None
        (length,) = HEADER.unpack(header)
        if length > MAX_FRAME:
            report(header)
            continue
        body = _read_exact(stream, length)
        if body is None:
            report(header)
            return None
        try:
            message = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            report(body)
            continue
        if not isinstance(message, dict) or "type" not in message:
            report(body)
            continue
        if SPILL_KEY in message:
            message = _load_spill(message)
        return message
