"""Standalone collector for Flowfile's opt-in anonymous telemetry.

Receives event batches on POST /events, validates them against the frozen
event schema, and appends accepted events as JSON lines to
``$TELEMETRY_DATA_DIR/events.jsonl``. Deliberately self-contained: stdlib +
FastAPI only, no imports from the Flowfile monorepo, no database.

The event schema constants below (``ALLOWED_EVENTS``, ``EVENT_PROPS``, the
bucket/target value sets) deliberately duplicate ``shared/telemetry.py`` —
this service must deploy from this directory alone.
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

MAX_BODY_BYTES = 256 * 1024
MAX_BATCH_SIZE = 100
MAX_STRING_LEN = 64
MAX_NODE_TYPES = 60

APP_VERSION_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z.+_-]{0,31}")

PLATFORMS = frozenset({"darwin", "linux", "windows", "other"})
MODES = frozenset({"electron", "docker", "package", "other"})
NODE_COUNT_BUCKETS = frozenset({"1-3", "4-7", "8-15", "16-30", "31+"})
DURATION_BUCKETS = frozenset({"<1s", "1-10s", "10-60s", "1-5m", "5-30m", "30m+"})
EXPORT_TARGETS = frozenset({"polars", "flowframe", "project", "project_zip", "project_save"})

EVENT_PROPS: dict[str, frozenset[str]] = {
    "app_started": frozenset(),
    "flow_created": frozenset(),
    "flow_run_started": frozenset(),
    "flow_run_succeeded": frozenset({"node_count_bucket", "node_types", "duration_bucket", "used_sample_data"}),
    "flow_run_failed": frozenset({"error_class"}),
    "activation": frozenset(),
    "ai_diff_accepted": frozenset(),
    "ai_diff_rejected": frozenset(),
    "catalog_used": frozenset(),
    "schedule_created": frozenset(),
    "kernel_used": frozenset(),
    "export_code_used": frozenset({"target"}),
}
ALLOWED_EVENTS = frozenset(EVENT_PROPS)

app = FastAPI(title="Flowfile telemetry collector")
_write_lock = threading.Lock()


def _data_dir() -> Path:
    return Path(os.environ.get("TELEMETRY_DATA_DIR") or "./data")


def _parse_ts(value: str) -> bool:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _valid_identifier(value: object) -> bool:
    return isinstance(value, str) and len(value) <= MAX_STRING_LEN and value.isidentifier()


def _valid_prop(key: str, value: object) -> bool:
    if key == "node_count_bucket":
        return isinstance(value, str) and value in NODE_COUNT_BUCKETS
    if key == "duration_bucket":
        return isinstance(value, str) and value in DURATION_BUCKETS
    if key == "used_sample_data":
        return isinstance(value, bool)
    if key == "target":
        return isinstance(value, str) and value in EXPORT_TARGETS
    if key == "error_class":
        return _valid_identifier(value)
    if key == "node_types":
        return (
            isinstance(value, list)
            and len(value) <= MAX_NODE_TYPES
            and all(_valid_identifier(item) for item in value)
        )
    return False


def _validate_event(raw: object) -> dict | None:
    """Return the cleaned envelope for a valid event, or None to reject it."""
    if not isinstance(raw, dict):
        return None
    event = raw.get("event")
    if not isinstance(event, str) or event not in ALLOWED_EVENTS:
        return None
    install_id = raw.get("install_id")
    if not isinstance(install_id, str) or len(install_id) > MAX_STRING_LEN:
        return None
    try:
        uuid.UUID(install_id)
    except ValueError:
        return None
    app_version = raw.get("app_version")
    if not isinstance(app_version, str) or not APP_VERSION_RE.fullmatch(app_version):
        return None
    platform = raw.get("platform")
    if not isinstance(platform, str) or platform not in PLATFORMS:
        return None
    mode = raw.get("mode")
    if not isinstance(mode, str) or mode not in MODES:
        return None
    ts = raw.get("ts")
    if not isinstance(ts, str) or len(ts) > MAX_STRING_LEN or not _parse_ts(ts):
        return None
    props = raw.get("props")
    if not isinstance(props, dict):
        return None
    allowed = EVENT_PROPS[event]
    for key, value in props.items():
        if key not in allowed or not _valid_prop(key, value):
            return None
    event_id = raw.get("event_id")
    if event_id is not None:
        if not isinstance(event_id, str) or len(event_id) > MAX_STRING_LEN:
            return None
        try:
            uuid.UUID(event_id)
        except ValueError:
            return None
    cleaned = {
        "event": event,
        "install_id": install_id,
        "app_version": app_version,
        "platform": platform,
        "mode": mode,
        "ts": ts,
        "props": props,
    }
    if event_id is not None:
        cleaned["event_id"] = event_id  # optional: pre-spool clients never sent one
    return cleaned


async def _read_capped_body(request: Request) -> bytes | None:
    """Read the body without ever buffering more than MAX_BODY_BYTES; None means too large."""
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            if int(declared) > MAX_BODY_BYTES:
                return None
        except ValueError:
            return None
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_BODY_BYTES:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _append_lines(lines: list[str]) -> None:
    directory = _data_dir()
    directory.mkdir(parents=True, exist_ok=True)
    with _write_lock, open(directory / "events.jsonl", "a", encoding="utf-8") as handle:
        handle.write("".join(line + "\n" for line in lines))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/events")
def ingest(body: bytes | None = Depends(_read_capped_body)) -> JSONResponse:
    """Plain ``def`` on purpose: FastAPI runs it in the threadpool, so the append never blocks the loop.

    The body is read by the async dependency above — that part must stay on the
    event loop; everything after it (parse, validate, write) is what benefits
    from leaving it.
    """
    if body is None:
        return JSONResponse(status_code=413, content={"detail": "request body too large"})
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return JSONResponse(status_code=422, content={"detail": "body is not valid JSON"})
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return JSONResponse(status_code=422, content={"detail": 'body must contain an "events" list'})
    if len(events) > MAX_BATCH_SIZE:
        return JSONResponse(status_code=422, content={"detail": f"at most {MAX_BATCH_SIZE} events per batch"})

    received_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    accepted: list[str] = []
    rejected = 0
    for raw in events:
        try:
            cleaned = _validate_event(raw)
        except Exception:  # one malformed event must never abort the batch
            cleaned = None
        if cleaned is None:
            rejected += 1
            continue
        cleaned["received_at"] = received_at
        accepted.append(json.dumps(cleaned, separators=(",", ":")))
    if accepted:
        try:
            _append_lines(accepted)
        except OSError:  # a full or read-only disk answers 500, and the client re-sends later
            return JSONResponse(status_code=500, content={"detail": "could not store events"})
    return JSONResponse(status_code=202, content={"accepted": len(accepted), "rejected": rejected})
