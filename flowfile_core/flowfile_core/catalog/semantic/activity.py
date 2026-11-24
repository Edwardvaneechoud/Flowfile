"""What a semantic model's Suggest is doing now, and how the last one ended, per namespace.

The rules route (``POST …/semantic-model/suggest``), the AI draft (``POST /ai/semantic/draft``) and specific
suggestions (``POST /ai/semantic/suggest``) can take minutes on a local model. Each marks its start and its end here
so a page opened (or reloaded) meanwhile can say a suggestion is running, and one opened after it ended can still say
what it proposed. One run per namespace at a time: a second start while one runs raises ``ActivityRunning``, which
the routes answer as a 409 ``SUGGEST_RUNNING``.

This is **single-process, in-memory state**: no table, nothing on disk. A core restart forgets a running
suggestion's status (whatever it had already proposed stays in Review), and two core processes on one catalog do
not see each other's runs. A finished run is kept for ``KEEP_SECONDS``. A client that goes away does not end a run:
uvicorn neither cancels a sync handler's thread nor an ``async def`` handler when the connection drops, so the work
finishes and records its end. Thread-safe; imports nothing from glosslayer.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

KEEP_SECONDS = 30 * 60
_NOUNS = (
    ("roles", "column role", "column roles"),
    ("joins", "join", "joins"),
    ("metrics", "metric", "metrics"),
    ("descriptions", "description", "descriptions"),
    ("synonyms", "set of synonyms", "sets of synonyms"),
)


class ActivityRunning(Exception):
    """A suggestion is already running for this namespace."""

    def __init__(self, running: Activity) -> None:
        super().__init__(f"A suggestion is already running for this model ({running.label.rstrip('…')}).")
        self.running = running


@dataclass
class Activity:
    namespace_id: int
    kind: str
    label: str
    user_id: int | None
    started_at: float
    token: str = field(default_factory=lambda: uuid.uuid4().hex)
    step: str | None = None
    finished_at: float | None = None
    ok: bool | None = None
    summary: str | None = None
    error: str | None = None
    counts: dict[str, int] = field(default_factory=dict)

    def as_dict(self, viewer_id: int | None) -> dict[str, Any]:
        """The fields a reader may see: no request text is ever held here, so none can leak."""
        out: dict[str, Any] = {
            "kind": self.kind,
            "label": self.label,
            "started_at": _iso(self.started_at),
            "user_id": self.user_id,
            "mine": viewer_id is not None and viewer_id == self.user_id,
            "step": self.step,
        }
        if self.finished_at is not None:
            out.update(
                finished_at=_iso(self.finished_at),
                ok=bool(self.ok),
                summary=self.summary,
                error=self.error,
                counts=dict(self.counts),
            )
        return out


_lock = threading.Lock()
_running: dict[int, Activity] = {}
_last: dict[int, Activity] = {}


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat()


def _expire(now: float) -> None:
    for ns_id in [k for k, a in _last.items() if now - (a.finished_at or now) > KEEP_SECONDS]:
        del _last[ns_id]


def begin(namespace_id: int, kind: str, label: str, user_id: int | None) -> Activity:
    """Mark a run started; ``ActivityRunning`` when one already runs for ``namespace_id``."""
    with _lock:
        current = _running.get(namespace_id)
        if current is not None:
            raise ActivityRunning(current)
        act = Activity(namespace_id=namespace_id, kind=kind, label=label, user_id=user_id, started_at=time.time())
        _running[namespace_id] = act
        return act


def set_step(act: Activity, step: str | None) -> None:
    with _lock:
        if _running.get(act.namespace_id) is act:
            act.step = step


def end(
    act: Activity,
    *,
    ok: bool,
    summary: str | None = None,
    error: str | None = None,
    counts: dict[str, int] | None = None,
) -> None:
    """Mark the run ended and keep it as the namespace's last; a second end of the same run is ignored."""
    with _lock:
        if _running.get(act.namespace_id) is not act:
            return
        del _running[act.namespace_id]
        now = time.time()
        act.finished_at, act.ok, act.summary, act.counts = now, ok, summary, dict(counts or {})
        act.error = ((error or "")[:300] or None) if not ok else None
        act.step = None
        _last[act.namespace_id] = act
        _expire(now)


def read(namespace_id: int) -> tuple[Activity | None, Activity | None]:
    """``(running, last)`` for the namespace; ``last`` only while it is younger than ``KEEP_SECONDS``."""
    with _lock:
        _expire(time.time())
        return _running.get(namespace_id), _last.get(namespace_id)


class Tracker:
    """The handle ``tracking`` yields: ``step(text)`` while running, ``done(summary, counts)`` before it returns."""

    def __init__(self, act: Activity) -> None:
        self.activity = act
        self.summary: str | None = None
        self.counts: dict[str, int] = {}
        self.ok = True

    def step(self, text: str | None) -> None:
        set_step(self.activity, text)

    def done(self, summary: str | None, counts: dict[str, int] | None = None, *, ok: bool = True) -> None:
        self.summary, self.counts, self.ok = summary, dict(counts or {}), ok


@contextmanager
def tracking(namespace_id: int, kind: str, label: str, user_id: int | None) -> Iterator[Tracker]:
    """Begin a run, end it however the block leaves (an exception ends it as failed, with its message)."""
    tracker = Tracker(begin(namespace_id, kind, label, user_id))
    try:
        yield tracker
    except BaseException as exc:
        end(tracker.activity, ok=False, summary="The suggestion failed.", error=str(exc) or type(exc).__name__)
        raise
    else:
        end(tracker.activity, ok=tracker.ok, summary=tracker.summary, counts=tracker.counts)


def summary_of(counts: dict[str, int]) -> str:
    """``Proposed 12 column roles, 2 joins and 9 metrics.`` from counts by kind (plural keys)."""
    parts = [f"{n} {one if n == 1 else many}" for key, one, many in _NOUNS if (n := counts.get(key, 0))]
    if not parts:
        return "Nothing new to propose: the model already holds every suggestion."
    listed = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]
    return f"Proposed {listed}."


def counts_of(added: Any) -> dict[str, int]:
    """A ``SuggestCounts`` as a plain dict."""
    return {key: int(getattr(added, key, 0) or 0) for key, _one, _many in _NOUNS}


def label_for(kind: str, kinds: list[str] | None = None) -> str:
    """What the status line says while the run goes on; never a person's request text."""
    if kind == "request":
        return "Asking the AI for specific suggestions…"
    if kinds:
        names = {"join": "joins", "metric": "metrics", "description": "descriptions", "role": "column roles"}
        listed = [names.get(k, k) for k in kinds]
        text = listed[0] if len(listed) == 1 else ", ".join(listed[:-1]) + " and " + listed[-1]
        return f"Suggesting {text}…"
    return "Suggesting a model…"


__all__ = [
    "KEEP_SECONDS",
    "Activity",
    "ActivityRunning",
    "Tracker",
    "begin",
    "counts_of",
    "end",
    "label_for",
    "read",
    "set_step",
    "summary_of",
    "tracking",
]
