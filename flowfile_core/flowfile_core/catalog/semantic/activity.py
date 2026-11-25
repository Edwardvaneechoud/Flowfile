"""What a semantic model's Suggest is doing now, and how the last one ended, per namespace.

The rules route, the AI draft and specific suggestions can take minutes on a local model, so each tracks its run here
and a page opened meanwhile can say so. One run per namespace at a time: a second start raises ``ActivityRunning``,
which the routes answer as a 409 ``SUGGEST_RUNNING``.

**Single-process, in-memory state**: a core restart forgets a running suggestion's status (what it had proposed stays
in Review), and two core processes on one catalog do not see each other's runs. A finished run is kept for
``KEEP_SECONDS``. A client that goes away does not end a run. Thread-safe; imports nothing from glosslayer.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from flowfile_core.schemas.semantic_schema import SemanticActivity, SuggestCounts

KEEP_SECONDS = 30 * 60
_NOUNS = {
    "roles": ("column role", "column roles"),
    "joins": ("join", "joins"),
    "metrics": ("metric", "metrics"),
    "descriptions": ("description", "descriptions"),
    "synonyms": ("set of synonyms", "sets of synonyms"),
}

_lock = threading.Lock()
_running: dict[int, SemanticActivity] = {}
_last: dict[int, SemanticActivity] = {}


class ActivityRunning(Exception):
    """A suggestion is already running for this namespace."""

    def __init__(self, running: SemanticActivity) -> None:
        super().__init__(f"A suggestion is already running for this model ({running.label.rstrip('…')}).")


def read(namespace_id: int, viewer_id: int | None) -> tuple[SemanticActivity | None, SemanticActivity | None]:
    """``(running, last)`` for the namespace as ``viewer_id`` sees them; ``last`` is dropped after ``KEEP_SECONDS``."""
    with _lock:
        cutoff = datetime.now(UTC) - timedelta(seconds=KEEP_SECONDS)
        for ns_id in [k for k, a in _last.items() if a.finished_at < cutoff]:
            del _last[ns_id]
        found = (_running.get(namespace_id), _last.get(namespace_id))
        return tuple(a and a.model_copy(update={"mine": a.user_id == viewer_id}) for a in found)


@contextmanager
def tracking(namespace_id: int, kind: str, label: str, user_id: int | None) -> Iterator[SemanticActivity]:
    """Begin a run and yield it: the block sets ``step`` while running and ``summary``/``counts`` before it leaves.

    An exception ends it as failed, with its message.
    """
    act = SemanticActivity(kind=kind, label=label, user_id=user_id, started_at=datetime.now(UTC))
    with _lock:
        if namespace_id in _running:
            raise ActivityRunning(_running[namespace_id])
        _running[namespace_id] = act
    try:
        yield act
        act.ok = True
    except BaseException as exc:
        act.ok, act.summary, act.error = False, "The suggestion failed.", (str(exc) or type(exc).__name__)[:300]
        raise
    finally:
        with _lock:
            act.finished_at, act.step = datetime.now(UTC), None
            _last[namespace_id] = _running.pop(namespace_id)


def listed(parts: list[str]) -> str:
    """``a``, ``a and b``, ``a, b and c``."""
    return " and ".join([", ".join(parts[:-1]), parts[-1]] if len(parts) > 1 else parts)


def summary_of(counts: dict[str, int]) -> str:
    """``Proposed 12 column roles, 2 joins and 9 metrics.`` from counts by kind (plural keys)."""
    parts = [f"{n} {nouns[n != 1]}" for key, nouns in _NOUNS.items() if (n := counts.get(key, 0))]
    if not parts:
        return "Nothing new to propose: the model already holds every suggestion."
    return f"Proposed {listed(parts)}."


def counts_of(added: SuggestCounts) -> dict[str, int]:
    """The counts of a rules run, with a ``synonyms`` key like a request's."""
    return {"synonyms": 0} | added.model_dump()


def suggest_label(kinds: list[str] | None) -> str:
    """What the status line says while the rules run."""
    names = {"join": "joins", "metric": "metrics", "description": "descriptions", "role": "column roles"}
    return f"Suggesting {listed([names.get(k, k) for k in kinds])}…" if kinds else "Suggesting a model…"
