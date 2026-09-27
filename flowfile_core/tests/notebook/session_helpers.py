"""Shared helpers for the notebook session tests: a small canvas flow and event waiting."""

from __future__ import annotations

import time

import flowfile_frame as ff
from flowfile_core.notebook.registry import NotebookSession, seed_snapshot


def small_flow():
    """A two-node frame-built canvas: a manual input and a filter over it."""
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return orders.filter(ff.col("amount") > 10).flow_graph


def small_snapshot() -> dict:
    return seed_snapshot(small_flow())


def wait_for(session: NotebookSession, predicate, timeout: float = 30.0, after: int = 0) -> dict:
    """The first event after ``after`` matching ``predicate``; fails after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    cursor = after
    while time.monotonic() < deadline:
        for event in session.wait_events(cursor, 0.2):
            cursor = event["seq"]
            if predicate(event):
                return event
    raise AssertionError(f"no matching event in {timeout}s; buffered: {session.events_after(after)[-10:]}")


def done_of(ticket: str):
    return lambda event: event.get("type") == "done" and event.get("ticket") == ticket


def run_cell(session: NotebookSession, code: str, cell_id: str = "c", timeout: float = 30.0) -> tuple[dict, list]:
    """Execute ``code`` and wait for its ``done``; returns it and every event of the ticket."""
    start = session.events_after(0)[-1]["seq"] if session.events_after(0) else 0
    ticket = session.execute(cell_id, code)
    done = wait_for(session, done_of(ticket), timeout, after=start)
    return done, [e for e in session.events_after(start) if e.get("ticket") == ticket]
