"""Shared helpers for the notebook session tests: a small canvas flow and its seed snapshot."""

from __future__ import annotations

import flowfile_frame as ff
from flowfile_core.notebook.registry import seed_snapshot


def small_flow():
    """A two-node frame-built canvas: a manual input and a filter over it."""
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return orders.filter(ff.col("amount") > 10).flow_graph


def small_snapshot() -> dict:
    return seed_snapshot(small_flow())
