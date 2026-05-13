""":func:`flowfile_core.ai.tools.executor._resolve_insertion_position` tests.

Pure unit tests for the auto-layout resolver. The resolver derives
``(pos_x, pos_y)`` for an AI-staged node from the live graph: most-recent
upstream wins, deterministic offset, fan-out covered by
``staged_offset_index``.

Cases:

* ``test_resolves_from_single_upstream`` — one upstream → upstream.x + Δx,
  upstream.y + 0.
* ``test_chain_offsets_horizontally`` — chained adds (each upstream is the
  prior staged add) lay out as a straight horizontal line because each call
  passes ``staged_offset_index=0``.
* ``test_fan_out_uses_staged_offset_index`` — multiple staged adds anchored
  to the same upstream stack vertically as ``staged_offset_index`` grows.
* ``test_no_upstream_falls_back_to_seed`` — empty ``upstream_node_ids`` →
  ``(50.0, 50.0)`` with the offset-index y-stack.
* ``test_no_upstream_with_offset_index_stacks`` — multiple cold-flow adds in
  one batch don't collapse onto each other.
* ``test_picks_most_recent_upstream`` — ``upstream_node_ids=[a, b]`` reads
  ``b``'s position (the LLM puts the most-relevant upstream last).
* ``test_unknown_upstream_falls_back_to_seed`` — id not in flow → fallback.
* ``test_uses_canonical_layout_constants`` — Δx / Δy match
  ``calculate_layered_layout`` defaults (250 / 100).
"""

from __future__ import annotations

from typing import Any

import pytest

from flowfile_core.ai.tools.executor import (
    _AUTO_LAYOUT_FALLBACK_X,
    _AUTO_LAYOUT_FALLBACK_Y,
    _AUTO_LAYOUT_X_SPACING,
    _AUTO_LAYOUT_Y_SPACING,
    _resolve_insertion_position,
)
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout
from flowfile_core.schemas import input_schema, schemas


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w62_insertion_position",
    )


def _add_manual_input(
    flow: FlowGraph,
    *,
    node_id: int,
    pos_x: float = 0.0,
    pos_y: float = 0.0,
) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        pos_x=pos_x,
        pos_y=pos_y,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="x", data_type="Integer")],
            data=[[1]],
        ),
    )
    flow.add_manual_input(raw)


def _make_flow_with_node(node_id: int, pos_x: float, pos_y: float) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(), name="w62_resolver_test")
    _add_manual_input(flow, node_id=node_id, pos_x=pos_x, pos_y=pos_y)
    return flow


# --------------------------------------------------------------------------- #
# Single upstream #
# --------------------------------------------------------------------------- #


def test_resolves_from_single_upstream() -> None:
    flow = _make_flow_with_node(node_id=1, pos_x=400.0, pos_y=300.0)
    pos_x, pos_y = _resolve_insertion_position(flow, [1], staged_offset_index=0)
    assert pos_x == 400.0 + _AUTO_LAYOUT_X_SPACING
    assert pos_y == 300.0


# --------------------------------------------------------------------------- #
# Chained — each new call has a different upstream #
# --------------------------------------------------------------------------- #


def test_chain_offsets_horizontally() -> None:
    """Chained transformations: caller passes ``staged_offset_index=0`` for
    each call (each upstream has 0 prior siblings). Result: straight
    horizontal chain."""
    flow = FlowGraph(flow_settings=_flow_settings(), name="chain")
    _add_manual_input(flow, node_id=1, pos_x=0.0, pos_y=0.0)
    _add_manual_input(flow, node_id=2, pos_x=_AUTO_LAYOUT_X_SPACING, pos_y=0.0)
    _add_manual_input(flow, node_id=3, pos_x=2 * _AUTO_LAYOUT_X_SPACING, pos_y=0.0)

    p1 = _resolve_insertion_position(flow, [1], staged_offset_index=0)
    p2 = _resolve_insertion_position(flow, [2], staged_offset_index=0)
    p3 = _resolve_insertion_position(flow, [3], staged_offset_index=0)

    # All three downstreams lay out at the same y, increasing x.
    assert {p[1] for p in (p1, p2, p3)} == {0.0}
    xs = [p[0] for p in (p1, p2, p3)]
    assert xs == sorted(xs)
    assert xs[1] - xs[0] == _AUTO_LAYOUT_X_SPACING
    assert xs[2] - xs[1] == _AUTO_LAYOUT_X_SPACING


# --------------------------------------------------------------------------- #
# Fan-out — same upstream, multiple offset indices #
# --------------------------------------------------------------------------- #


def test_fan_out_uses_staged_offset_index() -> None:
    flow = _make_flow_with_node(node_id=1, pos_x=100.0, pos_y=200.0)
    p0 = _resolve_insertion_position(flow, [1], staged_offset_index=0)
    p1 = _resolve_insertion_position(flow, [1], staged_offset_index=1)
    p2 = _resolve_insertion_position(flow, [1], staged_offset_index=2)
    expected_x = 100.0 + _AUTO_LAYOUT_X_SPACING
    assert p0 == (expected_x, 200.0)
    assert p1 == (expected_x, 200.0 + _AUTO_LAYOUT_Y_SPACING)
    assert p2 == (expected_x, 200.0 + 2 * _AUTO_LAYOUT_Y_SPACING)
    # Distinct y values — the bug that prompted was the y-stack being a
    # single point (all overlap).
    assert len({p0[1], p1[1], p2[1]}) == 3


# --------------------------------------------------------------------------- #
# Cold flow / fallback #
# --------------------------------------------------------------------------- #


def test_no_upstream_falls_back_to_seed() -> None:
    flow = FlowGraph(flow_settings=_flow_settings(), name="cold")
    pos_x, pos_y = _resolve_insertion_position(flow, [], staged_offset_index=0)
    assert (pos_x, pos_y) == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)


def test_no_upstream_with_offset_index_stacks() -> None:
    flow = FlowGraph(flow_settings=_flow_settings(), name="cold-batch")
    p0 = _resolve_insertion_position(flow, [], staged_offset_index=0)
    p1 = _resolve_insertion_position(flow, [], staged_offset_index=1)
    p2 = _resolve_insertion_position(flow, [], staged_offset_index=2)
    assert p0 == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)
    assert p1 == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y + _AUTO_LAYOUT_Y_SPACING)
    assert p2 == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y + 2 * _AUTO_LAYOUT_Y_SPACING)
    # Three cold-flow adds in one batch land at three distinct points.
    assert len({p0, p1, p2}) == 3


# --------------------------------------------------------------------------- #
# Multi-upstream picks most-recent #
# --------------------------------------------------------------------------- #


def test_picks_most_recent_upstream() -> None:
    """The LLM emits ``upstream_node_ids`` with the most-relevant upstream
    last (the planner appends to this list as it chains). The resolver
    should anchor on that one — i.e. iterate in reverse."""
    flow = FlowGraph(flow_settings=_flow_settings(), name="multi-upstream")
    _add_manual_input(flow, node_id=1, pos_x=10.0, pos_y=10.0)
    _add_manual_input(flow, node_id=2, pos_x=999.0, pos_y=888.0)
    pos_x, pos_y = _resolve_insertion_position(flow, [1, 2], staged_offset_index=0)
    # Anchored on node 2, not node 1.
    assert pos_x == 999.0 + _AUTO_LAYOUT_X_SPACING
    assert pos_y == 888.0


# --------------------------------------------------------------------------- #
# Robustness #
# --------------------------------------------------------------------------- #


def test_unknown_upstream_falls_back_to_seed() -> None:
    """Stale ``upstream_node_ids`` referencing a deleted node → resolver
    falls through to the cold-flow seed instead of raising."""
    flow = _make_flow_with_node(node_id=1, pos_x=42.0, pos_y=42.0)
    pos_x, pos_y = _resolve_insertion_position(flow, [999], staged_offset_index=0)
    assert (pos_x, pos_y) == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)


def test_skips_unknown_then_uses_known_upstream() -> None:
    """``[unknown, known]`` → resolver iterates in reverse: known wins."""
    flow = _make_flow_with_node(node_id=1, pos_x=42.0, pos_y=42.0)
    pos_x, pos_y = _resolve_insertion_position(flow, [999, 1], staged_offset_index=0)
    assert pos_x == 42.0 + _AUTO_LAYOUT_X_SPACING
    assert pos_y == 42.0


# --------------------------------------------------------------------------- #
# Canonical-constant lockstep #
# --------------------------------------------------------------------------- #


def test_uses_canonical_layout_constants() -> None:
    """The resolver reuses the spacings of
    :func:`calculate_layered_layout` so AI-staged layout matches the
    project's existing auto-layout helper. Drift here means the two
    auto-layouts visually disagree.
    """
    import inspect

    sig = inspect.signature(calculate_layered_layout)
    canonical_x = sig.parameters["x_spacing"].default
    canonical_y = sig.parameters["y_spacing"].default
    canonical_initial_y = sig.parameters["initial_y"].default
    assert _AUTO_LAYOUT_X_SPACING == float(canonical_x)
    assert _AUTO_LAYOUT_Y_SPACING == float(canonical_y)
    # Fallback Y uses the same seed as the layered helper's ``initial_y``.
    assert _AUTO_LAYOUT_FALLBACK_Y == float(canonical_initial_y)


# --------------------------------------------------------------------------- #
# Defensive — non-numeric pos_x on upstream #
# --------------------------------------------------------------------------- #


def test_non_numeric_upstream_pos_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """Defence-in-depth: if a future settings class ever stores pos_x as
    something non-numeric, the resolver doesn't crash — it falls back."""
    flow = _make_flow_with_node(node_id=1, pos_x=10.0, pos_y=10.0)
    node = flow.get_node(1)
    # Forcefully break the upstream's pos_x to simulate a corrupt setting.
    setting = node.setting_input

    class _Wrapped:
        pos_x: Any = "not-a-number"
        pos_y: Any = "also-bad"

        def __getattr__(self, item: str) -> Any:
            return getattr(setting, item)

    monkeypatch.setattr(node, "_setting_input", _Wrapped())
    pos_x, pos_y = _resolve_insertion_position(flow, [1], staged_offset_index=0)
    assert (pos_x, pos_y) == (_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)
