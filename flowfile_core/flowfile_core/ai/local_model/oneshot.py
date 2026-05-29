"""Whole-flow generation from one model response — two build modes.

The model emits a flow as a compact ``{"nodes": [...], "edges": [...]}`` object
(no tool-calling). There are two ways to turn that into a canvas change:

* **one-shot** (``_stage_flow``) — the original path, tuned for *bigger* models.
  Each node is staged through the real executor (``execute_tool_call(mode=
  "stage")``): Pydantic validation, schema prediction, kernel dry-run, column-ref
  refusals. Catches mistakes before they reach the canvas. More expensive.
* **simple** (``_build_simple_diff``) — a deliberately-light path. It lays the
  nodes out (:func:`_plan_insertions`) and builds a
  :class:`~flowfile_core.ai.diff.GraphDiff` **directly**, with NO validation /
  prediction / dry-run at generate time. The only check is Pydantic
  ``model_validate`` at *apply* time (``diff.apply_diff``) — a bad node surfaces
  when the user clicks "Add to canvas". Cheap and forgiving; good for small
  local models. Both modes are provider-agnostic (local or cloud).

Reliability rests on a tolerant parser (:func:`extract_flow_json`) plus a tight
prompt with a small curated node vocabulary — see ``prompts/local_oneshot.md``.

Writer / sink nodes are dropped in both modes (mirroring the agent's
``safety.AGENT_BLOCKED_NODE_TYPES``) so a generated flow never auto-creates an
external write; the user attaches the destination after inserting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import safety
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import InsertionContext, execute_tool_call
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "local_oneshot.md"
_ADD_PREFIX = "flowfile.graph.add_"
_TWO_INPUT_TYPES = frozenset({"join", "fuzzy_match", "cross_join"})

_LAYOUT_X0 = 50.0
_LAYOUT_Y0 = 50.0
_LAYOUT_X = 250.0
_LAYOUT_Y = 130.0

_FALLBACK_PROMPT = (
    "You convert a plain-English data-pipeline request into ONE JSON object: "
    '{"nodes":[{"id":"n1","type":"read","settings":{...}}],'
    '"edges":[{"source":"n1","target":"n2"}]}. '
    "Output only the JSON object. Do not create output/writer nodes."
)


def _load_prompt() -> str:
    try:
        return _PROMPT_PATH.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - prompt ships with the package
        logger.warning("local_oneshot prompt missing at %s: %s", _PROMPT_PATH, exc)
        return _FALLBACK_PROMPT


SYSTEM_PROMPT = _load_prompt()


class OneShotError(RuntimeError):
    """Raised when the model output can't be parsed into a usable flow spec."""


def extract_flow_json(text: str) -> dict[str, Any]:
    """Parse model output into a ``{"nodes", "edges"}`` dict — tolerant.

    Small models wrap the object in prose, fence it as ```json``` / ```yaml``` /
    bare ```` ``` ````, or emit YAML instead of JSON. We try, in order:

    1. the whole string as JSON (constrained ``json_object`` output),
    2. each fenced code block (JSON then YAML),
    3. the widest ``{ ... }`` brace substring as JSON then YAML,
    4. the whole string as YAML (a JSON superset, so this also catches
       single-quoted / trailing-comma-free loose JSON).

    The first candidate that parses to a dict with a ``nodes`` list wins.
    Raises :class:`OneShotError` only when nothing does.
    """
    text = (text or "").strip()
    if not text:
        raise OneShotError("model returned empty output")

    candidates: list[str] = [text]
    candidates.extend(_fenced_blocks(text))
    brace = _widest_brace_span(text)
    if brace:
        candidates.append(brace)

    for candidate in candidates:
        obj = _try_parse(candidate)
        if isinstance(obj, dict) and isinstance(obj.get("nodes"), list):
            return obj

    raise OneShotError("could not parse a {nodes, edges} object from the model output")


def _try_parse(candidate: str) -> Any:
    """JSON first (fast/strict), then YAML (a JSON superset — tolerates single
    quotes, unquoted keys, trailing prose stripped by the caller). Returns the
    parsed value or ``None`` on failure."""
    candidate = candidate.strip()
    if not candidate:
        return None
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass
    try:
        return yaml.safe_load(candidate)
    except yaml.YAMLError:
        return None


def _fenced_blocks(text: str) -> list[str]:
    """Every fenced code block body, in order. Handles ```json / ```yaml / ``` ."""
    blocks: list[str] = []
    idx = 0
    while True:
        start = text.find("```", idx)
        if start == -1:
            break
        nl = text.find("\n", start)
        if nl == -1:
            break
        end = text.find("```", nl + 1)
        if end == -1:
            break
        blocks.append(text[nl + 1 : end].strip())
        idx = end + 3
    return blocks


def _widest_brace_span(text: str) -> str | None:
    """The substring from the first ``{`` to the last ``}`` (inclusive), or
    ``None``. A blunt but effective way to peel surrounding prose off an
    otherwise-valid object."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start : end + 1]


@dataclass
class _PlannedNode:
    str_id: str
    node_type: str
    node_id: int
    settings: dict[str, Any]
    upstream_ids: list[int]
    right_input_id: int | None
    pos_x: float
    pos_y: float


def _plan_insertions(spec: dict[str, Any], start_id: int) -> list[_PlannedNode]:
    """Topologically order the spec's nodes, allocate int ids, resolve wiring.

    ``start_id`` is the first free node id in the target flow; ids are assigned
    sequentially in topological order. Edges become ``upstream_ids``; for
    two-input node types (join / fuzzy_match / cross_join) the second upstream
    becomes ``right_input_id``.
    """
    nodes: dict[str, dict[str, Any]] = {}
    order_seen: list[str] = []
    for raw in spec.get("nodes") or []:
        if not isinstance(raw, dict):
            continue
        sid = str(raw.get("id") or "").strip()
        ntype = str(raw.get("type") or "").strip()
        if not sid or not ntype or sid in nodes:
            continue
        settings = raw.get("settings")
        nodes[sid] = {"type": ntype, "settings": settings if isinstance(settings, dict) else {}}
        order_seen.append(sid)

    incoming: dict[str, list[str]] = {sid: [] for sid in order_seen}
    outgoing: dict[str, list[str]] = {sid: [] for sid in order_seen}
    for raw in spec.get("edges") or []:
        if not isinstance(raw, dict):
            continue
        s = str(raw.get("source") or "").strip()
        t = str(raw.get("target") or "").strip()
        if s in nodes and t in nodes and s != t:
            incoming[t].append(s)
            outgoing[s].append(t)

    topo = _toposort(order_seen, incoming, outgoing)
    id_map = {sid: start_id + i for i, sid in enumerate(topo)}

    depth: dict[str, int] = {}
    lane_at_depth: dict[int, int] = {}
    planned: list[_PlannedNode] = []
    for sid in topo:
        ups = incoming[sid]
        d = 0 if not ups else max((depth.get(u, 0) for u in ups), default=0) + 1
        depth[sid] = d
        lane = lane_at_depth.get(d, 0)
        lane_at_depth[d] = lane + 1
        up_ids = [id_map[u] for u in ups]
        right_id: int | None = None
        if nodes[sid]["type"] in _TWO_INPUT_TYPES and len(up_ids) >= 2:
            right_id = up_ids[1]
            up_ids = [up_ids[0]]
        planned.append(
            _PlannedNode(
                str_id=sid,
                node_type=nodes[sid]["type"],
                node_id=id_map[sid],
                settings=nodes[sid]["settings"],
                upstream_ids=up_ids,
                right_input_id=right_id,
                pos_x=_LAYOUT_X0 + d * _LAYOUT_X,
                pos_y=_LAYOUT_Y0 + lane * _LAYOUT_Y,
            )
        )
    return planned


def _toposort(order_seen: list[str], incoming: dict[str, list[str]], outgoing: dict[str, list[str]]) -> list[str]:
    indeg = {sid: len(incoming[sid]) for sid in order_seen}
    queue = deque(sid for sid in order_seen if indeg[sid] == 0)
    out: list[str] = []
    while queue:
        sid = queue.popleft()
        out.append(sid)
        for t in outgoing[sid]:
            indeg[t] -= 1
            if indeg[t] == 0:
                queue.append(t)
    if len(out) != len(order_seen):  # cycle / unreachable — append the rest in seen order
        seen = set(out)
        out.extend(sid for sid in order_seen if sid not in seen)
    return out


def _next_node_id(flow: Any) -> int:
    used: set[int] = set()
    for node in getattr(flow, "nodes", None) or []:
        try:
            used.add(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    return (max(used) + 1) if used else 1


def _staged_upstream_schemas(entries: list[diff_module.StagedToolEntry]) -> dict[int, list[Any]]:
    """``{node_id: [FlowfileColumn-like]}`` for prior staged adds so a chained
    node can resolve its staged-but-unapplied upstream's columns. Mirrors the
    planner's ``_collect_staged_upstream_schemas`` (reuses its column rebuilder).
    """
    from flowfile_core.ai.agents.planner.staged_schemas import _staged_dict_to_flowfile_column

    out: dict[int, list[Any]] = {}
    for entry in entries:
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
        settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
        nid = settings.get("node_id") if isinstance(settings, dict) else None
        preds = payload.get("predicted_output_schema")
        if isinstance(nid, int) and isinstance(preds, list):
            cols = [_staged_dict_to_flowfile_column(c) for c in preds if isinstance(c, dict)]
            if cols:
                out[nid] = cols
    return out


def _stage_flow(*, flow: Any, flow_id: int, user_id: int, spec: dict[str, Any]) -> dict[str, Any]:
    """Translate a parsed ``{nodes, edges}`` spec into a registered GraphDiff.

    Each node is staged via ``execute_tool_call(mode="stage")``; failures
    (validation, writer-block, …) are collected as warnings and skipped rather
    than aborting the batch. Raises :class:`OneShotError` if nothing stages.
    """
    planned = _plan_insertions(spec, _next_node_id(flow))
    if not planned:
        raise OneShotError("model output contained no usable nodes")

    session_id = f"oneshot-{flow_id}-{uuid.uuid4().hex[:8]}"
    cache = DryRunCache()
    entries: list[diff_module.StagedToolEntry] = []
    created: list[dict[str, Any]] = []
    warnings: list[str] = []

    for p in planned:
        if p.node_type in safety.AGENT_BLOCKED_NODE_TYPES:
            warnings.append(
                f"{p.str_id} ({p.node_type}): writer/sink nodes are added manually — "
                "skipped. Attach your destination after inserting."
            )
            continue
        tool_name = f"{_ADD_PREFIX}{p.node_type}"
        tool_args: dict[str, Any] = {"flow_id": flow_id, "node_id": p.node_id, **p.settings}
        ctx = InsertionContext(
            upstream_node_ids=p.upstream_ids,
            right_input_node_id=p.right_input_id,
            pos_x=p.pos_x,
            pos_y=p.pos_y,
        )
        try:
            result = execute_tool_call(
                flow_id=flow_id,
                tool_name=tool_name,
                tool_args=tool_args,
                insertion_context=ctx,
                session_id=session_id,
                user_id=user_id,
                mode="stage",
                flow=flow,
                dry_run_cache=cache,
                extra_upstream_schemas=_staged_upstream_schemas(entries) or None,
            )
        except Exception as exc:  # noqa: BLE001 — one bad node must not abort the batch
            logger.warning("oneshot: staging %s (%s) raised: %s", p.str_id, p.node_type, exc)
            warnings.append(f"{p.str_id} ({p.node_type}): {exc}")
            continue

        if result.status in ("staged", "warned") and result.staged_node_payload is not None:
            entries.append(
                diff_module.StagedToolEntry(
                    tool_name=tool_name,
                    audit_id=result.audit_id,
                    staged_node_payload=result.staged_node_payload,
                )
            )
            created.append({"id": p.str_id, "type": p.node_type, "node_id": p.node_id})
        else:
            detail = result.refusal_detail or result.refusal_reason or "rejected"
            warnings.append(f"{p.str_id} ({p.node_type}): {detail}")

    if not entries:
        raise OneShotError("none of the generated nodes could be staged: " + "; ".join(warnings[:5]))

    rationale = "Generated by the local model"
    graph_diff = diff_module.bundle_staged_results(entries).model_copy(
        update={"session_id": session_id, "flow_id": flow_id, "rationale": rationale}
    )
    diff_id = diff_module.register_diff(graph_diff)
    return {
        "diff_id": diff_id,
        "op_count": len(entries),
        "created": created,
        "warnings": warnings,
        "rationale": rationale,
        # Full GraphDiff so the frontend can drive its existing diff-review
        # panel directly (same shape the planner emits on its ``complete``
        # event) — no separate GET round-trip needed.
        "diff_payload": graph_diff.model_dump(mode="json"),
    }


def _build_simple_diff(*, flow: Any, flow_id: int, spec: dict[str, Any]) -> dict[str, Any]:
    """Build a :class:`GraphDiff` DIRECTLY from a ``{nodes, edges}`` spec — the
    light "simple" path: no executor, no schema prediction, no dry-run.

    Each planned node becomes a :class:`StagedAddition` whose ``settings`` carry
    the model's raw props plus the planner-assigned ``flow_id`` / ``node_id`` /
    ``pos_x`` / ``pos_y``; each wire becomes a :class:`StagedConnection` in the
    ``NodeConnection`` dump shape. Nothing is validated here — ``apply_diff``
    runs ``settings_cls.model_validate`` when the user clicks Add, so a bad node
    fails *then*, not now. Unknown node types and writer/sink types are dropped
    with a warning (a generated flow must never auto-create an external write).
    """
    planned = _plan_insertions(spec, _next_node_id(flow))
    if not planned:
        raise OneShotError("model output contained no usable nodes")

    # Only keep ids that survive filtering, so an addition never references a
    # dropped (writer/unknown) upstream.
    kept_ids = {
        p.node_id
        for p in planned
        if p.node_type not in safety.AGENT_BLOCKED_NODE_TYPES
        and get_settings_class_for_node_type(p.node_type) is not None
    }

    additions: list[diff_module.StagedAddition] = []
    created: list[dict[str, Any]] = []
    warnings: list[str] = []

    for p in planned:
        if p.node_type in safety.AGENT_BLOCKED_NODE_TYPES:
            warnings.append(f"{p.str_id} ({p.node_type}): writer/sink nodes are added manually — skipped.")
            continue
        if get_settings_class_for_node_type(p.node_type) is None:
            warnings.append(f"{p.str_id}: unknown node type {p.node_type!r} — skipped.")
            continue
        # The wiring rides on insertion_context: ``_apply_add_node`` connects
        # ``upstream_node_ids`` → input-0 and ``right_input_node_id`` → input-1
        # at add time, so no separate connection ops are needed. Drop refs to
        # filtered-out upstreams.
        upstream_ids = [u for u in p.upstream_ids if u in kept_ids]
        right_id = p.right_input_id if (p.right_input_id in kept_ids) else None
        settings = {
            **p.settings,
            "flow_id": flow_id,
            "node_id": p.node_id,
            "pos_x": p.pos_x,
            "pos_y": p.pos_y,
        }
        additions.append(
            diff_module.StagedAddition(
                node_type=p.node_type,
                settings=settings,
                insertion_context=diff_module.StagedInsertionContext(
                    upstream_node_ids=upstream_ids,
                    right_input_node_id=right_id,
                    pos_x=p.pos_x,
                    pos_y=p.pos_y,
                ),
            )
        )
        created.append({"id": p.str_id, "type": p.node_type, "node_id": p.node_id})

    if not additions:
        raise OneShotError("no usable nodes after dropping writers/unknowns: " + "; ".join(warnings[:5]))

    rationale = "Generated flow (simple mode)"
    session_id = f"simple-{flow_id}-{uuid.uuid4().hex[:8]}"
    graph_diff = diff_module.GraphDiff(
        session_id=session_id,
        flow_id=flow_id,
        additions=additions,
        rationale=rationale,
    )
    diff_id = diff_module.register_diff(graph_diff)
    return {
        "diff_id": diff_id,
        "op_count": len(additions),
        "created": created,
        "warnings": warnings,
        "rationale": rationale,
        "diff_payload": graph_diff.model_dump(mode="json"),
    }


async def generate_flow(
    *,
    provider: Provider,
    flow: Any,
    flow_id: int,
    user_id: int,
    user_request: str,
    max_tokens: int | None = None,
    mode: str = "one_shot",
) -> dict[str, Any]:
    """Generate a flow from ``user_request`` and register it as a GraphDiff.

    ``mode``:
      * ``"one_shot"`` (default) — validate each node through the executor
        (schema prediction + dry-run). For bigger models; catches errors early.
      * ``"simple"`` — build the diff directly, no validation until apply. For
        small local models; cheap and forgiving.

    Returns ``{diff_id, op_count, created, warnings, rationale, diff_payload}``.
    The build step is offloaded to a worker thread (the one-shot path can hit
    the kernel / worker for schema prediction).
    """
    messages = [
        Message(role="system", content=SYSTEM_PROMPT),
        Message(role="user", content=user_request),
    ]
    response = await provider.chat(
        messages,
        max_tokens=max_tokens or 1024,
        response_format={"type": "json_object"},
        surface="local_oneshot",
        user_id=user_id,
    )
    spec = extract_flow_json(response.content or "")
    if mode == "simple":
        return await asyncio.to_thread(_build_simple_diff, flow=flow, flow_id=flow_id, spec=spec)
    return await asyncio.to_thread(_stage_flow, flow=flow, flow_id=flow_id, user_id=user_id, spec=spec)
