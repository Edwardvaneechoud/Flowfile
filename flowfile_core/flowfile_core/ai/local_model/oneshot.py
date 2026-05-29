"""One-shot flow generation for the local model (non-agentic).

The small model emits a whole flow as a compact ``{"nodes": [...], "edges":
[...]}`` JSON object — no tool-calling, since a 1.5B model can't drive the
staged agent reliably. Each node is translated through the SAME executor
staging path the planner uses (``execute_tool_call(mode="stage")``), the
results are bundled into a :class:`~flowfile_core.ai.diff.GraphDiff`, and the
diff is registered so the existing diff-accept UI inserts it onto the canvas.

Reliability rests on constrained decoding (``response_format`` =
``{"type": "json_object"}``, honoured by llama.cpp) plus a tight prompt with a
small curated node vocabulary — see ``prompts/local_oneshot.md``.

Writer / sink nodes are never created: the executor blocks them
(``safety.AGENT_BLOCKED_NODE_TYPES``), so the user attaches the destination
after inserting — the same contract as the full agent.
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

from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import safety
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import InsertionContext, execute_tool_call

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
    """Parse model output into a ``{"nodes", "edges"}`` dict.

    Tries a direct ``json.loads`` first (constrained ``json_object`` output),
    then the first fenced ``json`` block. Raises :class:`OneShotError` if
    neither yields an object with a ``nodes`` list.
    """
    text = (text or "").strip()
    if not text:
        raise OneShotError("model returned empty output")
    for candidate in (text, _first_fenced_block(text)):
        if not candidate:
            continue
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("nodes"), list):
            return obj
    raise OneShotError("could not parse a {nodes, edges} JSON object from the model output")


def _first_fenced_block(text: str) -> str | None:
    lower = text.lower()
    start = lower.find("```json")
    if start == -1:
        start = lower.find("```")
    if start == -1:
        return None
    nl = text.find("\n", start)
    if nl == -1:
        return None
    end = text.find("```", nl + 1)
    if end == -1:
        return None
    return text[nl + 1 : end].strip()


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
    }


async def generate_flow(
    *,
    provider: Provider,
    flow: Any,
    flow_id: int,
    user_id: int,
    user_request: str,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Run the local model on ``user_request`` and stage the resulting flow.

    Returns ``{diff_id, op_count, created, warnings, rationale}``. The provider
    call is async; the executor staging loop is offloaded to a worker thread
    (it can hit the kernel / worker for schema prediction).
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
    return await asyncio.to_thread(_stage_flow, flow=flow, flow_id=flow_id, user_id=user_id, spec=spec)
