"""Fuse linearly-chained code-generator statements into piped expressions.

The code generator emits one ``var = prev.op(...)`` statement per node. For a
linear run of single-use nodes this reads as a ladder of throwaway intermediates
whose names track node ids rather than data flow. This module collapses such runs
into a single piped expression and keeps named variables only where the graph
branches (fan-out) or merges (joins/unions), which is how the code would be
written by hand.

The pass is deliberately conservative: anything it does not confidently recognise
(multi-statement nodes, multi-input nodes, a producer variable referenced anywhere
but the chain base) is left as its own named statement, so correctness degrades to
"less pretty", never to "wrong".
"""

import re
import textwrap
from dataclasses import dataclass

_ASSIGNMENT_RE = re.compile(r"[A-Za-z_]\w* = ")


@dataclass
class NodeEmission:
    """A node's generated statement plus the graph facts the fusion pass needs."""

    node_id: int
    var_name: str
    lines: list[str]  # non-empty code lines for this node (no trailing blanks)
    main_producer_id: int | None  # sole input node id, or None for sources / multi-input
    num_inputs: int  # number of distinct (resolved) input nodes
    is_flow_output: bool
    pinned: bool = False  # user named the node (node_reference) -> keep it a variable


def _assignment_count(lines: list[str]) -> int:
    return sum(1 for line in lines if _ASSIGNMENT_RE.match(line))


def _is_simple(em: NodeEmission) -> bool:
    """True when the node is exactly one top-level assignment to its own var."""
    return bool(em.lines) and em.lines[0].startswith(f"{em.var_name} = ") and _assignment_count(em.lines) == 1


def _dedent(lines: list[str]) -> list[str]:
    return textwrap.dedent("\n".join(lines)).split("\n")


def _link_suffix(consumer: NodeEmission, producer_var: str) -> list[str] | None:
    """Return the method-chain lines of ``consumer`` applied to ``producer_var``.

    Recognises ``c = producer.method(...)`` and the paren form ``c = (producer``
    ... ``)``. Returns None (not fusable) when the head does not anchor on
    ``producer_var`` or when ``producer_var`` is referenced anywhere but the chain
    base (e.g. ``record_id``'s ``... for col in producer.columns``), since fusing
    would drop the named variable that reference relies on.
    """
    if not consumer.lines:
        return None
    head = consumer.lines[0]
    direct = f"{consumer.var_name} = {producer_var}"
    paren = f"{consumer.var_name} = ({producer_var}"
    if head.startswith(direct + "."):
        suffix = [head[len(direct) :]] + consumer.lines[1:]
    elif head == paren:
        body = consumer.lines[1:]
        if not body or body[-1].rstrip() != ")":
            return None
        suffix = _dedent(body[:-1])
    else:
        return None
    token = re.compile(r"\b" + re.escape(producer_var) + r"\b")
    if any(token.search(line) for line in suffix):
        return None
    return suffix


def _root_rhs(em: NodeEmission) -> list[str]:
    """The right-hand side of a chain-root statement (``var = `` stripped)."""
    head = em.lines[0]
    return [head[len(f"{em.var_name} = ") :]] + em.lines[1:]


def _indent(line: str) -> str:
    return f"    {line}" if line.strip() else ""


def _render_chain(chain: list[NodeEmission]) -> list[str] | None:
    """Render a producer→consumer chain as one piped expression (or None to bail)."""
    if len(chain) == 1:
        return list(chain[0].lines)
    out = [f"{chain[-1].var_name} = ("]
    out.extend(_indent(line) for line in _root_rhs(chain[0]))
    for producer, node in zip(chain, chain[1:], strict=False):
        suffix = _link_suffix(node, producer.var_name)
        if suffix is None:
            return None
        out.extend(_indent(line) for line in suffix)
    out.append(")")
    return out


def render_pipeline(emissions: list[NodeEmission], consumers: dict[int, list[int]]) -> list[str]:
    """Render all node statements, fusing linear single-use chains into pipes.

    Args:
        emissions: node statements in emission (data-flow) order.
        consumers: producer node id -> list of consumer node ids (emitted nodes only).
    """
    by_id = {em.node_id: em for em in emissions}
    simple = {em.node_id: _is_simple(em) for em in emissions}

    absorbed_into: dict[int, int] = {}
    for producer in emissions:
        if not simple[producer.node_id] or producer.num_inputs > 1 or producer.is_flow_output:
            continue
        if producer.pinned:  # user named it; keep it as its own variable
            continue
        consuming = consumers.get(producer.node_id, [])
        if len(consuming) != 1:
            continue
        consumer = by_id.get(consuming[0])
        if consumer is None or not simple[consumer.node_id]:
            continue
        if consumer.num_inputs != 1 or consumer.main_producer_id != producer.node_id:
            continue
        if _link_suffix(consumer, producer.var_name) is None:
            continue
        absorbed_into[producer.node_id] = consumer.node_id

    rendered: list[str] = []
    for em in emissions:
        if em.node_id in absorbed_into:
            continue  # emitted as part of its consumer's chain
        chain = [em]
        cur = em
        while cur.main_producer_id is not None and absorbed_into.get(cur.main_producer_id) == cur.node_id:
            cur = by_id[cur.main_producer_id]
            chain.insert(0, cur)
        block = _render_chain(chain)
        if block is None:  # defensive: fall back to separate statements
            block = [line for node in chain for line in node.lines]
        if rendered:
            rendered.append("")
        rendered.extend(block)
    return rendered
