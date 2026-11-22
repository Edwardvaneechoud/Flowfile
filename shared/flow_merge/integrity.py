"""What a flow document must hold for every reference in it to point somewhere (pure, no core).

A load drops an edge from a step that is not in the flow without a word, so a merge that left one would run as a
different flow than the one reviewed. This names each such problem: open conflicts, edges the two ends do not agree
on, a step reading itself through a cycle, two steps with one reference, a step in a group that is not there, and a
model step naming a training step that is not there.
"""

from __future__ import annotations

from collections.abc import Mapping

from shared.flow_merge.canonical import Issue, canonical
from shared.flow_merge.conflict import is_conflicted, open_conflicts

_UPSTREAM_IDS = (("apply_input", "upstream_node_id"), ("evaluate_input", "upstream_train_node_id"))


def integrity(document: Mapping) -> list[Issue]:
    if is_conflicted(document):
        count = len(open_conflicts(document))
        return [
            Issue("open_conflicts", f"The flow holds {count} merge conflict{'s' if count != 1 else ''} not settled.")
        ]
    flow, issues = canonical(document)
    looped = _in_or_after_cycles(flow)
    if looped:
        issues.append(
            Issue("cycle", f"Steps {', '.join(map(str, sorted(looped)))} feed each other in a cycle.", tuple(looped))
        )
    references: dict[str, int] = {}
    for sid, node in flow.steps.items():
        ref = node.get("node_reference")
        if ref and ref in references:
            issues.append(
                Issue(
                    "duplicate_reference",
                    f"Steps {references[ref]} and {sid} are both called {ref!r}.",
                    (references[ref], sid),
                )
            )
        elif ref:
            references[ref] = sid
        group = node.get("group_id")
        if group is not None and group not in flow.groups:
            issues.append(Issue("missing_group", f"Step {sid} is in group {group}, which is not in the flow.", (sid,)))
        settings = node.get("setting_input")
        for key, field in _UPSTREAM_IDS:
            upstream = (settings or {}).get(key, {}) if isinstance(settings, dict) else {}
            target = upstream.get(field) if isinstance(upstream, dict) else None
            if isinstance(target, int) and target and target not in flow.steps:
                issues.append(
                    Issue(
                        "missing_upstream", f"Step {sid} names step {target}, which is not in the flow.", (sid, target)
                    )
                )
    return issues


def _sources(flow, step_id: int) -> set[int]:
    return {s for edges in flow.inputs.get(step_id, {}).values() for s, _ in edges}


def _in_or_after_cycles(flow) -> list[int]:
    """The steps that can never run because a cycle feeds them, sorted."""
    parents = {sid: _sources(flow, sid) & set(flow.steps) for sid in flow.steps}
    ready: set[int] = set()
    grew = True
    while grew:
        grew = False
        for sid, sources in parents.items():
            if sid not in ready and sources <= ready:
                ready.add(sid)
                grew = True
    return sorted(set(flow.steps) - ready)
