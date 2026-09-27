"""Render p95 benchmark (plan section 7): 50 renders of a synthetic 50-node flow stay under 150 ms at p95."""

import statistics
import time

import flowfile_frame as ff
from flowfile_core.notebook.render import render

NODE_TARGET = 50
RENDERS = 50
P95_BUDGET_MS = 150.0


def _synthetic_flow():
    """A chain of filters, formulas and joins against a lookup source, grown until it holds 50 nodes."""
    base = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30], "key": [1, 2, 1]})
    graph = base.flow_graph
    lookup = ff.from_dict({"key": [1, 2], "label": ["a", "b"]}, flow_graph=graph)
    frame = base
    step = 0
    while len(graph.nodes) < NODE_TARGET:
        kind = step % 3
        if kind == 0:
            frame = frame.filter(ff.col("amount") > step)
        elif kind == 1:
            frame = frame.with_columns((ff.col("amount") * 2).alias(f"f{step}"))
        else:
            frame = frame.join(lookup.select("key", ff.col("label").alias(f"l{step}")), on="key", how="left")
        step += 1
    return graph


def test_render_p95_under_budget():
    graph = _synthetic_flow()
    assert len(graph.nodes) >= NODE_TARGET
    render(graph)
    timings = []
    for _ in range(RENDERS):
        start = time.perf_counter()
        rendering = render(graph)
        timings.append((time.perf_counter() - start) * 1000)
    assert sum(len(cell.node_ids) for cell in rendering.cells) == len(graph.nodes)
    ordered = sorted(timings)
    p95 = ordered[int(0.95 * len(ordered)) - 1]
    print(f"render p50={statistics.median(timings):.1f}ms p95={p95:.1f}ms over {len(graph.nodes)} nodes")
    assert p95 < P95_BUDGET_MS
