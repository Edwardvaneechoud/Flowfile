"""Run over the committed corpus: every rendered cell runs, twice, in a session seeded from its flow."""

from __future__ import annotations

import json

from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import execute_cell, new_namespace, seed_session


def test_every_rendered_cell_runs_twice_in_its_seeded_session(notebook_corpus):
    failed: dict[str, list[str]] = {}
    for name, graph in notebook_corpus:
        snapshot = seed_snapshot(graph)
        cells = render(graph).cells
        try:
            namespace = {**new_namespace(), **seed_session(**snapshot, user_id=1)}
            for attempt in (1, 2):
                for cell in cells:
                    result = execute_cell(cell.cell_id, cell.code, namespace)
                    if not result.ok:
                        failed.setdefault(name, []).append(f"{attempt}:{cell.cell_id}: {result.error.splitlines()[-1]}")
        finally:
            if notebook.current() is not None:
                notebook.exit()
    assert not failed, json.dumps(failed, indent=1)
