"""A seed plus a clean run inside this process, the way a runner in the server does it, leaves the process alone.

The rendered cells run without their ``import flowfile as fl`` line: the cell namespace brings its own ``fl``,
and importing ``flowfile`` writes the process environment. The flow holds no custom node, since loading a
custom node file imports ``flowfile`` too.
"""

import os

from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run, seed_session
from test_utils.imports import unimportable
from tests.notebook.conftest import NOTEBOOK_OWNER_ID

FLOW = "complex_workflow"


def _without_flowfile_import(code: str) -> str:
    return "\n".join(line for line in code.splitlines() if line != "import flowfile as fl")


def test_a_seed_and_clean_run_leave_the_process_environment_alone(notebook_corpus):
    graph = dict(notebook_corpus)[FLOW]
    assert not any(getattr(node.setting_input, "is_user_defined", False) for node in graph.nodes)
    rendering = render(graph)
    cells = [(cell.cell_id, _without_flowfile_import(cell.code)) for cell in rendering.cells]
    provenance = {
        cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }
    snapshot = seed_snapshot(graph)
    before = dict(os.environ)
    with unimportable("flowfile") as attempts, notebook.RUN_LOCK:
        try:
            seed_session(**snapshot, user_id=NOTEBOOK_OWNER_ID)
            result = clean_run(cells, max(node.node_id for node in graph.nodes), provenance)
        finally:
            notebook.exit()
    assert attempts == []
    assert result["ok"], result.get("error")
    assert dict(os.environ) == before
