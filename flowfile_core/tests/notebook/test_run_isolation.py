"""A seed plus a clean run inside this process, the way a runner in the server does it, leaves the process alone.

Through each runner's executor. The interpreter reads the rendered cells as they are: ``import flowfile as ff``
binds the cell namespace's own ``ff`` and imports nothing. For ``exec`` the cells run without that line, since
importing ``flowfile`` writes the process environment. The flow holds no custom node, since loading a custom
node file imports ``flowfile`` too.
"""

import os

from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run, seed_session
from test_utils.imports import unimportable
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, RUNNERS, cell_provenance

FLOW = "complex_workflow"


def _without_flowfile_import(code: str) -> str:
    return "\n".join(line for line in code.splitlines() if line != "import flowfile as ff")


def test_a_seed_and_clean_run_leave_the_process_environment_alone(notebook_corpus, runner_kind):
    graph = dict(notebook_corpus)[FLOW]
    assert not any(getattr(node.setting_input, "is_user_defined", False) for node in graph.nodes)
    rendering = render(graph)
    keep = (lambda code: code) if runner_kind == "interpreting" else _without_flowfile_import
    cells = [(cell.cell_id, keep(cell.code)) for cell in rendering.cells]
    assert any("import flowfile as ff" in code for _, code in cells) == (runner_kind == "interpreting")
    provenance = cell_provenance(graph, rendering)
    snapshot = seed_snapshot(graph)
    before = dict(os.environ)
    with unimportable("flowfile") as attempts, notebook.RUN_LOCK:
        try:
            seed_session(**snapshot, user_id=NOTEBOOK_OWNER_ID)
            ceiling = max(node.node_id for node in graph.nodes)
            result = clean_run(cells, ceiling, provenance, executor=RUNNERS[runner_kind].executor())
        finally:
            notebook.exit()
    assert attempts == []
    assert result["ok"], result.get("error")
    assert dict(os.environ) == before
