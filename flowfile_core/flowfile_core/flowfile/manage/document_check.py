"""Whether a flow document, a merged one in particular, is a flow that holds together before anyone runs it.

A merge can be clean and still broken: one side renamed a column another side's new filter reads, or a step lost an
input the other side removed. This builds the document in memory, never in a user's session and never from a file,
and asks core what a person would otherwise find at the first run:

* every reference in the document points somewhere (``shared.flow_merge.integrity``);
* it builds at all, every step's settings valid for its type;
* every step has the inputs its type needs (``FlowNode.is_correct``), such as a join with both sides;
* nothing was lost on the way in: an edge or a step the loader dropped shows as a difference between the document and
  the graph it built;
* the settings check finds no column a step reads that its input does not have (``validate_flow_settings``, which
  stays silent where it cannot be sure).

Nothing runs and nothing is read from storage beyond what the settings check's schema prediction reads.
"""

from __future__ import annotations

from pydantic import BaseModel

from shared.flow_merge.conflict import is_conflicted
from shared.flow_merge.diff import diff
from shared.flow_merge.integrity import integrity


class DocumentCheck(BaseModel):
    integrity: list[str] = []
    build_error: str | None = None
    not_wired: list[str] = []
    lost_on_load: list[str] = []
    settings: list[str] = []

    @property
    def ok(self) -> bool:
        return not (self.integrity or self.build_error or self.not_wired or self.lost_on_load or self.settings)

    def problems(self) -> list[str]:
        out = list(self.integrity)
        if self.build_error:
            out.append(f"The flow does not build: {self.build_error}")
        return out + self.not_wired + self.lost_on_load + self.settings


def check_document(document: dict) -> DocumentCheck:
    from flowfile_core.flowfile.manage.ephemeral_graph import ephemeral_document_graph
    from flowfile_core.flowfile.settings_validation import validate_flow_settings

    result = DocumentCheck(integrity=[issue.message for issue in integrity(document)])
    if is_conflicted(document):
        return result
    try:
        with ephemeral_document_graph(document) as graph:
            result.not_wired = [
                f"Step {node.node_id} {node.node_type} does not have the inputs a {node.node_type} needs."
                for node in graph.nodes
                if not node.is_correct
            ]
            rebuilt = graph.get_flowfile_data().model_dump(mode="json")
            lost = diff(document, rebuilt)
            result.lost_on_load = [
                f"Loading the flow changes {change.label}: "
                + ("it is not there" if change.kind == "removed" else ", ".join(f.path for f in change.fields))
                for change in lost.changes
                if change.item == "step" and (change.kind != "changed" or "inputs" in change.aspects)
            ]
            graph.flow_settings.validate_settings = True
            checked = validate_flow_settings(graph)
            result.settings = [
                f"Step {node.node_id}: {issue.message}" for node in checked.nodes for issue in node.issues
            ]
    except Exception as e:
        result.build_error = str(e) or type(e).__name__
    return result


def build_check(document: dict) -> list[str]:
    """``flowfile-merge check --build``: the problems core finds in ``document``, one sentence each, ``[]`` when it
    holds together.

    Registered under the ``flowfile.flow_merge`` entry point group in the root ``pyproject.toml``, where the command
    (``shared.flow_merge.cli``, which imports nothing of core) looks it up, so core is loaded only when ``--build`` is
    asked for. Loading it starts core's catalog database (``FLOWFILE_STORAGE_DIR``), so a pipeline points that at a
    scratch folder.
    """
    return check_document(document).problems()
