"""HTTP routes for the ML metadata API."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from flowfile_core import flow_file_handler
from shared.ml.algorithms import MLAlgorithmSpec, get_algorithm_specs

router = APIRouter(prefix="/ml", tags=["ml"])


class UpstreamTrainModelOption(BaseModel):
    """One Train Model node available upstream of an Apply Model node."""

    model_config = ConfigDict(protected_namespaces=())

    node_id: int
    description: str = ""
    target_column: str = ""
    feature_columns: list[str] = Field(default_factory=list)
    model_type: str = ""
    publish_to_catalog: bool = False
    model_name: str = ""


@router.get(
    "/algorithms",
    response_model=list[MLAlgorithmSpec],
    summary="List available ML algorithms",
    description=(
        "Returns the registered ML algorithms and their hyperparameter specs. "
        "The frontend uses this to render the dynamic params form on the "
        "Train Model node — adding a new algorithm is a backend-only change."
    ),
)
def list_algorithms() -> list[MLAlgorithmSpec]:
    return get_algorithm_specs()


@router.get(
    "/upstream-train-models",
    response_model=list[UpstreamTrainModelOption],
    summary="List Train Model nodes upstream of a given node",
    description=(
        "Walks the flow graph backwards from *node_id* and returns every "
        "Train Model node it can reach. The Apply Model drawer uses this to "
        "populate its 'Upstream training node' picker so users can wire up "
        "train→apply at design time without first running the flow."
    ),
)
def list_upstream_train_models(flow_id: int, node_id: int) -> list[UpstreamTrainModelOption]:
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, f"Flow {flow_id} not found")
    starting_node = flow.get_node(node_id)
    if starting_node is None:
        raise HTTPException(404, f"Node {node_id} not found in flow {flow_id}")

    # Strict ancestor walk: only follow an edge (parent -> child) if BOTH
    # endpoints agree on it — parent.leads_to_nodes contains child AND
    # child.all_inputs contains parent. Disconnect bugs that leave one side
    # dangling won't surface phantom Train Models in the picker.
    forward_parents: dict[int, set[int]] = {}
    for n in flow.nodes:
        for c in n.leads_to_nodes:
            forward_parents.setdefault(c.node_id, set()).add(n.node_id)

    ancestor_ids: set[int] = set()
    queue: list[int] = [node_id]
    while queue:
        current_id = queue.pop(0)
        current = flow.get_node(current_id)
        if current is None:
            continue
        backward_parent_ids = {p.node_id for p in current.all_inputs}
        agreed = forward_parents.get(current_id, set()) & backward_parent_ids
        for pid in agreed:
            if pid in ancestor_ids:
                continue
            ancestor_ids.add(pid)
            queue.append(pid)

    train_nodes: list[UpstreamTrainModelOption] = []
    for aid in sorted(ancestor_ids):
        upstream = flow.get_node(aid)
        if upstream is None or upstream.node_type != "train_model":
            continue
        s = getattr(upstream.setting_input, "train_input", None)
        train_nodes.append(
            UpstreamTrainModelOption(
                node_id=upstream.node_id,
                description=getattr(upstream.setting_input, "description", "") or "",
                target_column=getattr(s, "target_column", "") or "",
                feature_columns=list(getattr(s, "feature_columns", []) or []),
                model_type=getattr(s, "model_type", "") or "",
                publish_to_catalog=bool(getattr(s, "publish_to_catalog", False)),
                model_name=getattr(s, "model_name", "") or "",
            )
        )
    return train_nodes
