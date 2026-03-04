"""API routes for subflow node management.

Allows registering saved flows as reusable custom nodes, listing them,
and extracting their arguments for preview.
"""

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from flowfile_core.schemas.flow_args import FlowArgument
from flowfile_core.schemas.schemas import NodeTemplate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subflow", tags=["subflow"])


class SubflowRegisterRequest(BaseModel):
    flow_path: str


class SubflowRegisterResponse(BaseModel):
    item: str
    node_template: NodeTemplate
    flow_arguments: list[FlowArgument]


class SubflowListItem(BaseModel):
    item: str
    name: str
    flow_path: str
    flow_arguments: list[FlowArgument]


@router.post("/register", response_model=SubflowRegisterResponse)
def register_subflow(request: SubflowRegisterRequest):
    """Register a saved flow as a subflow custom node."""
    from flowfile_core.configs.node_store import register_subflow_node

    path = Path(request.flow_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Flow file not found: {request.flow_path}")

    try:
        node = register_subflow_node(str(path))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to register subflow: {e}") from e

    return SubflowRegisterResponse(
        item=node.item,
        node_template=node.to_node_template(),
        flow_arguments=node.flow_arguments,
    )


@router.get("/list", response_model=list[SubflowListItem])
def list_subflow_nodes():
    """List all registered subflow nodes."""
    from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
    from flowfile_core.flowfile.node_designer.subflow_node import SubflowNode

    items = []
    for key, node_cls in CUSTOM_NODE_STORE.items():
        try:
            instance = node_cls()
            if isinstance(instance, SubflowNode):
                items.append(SubflowListItem(
                    item=key,
                    name=instance.node_name,
                    flow_path=instance.flow_path,
                    flow_arguments=instance.flow_arguments,
                ))
        except Exception:
            continue
    return items


@router.delete("/{node_item}")
def unregister_subflow(node_item: str):
    """Unregister a subflow node."""
    from flowfile_core.configs.node_store import remove_from_custom_node_store

    removed = remove_from_custom_node_store(node_item)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Subflow node '{node_item}' not found")
    return {"success": True, "removed": node_item}


@router.get("/arguments", response_model=list[FlowArgument])
def get_flow_arguments(flow_path: str = Query(..., description="Path to the flow file")):
    """Extract and return flow arguments from a flow file without registering it."""
    from flowfile_core.flowfile.manage.io_flowfile import _load_flow_storage, _validate_flow_path

    path = Path(flow_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Flow file not found: {flow_path}")

    try:
        validated_path = _validate_flow_path(path)
        flow_info = _load_flow_storage(validated_path)
        return flow_info.flow_settings.flow_arguments
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read flow arguments: {e}") from e
