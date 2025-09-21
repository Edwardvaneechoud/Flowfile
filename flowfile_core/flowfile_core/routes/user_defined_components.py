
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Depends

from flowfile_core import flow_file_handler
# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
# File handling
from flowfile_core.schemas import input_schema
from flowfile_core.utils.utils import camel_case_to_snake_case

# External dependencies


router = APIRouter()


@router.get("/custom-node-schema", summary="Get a simple UI schema")
def get_simple_custom_object(flow_id: int, node_id: int):
    """
    This endpoint returns a hardcoded JSON object that represents the UI
    for our SimpleFilterNode.
    """
    try:
        node = flow_file_handler.get_node(flow_id=flow_id, node_id=node_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    user_defined_node = CUSTOM_NODE_STORE.get(node.node_type)

    if not user_defined_node:
        raise HTTPException(status_code=404, detail=f"Node type '{node.node_type}' not found")
    if node.is_setup:
        settings = node.setting_input.settings
        return user_defined_node.from_settings(settings).get_frontend_schema()
    return user_defined_node().get_frontend_schema()


@router.post("/update_user_defined_node", tags=["transform"])
def update_user_defined_node(input_data: Dict[str, Any], node_type: str, current_user=Depends(get_current_active_user)):
    input_data['user_id'] = current_user.id
    node_type = camel_case_to_snake_case(node_type)
    flow_id = int(input_data.get('flow_id'))
    logger.info(f'Updating the data for flow: {flow_id}, node {input_data["node_id"]}')
    flow = flow_file_handler.get_flow(flow_id)
    user_defined_model = CUSTOM_NODE_STORE.get(node_type)
    if not user_defined_model:
        raise HTTPException(status_code=404, detail=f"Node type '{node_type}' not found")

    user_defined_node_settings = input_schema.UserDefinedNode.model_validate(input_data)
    initialized_model = user_defined_model.from_settings(user_defined_node_settings.settings)

    flow.add_user_defined_node(custom_node=initialized_model, user_defined_node_settings=user_defined_node_settings)
