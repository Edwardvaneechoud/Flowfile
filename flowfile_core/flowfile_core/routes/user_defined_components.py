
import ast
import re
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from flowfile_core import flow_file_handler
# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, add_to_custom_node_store
# File handling
from flowfile_core.schemas import input_schema
from flowfile_core.utils.utils import camel_case_to_snake_case
from shared import storage

# External dependencies


router = APIRouter()


class SaveCustomNodeRequest(BaseModel):
    """Request model for saving a custom node."""
    file_name: str
    code: str


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


@router.post("/save-custom-node", summary="Save a custom node definition")
def save_custom_node(request: SaveCustomNodeRequest):
    """
    Save a custom node Python file to the user-defined nodes directory.

    This endpoint:
    1. Validates the Python syntax
    2. Ensures the file name is safe
    3. Writes the file to the user_defined_nodes directory
    4. Attempts to load and register the new node
    """
    # Validate file name
    file_name = request.file_name
    if not file_name.endswith('.py'):
        file_name += '.py'

    # Sanitize file name - only allow alphanumeric, underscore, and .py extension
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name[:-3]) + '.py'
    if not safe_name or safe_name == '.py':
        raise HTTPException(status_code=400, detail="Invalid file name")

    # Validate Python syntax
    try:
        ast.parse(request.code)
    except SyntaxError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Python syntax error at line {e.lineno}: {e.msg}"
        )

    # Get the directory path
    nodes_dir = storage.user_defined_nodes_directory
    file_path = nodes_dir / safe_name

    # Write the file
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(request.code)
        logger.info(f"Saved custom node to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save custom node: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Try to load and register the node
    try:
        import importlib.util
        import inspect
        from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase

        module_name = safe_name[:-3]  # Remove .py
        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find CustomNodeBase subclasses
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and
                    issubclass(obj, CustomNodeBase) and
                    obj is not CustomNodeBase):
                    # Register the node
                    add_to_custom_node_store(obj)
                    logger.info(f"Registered custom node: {obj().node_name}")

    except Exception as e:
        logger.warning(f"Node saved but failed to load: {e}")
        # Don't fail the request - the file is saved, it just couldn't be loaded yet

    return {
        "success": True,
        "file_name": safe_name,
        "message": f"Node saved successfully to {safe_name}"
    }
