
import ast
import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from flowfile_core import flow_file_handler
# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import (
    CUSTOM_NODE_STORE,
    add_to_custom_node_store,
    remove_from_custom_node_store,
    load_single_node_from_file,
)
# File handling
from flowfile_core.schemas import input_schema
from flowfile_core.utils.utils import camel_case_to_snake_case
from shared import storage

# External dependencies


router = APIRouter()


class CustomNodeInfo(BaseModel):
    """Info about a custom node file."""
    file_name: str
    node_name: str = ""
    node_category: str = ""
    title: str = ""
    intro: str = ""


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
    print('adding user defined node')
    print(input_data)
    print('-----')
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

    # Try to load and register the node using the centralized loader
    try:
        node_class = load_single_node_from_file(file_path)
        if node_class:
            add_to_custom_node_store(node_class)
            logger.info(f"Registered custom node: {node_class().node_name}")
    except Exception as e:
        logger.warning(f"Node saved but failed to load: {e}")
        # Don't fail the request - the file is saved, it just couldn't be loaded yet

    return {
        "success": True,
        "file_name": safe_name,
        "message": f"Node saved successfully to {safe_name}"
    }


def _extract_node_info_from_file(file_path: Path) -> CustomNodeInfo:
    """Extract node metadata from a Python file by parsing its AST."""
    info = CustomNodeInfo(file_name=file_path.name)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content)

        # Find class definitions that might be custom nodes
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Look for class attributes
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                attr_name = target.id
                                # Extract string values
                                if isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
                                    value = item.value.value
                                    if attr_name == "node_name":
                                        info.node_name = value
                                    elif attr_name == "node_category":
                                        info.node_category = value
                                    elif attr_name == "title":
                                        info.title = value
                                    elif attr_name == "intro":
                                        info.intro = value

                # If we found a node_name, this is likely a custom node class
                if info.node_name:
                    break

    except Exception as e:
        logger.warning(f"Failed to parse node info from {file_path}: {e}")

    return info


@router.get("/list-custom-nodes", summary="List all custom nodes", response_model=List[CustomNodeInfo])
def list_custom_nodes() -> List[CustomNodeInfo]:
    """
    List all custom node Python files in the user-defined nodes directory.
    Returns basic metadata extracted from each file.
    """
    nodes_dir = storage.user_defined_nodes_directory
    nodes: List[CustomNodeInfo] = []

    if not nodes_dir.exists():
        return nodes

    for file_path in nodes_dir.glob("*.py"):
        if file_path.name.startswith("_"):
            continue  # Skip private files
        info = _extract_node_info_from_file(file_path)
        nodes.append(info)

    # Sort by node name
    nodes.sort(key=lambda x: x.node_name or x.file_name)
    return nodes


@router.get("/get-custom-node/{file_name}", summary="Get custom node details")
def get_custom_node(file_name: str) -> Dict[str, Any]:
    """
    Get the full content and parsed metadata of a custom node file.
    This endpoint is used by the Node Designer to load an existing node for editing.
    """
    # Sanitize file name
    if not file_name.endswith('.py'):
        file_name += '.py'

    safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', file_name)
    file_path = storage.user_defined_nodes_directory / safe_name

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Node file '{safe_name}' not found")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")

    # Parse the file to extract metadata and sections
    result = {
        "file_name": safe_name,
        "content": content,
        "metadata": {},
        "sections": [],
        "processCode": ""
    }

    try:
        tree = ast.parse(content)

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if this looks like a custom node class (has node_name attribute)
                is_custom_node = False
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name) and target.id == "node_name":
                                is_custom_node = True
                                break

                if is_custom_node:
                    # Extract metadata
                    for item in node.body:
                        if isinstance(item, ast.Assign):
                            for target in item.targets:
                                if isinstance(target, ast.Name):
                                    attr_name = target.id
                                    if isinstance(item.value, ast.Constant):
                                        value = item.value.value
                                        if attr_name in ["node_name", "node_category", "title", "intro"]:
                                            result["metadata"][attr_name] = value
                                        elif attr_name == "number_of_inputs":
                                            result["metadata"]["number_of_inputs"] = value
                                        elif attr_name == "number_of_outputs":
                                            result["metadata"]["number_of_outputs"] = value

                    # Extract process method
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef) and item.name == "process":
                            # Get the source code of the process method
                            start_line = item.lineno - 1
                            end_line = item.end_lineno if hasattr(item, 'end_lineno') else start_line + 20
                            lines = content.split('\n')
                            process_lines = lines[start_line:end_line]
                            result["processCode"] = '\n'.join(process_lines)
                            break

                    break

    except Exception as e:
        logger.warning(f"Failed to parse custom node file: {e}")
        # Return the raw content even if parsing fails

    return result


@router.delete("/delete-custom-node/{file_name}", summary="Delete a custom node")
def delete_custom_node(file_name: str) -> Dict[str, Any]:
    """
    Delete a custom node Python file from the user-defined nodes directory.
    This also attempts to unregister the node from the node store.
    """
    # Sanitize file name
    if not file_name.endswith('.py'):
        file_name += '.py'

    safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', file_name)
    file_path = storage.user_defined_nodes_directory / safe_name

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Node file '{safe_name}' not found")

    # Try to find and unregister the node from all stores
    try:
        info = _extract_node_info_from_file(file_path)
        file_stem = file_path.stem  # filename without .py extension
        logger.info(f"Extracted node info: node_name='{info.node_name}', file_name='{info.file_name}', file_stem='{file_stem}'")

        # Use the centralized remove function which cleans up all stores
        # Pass both the computed key from node_name and the file_stem as fallback
        if info.node_name:
            node_type_key = info.node_name.lower().replace(' ', '_')
            logger.info(f"Computed node_type_key: '{node_type_key}'")
        else:
            node_type_key = file_stem
            logger.info(f"Using file_stem as node_type_key: '{node_type_key}'")

        if remove_from_custom_node_store(node_type_key, file_stem=file_stem):
            logger.info(f"Unregistered custom node: {info.node_name or file_stem}")
        else:
            logger.warning(f"Node '{node_type_key}' was not found in stores during unregister")
    except Exception as e:
        logger.warning(f"Could not unregister node: {e}")

    # Delete the file
    try:
        file_path.unlink()
        logger.info(f"Deleted custom node file: {file_path}")
    except Exception as e:
        logger.error(f"Failed to delete custom node file: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

    return {
        "success": True,
        "file_name": safe_name,
        "message": f"Node '{safe_name}' deleted successfully"
    }
