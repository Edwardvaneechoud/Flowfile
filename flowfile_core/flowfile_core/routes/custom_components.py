from typing import Any
from typing import List

import polars as pl
from fastapi import APIRouter

from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, Section, NodeSettings, to_frontend_schema
from pydantic import BaseModel
from custom_nodes.custom_node_definitions import SimpleFilterNode


# --- Define a Pydantic model for the response ---
# This ensures the API response has a predictable structure and is properly documented.
class UINodeSchema(BaseModel):
    main_config: Section
    advanced_options: Section


router = APIRouter()


@router.get("/custom-node-schema", summary="Get a simple UI schema")
def get_simple_custom_object():
    """
    This endpoint returns a hardcoded JSON object that represents the UI
    for our SimpleFilterNode.
    """
    return SimpleFilterNode().get_frontend_schema()


