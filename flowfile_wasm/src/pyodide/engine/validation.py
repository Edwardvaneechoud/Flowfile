from typing import Any

SETTING_INPUT_EXCLUDE = {
    "flow_id",
    "node_id",
    "pos_x",
    "pos_y",
    "is_setup",
    "description",
    "user_id",
    "is_flow_output",
    "is_user_defined",
    "depending_on_id",
    "depending_on_ids",
}


_pydantic_loaded = False


_FlowfileData = None


def _load_pydantic():
    """Lazy load pydantic and define validation models."""
    global _pydantic_loaded, _FlowfileData
    if _pydantic_loaded:
        return _FlowfileData is not None

    try:
        from typing import Literal

        from pydantic import BaseModel, Field

        ExecutionModeLiteral = Literal["Development", "Performance"]
        ExecutionLocationsLiteral = Literal["local", "remote"]

        class FlowfileSettings(BaseModel):
            description: str | None = None
            execution_mode: ExecutionModeLiteral = "Performance"
            execution_location: ExecutionLocationsLiteral = "local"
            auto_save: bool = False
            show_detailed_progress: bool = True

        class FlowfileNode(BaseModel):
            id: int
            type: str
            is_start_node: bool = False
            description: str | None = ""
            x_position: float | None = 0
            y_position: float | None = 0
            left_input_id: int | None = None
            right_input_id: int | None = None
            input_ids: list[int] | None = Field(default_factory=list)
            outputs: list[int] | None = Field(default_factory=list)
            setting_input: Any | None = None

        class FlowfileData(BaseModel):
            flowfile_version: str
            flowfile_id: int
            flowfile_name: str
            flowfile_settings: FlowfileSettings
            nodes: list[FlowfileNode]

        _FlowfileData = FlowfileData
        _pydantic_loaded = True
        return True
    except ImportError:
        _pydantic_loaded = True
        return False


def validate_flowfile_data(data: dict) -> dict:
    """Validate flowfile data using Pydantic schemas (lazy loaded).

    Returns a dict with:
    - success: bool
    - data: validated data (if successful)
    - error: error message (if failed)
    """
    # Try to lazy-load pydantic
    if not _load_pydantic():
        # Pydantic not available - skip validation, assume valid
        return {"success": True, "data": data, "error": None}

    try:
        validated = _FlowfileData.model_validate(data)
        return {"success": True, "data": validated.model_dump(), "error": None}
    except Exception as e:
        return {"success": False, "data": None, "error": str(e)}


def clean_setting_input(settings: dict) -> dict:
    """Clean setting_input by removing excluded fields."""
    if settings is None:
        return None
    return {k: v for k, v in settings.items() if k not in SETTING_INPUT_EXCLUDE}
