import ast
import hashlib
import re
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
from flowfile_core.flowfile.node_designer.codegen import CodegenError, generate_source
from flowfile_core.flowfile.node_designer.parsing import parse_source
from flowfile_core.flowfile.node_designer.state import DesignerState, ParseResult
from flowfile_core.flowfile.user_defined.dry_run import DryRunRequest, DryRunResponse, run_dry_run
from flowfile_core.flowfile.user_defined.registry import KernelRequiredError, LoadedNode, registry
from flowfile_core.schemas import input_schema
from flowfile_core.utils.utils import camel_case_to_snake_case
from shared import storage

router = APIRouter()


class CustomNodeInfo(BaseModel):
    """Info about a custom node file."""

    file_name: str
    node_name: str = ""
    node_category: str = ""
    title: str = ""
    intro: str = ""
    node_icon: str = "user-defined-icon.png"
    node_key: str = ""
    environment: Literal["local", "kernel"] = "local"
    source_hash: str = ""
    error: str | None = None


class SaveCustomNodeRequest(BaseModel):
    """Request model for saving a custom node.

    ``mode`` defaults to ``"code"`` so the legacy ``{file_name, code}`` body keeps
    working. In designer mode the source is regenerated from ``designer_state``.
    """

    file_name: str
    mode: Literal["designer", "code"] = "code"
    designer_state: DesignerState | None = None
    code: str | None = None
    expected_hash: str | None = None


class PreviewCustomNodeRequest(BaseModel):
    """Generate canonical source from a DesignerState without writing anything."""

    designer_state: DesignerState


def _safe_file_name(file_name: str) -> str:
    if not file_name.endswith(".py"):
        file_name += ".py"
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name[:-3]) + ".py"
    if not safe_name or safe_name == ".py":
        raise HTTPException(status_code=400, detail="Invalid file name")
    return safe_name


def _parse_result_for_file(path: Path, content: str) -> ParseResult:
    try:
        return parse_source(content, file_name=path.name)
    except Exception as e:  # parser is defensive, but never fail the route on it
        logger.warning("Failed to parse designer state for %s: %s", path.name, e)
        return ParseResult(mode="code_only", designer_state=None, issues=[])


def _extract_node_info_from_file(file_path: Path) -> CustomNodeInfo:
    """Extract node metadata from a Python file by parsing its AST (no code execution)."""
    info = CustomNodeInfo(file_name=file_path.name)

    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    attr_name = None
                    value = None

                    if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                        attr_name = item.target.id
                        if item.value and isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
                            value = item.value.value
                    elif isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                attr_name = target.id
                                if isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
                                    value = item.value.value
                                break

                    if attr_name and value:
                        if attr_name == "node_name":
                            info.node_name = value
                        elif attr_name == "node_category":
                            info.node_category = value
                        elif attr_name == "title":
                            info.title = value
                        elif attr_name == "intro":
                            info.intro = value
                        elif attr_name == "node_icon":
                            info.node_icon = value

                if info.node_name:
                    break

    except Exception as e:
        logger.warning(f"Failed to parse node info from {file_path}: {e}")

    return info


def _node_info_from_entry(entry: LoadedNode) -> CustomNodeInfo:
    if entry.node_class is None:
        # Broken file: best-effort AST metadata so the browser still names it.
        info = _extract_node_info_from_file(entry.file_path)
        info.node_key = entry.node_key
        info.source_hash = entry.source_hash
        info.error = entry.error
        return info
    node = entry.node_class()
    return CustomNodeInfo(
        file_name=entry.file_name,
        node_name=node.node_name,
        node_category=node.node_category,
        title=node.title or "",
        intro=node.intro or "",
        node_icon=node.node_icon,
        node_key=entry.node_key,
        environment=node.environment,
        source_hash=entry.source_hash,
        error=None,
    )


@router.get("/custom-node-schema", summary="Get the UI schema for a custom node instance")
def get_simple_custom_object(flow_id: int, node_id: int, current_user=Depends(get_current_active_user)):
    """Return the frontend schema (with current values) for a user-defined node in a flow."""
    try:
        node = flow_file_handler.get_node(flow_id=flow_id, node_id=node_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    user_defined_node = CUSTOM_NODE_STORE.get(node.node_type)

    if not user_defined_node:
        entry = registry.get(node.node_type)
        if entry is not None and entry.error:
            raise HTTPException(
                status_code=409,
                detail={"error": entry.error, "node_item": node.node_type},
            )
        raise HTTPException(
            status_code=404,
            detail={"error": f"Node type '{node.node_type}' not found", "node_item": node.node_type},
        )

    instance = user_defined_node()
    drift: dict[str, list[str]] | None = None
    if node.is_setup:
        stored_settings = node.setting_input.settings
        if instance.settings_schema is not None and stored_settings:
            report = instance.settings_schema.populate_values_report(stored_settings)
            if report.has_drift:
                drift = {
                    "unknown_sections": report.unknown_sections,
                    "unknown_components": report.unknown_components,
                }
    schema = instance.get_frontend_schema()
    schema["drift"] = drift
    return schema


@router.post("/update_user_defined_node", tags=["transform"])
def update_user_defined_node(input_data: dict[str, Any], node_type: str, current_user=Depends(get_current_active_user)):
    input_data["user_id"] = current_user.id
    node_type = camel_case_to_snake_case(node_type)
    flow_id = int(input_data.get("flow_id"))
    logger.info(f'Updating the data for flow: {flow_id}, node {input_data["node_id"]}')
    flow = flow_file_handler.get_flow(flow_id)
    user_defined_model = CUSTOM_NODE_STORE.get(node_type)
    if not user_defined_model:
        raise HTTPException(status_code=404, detail=f"Node type '{node_type}' not found")
    user_defined_node_settings = input_schema.UserDefinedNode.model_validate(input_data)
    initialized_model = user_defined_model.from_settings(user_defined_node_settings.settings)

    try:
        flow.add_user_defined_node(custom_node=initialized_model, user_defined_node_settings=user_defined_node_settings)
    except KernelRequiredError as e:
        raise HTTPException(
            status_code=422,
            detail={"error_code": "KERNEL_REQUIRED", "node_type": e.node_type, "message": str(e)},
        ) from e


def _render_save_source(request: SaveCustomNodeRequest) -> str:
    """Resolve the source to write. Designer mode regenerates it canonically and
    asserts the codegen round-trip invariant (a violation is a codegen bug → 500)."""
    if request.mode == "designer":
        if request.designer_state is None:
            raise HTTPException(status_code=400, detail="designer_state is required when mode='designer'")
        try:
            source = generate_source(request.designer_state)
        except CodegenError as e:
            raise HTTPException(status_code=422, detail=f"Cannot generate node source: {e}") from e
        try:
            ast.parse(source)
        except SyntaxError as e:  # generated code must always parse
            logger.error("Codegen produced invalid Python (line %s): %s", e.lineno, e.msg)
            raise HTTPException(status_code=500, detail="Generated node source is not valid Python") from e
        round_trip = parse_source(source)
        if round_trip.mode != "designer":
            codes = [issue.code for issue in round_trip.issues]
            logger.error("Codegen round-trip invariant broken: generated source parsed as code_only %s", codes)
            raise HTTPException(status_code=500, detail="Generated node source is not designer-editable (codegen bug)")
        return source

    if request.code is None:
        raise HTTPException(status_code=400, detail="code is required when mode='code'")
    try:
        ast.parse(request.code)
    except SyntaxError as e:
        raise HTTPException(status_code=400, detail=f"Python syntax error at line {e.lineno}: {e.msg}") from e
    return request.code


@router.post("/save-custom-node", summary="Save a custom node definition")
def save_custom_node(request: SaveCustomNodeRequest, current_user=Depends(get_current_active_user)):
    """Write a custom node .py file and hot-load it into the registry.

    Designer mode regenerates canonical source from the DesignerState; code mode
    writes the supplied source verbatim. A file that saves but fails to load is
    still a success — ``load_error`` carries the broken state. When
    ``expected_hash`` is set and no longer matches the on-disk file, responds 409.
    """
    safe_name = _safe_file_name(request.file_name)
    source = _render_save_source(request)

    nodes_dir = storage.user_defined_nodes_directory
    file_path = nodes_dir / safe_name

    if request.expected_hash is not None and file_path.exists():
        current_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()
        if current_hash != request.expected_hash:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "The node file changed on disk since it was opened.",
                    "current_hash": current_hash,
                },
            )

    try:
        file_path.write_text(source, encoding="utf-8")
        logger.info("Saved custom node to %s", file_path)
    except Exception as e:
        logger.error("Failed to save custom node: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}") from e

    entry = registry.load_file(file_path)
    written = file_path.read_bytes()

    return {
        "success": True,
        "file_name": safe_name,
        "message": f"Node saved successfully to {safe_name}",
        "node": _node_info_from_entry(entry).model_dump(),
        "parse_result": _parse_result_for_file(file_path, source).model_dump(),
        "code": source,
        "file_hash": hashlib.sha256(written).hexdigest(),
        "load_error": entry.error,
    }


@router.post("/preview-custom-node", summary="Render canonical source for a DesignerState")
def preview_custom_node(
    request: PreviewCustomNodeRequest, current_user=Depends(get_current_active_user)
) -> dict[str, str]:
    """Generate canonical Python from a DesignerState without writing (powers the diff preview)."""
    try:
        return {"code": generate_source(request.designer_state)}
    except CodegenError as e:
        raise HTTPException(status_code=422, detail=f"Cannot generate node source: {e}") from e


@router.post("/dry-run", summary="Run a candidate custom node against sample inputs")
def dry_run_custom_node(request: DryRunRequest, current_user=Depends(get_current_active_user)) -> DryRunResponse:
    """Execute a candidate node in the worker against bounded sample inputs.

    Never runs user code in core; a syntax error is caught before dispatch and an
    unreachable worker surfaces as ``error_kind='load'``.
    """
    return run_dry_run(request, current_user.id)


@router.get("/list-custom-nodes", summary="List all custom nodes", response_model=list[CustomNodeInfo])
def list_custom_nodes(current_user=Depends(get_current_active_user)) -> list[CustomNodeInfo]:
    """List all registered custom nodes, including files that failed to load (with their error)."""
    nodes = [_node_info_from_entry(entry) for entry in registry.all() if not entry.file_name.startswith("_")]
    nodes.sort(key=lambda x: x.node_name or x.file_name)
    return nodes


@router.get("/get-custom-node/{file_name}", summary="Get custom node details")
def get_custom_node(file_name: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """
    Get the full content and parsed metadata of a custom node file.
    This endpoint is used by the Node Designer to load an existing node for editing.
    """
    if not file_name.endswith(".py"):
        file_name += ".py"

    safe_name = re.sub(r"[^a-zA-Z0-9_.]", "_", file_name)
    file_path = storage.user_defined_nodes_directory / safe_name

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Node file '{safe_name}' not found")

    try:
        content_bytes = file_path.read_bytes()
        content = content_bytes.decode("utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}") from e

    parse_result = _parse_result_for_file(file_path, content)

    return {
        "file_name": safe_name,
        "code": content,
        "parse_result": parse_result.model_dump(),
        "file_hash": hashlib.sha256(content_bytes).hexdigest(),
        "designer_state": parse_result.designer_state.model_dump() if parse_result.designer_state else None,
        "supports_visual_edit": parse_result.mode == "designer",
        # Legacy alias for `code`; pre-designer-rewrite callers read `content`.
        "content": content,
    }


@router.delete("/delete-custom-node/{file_name}", summary="Delete a custom node")
def delete_custom_node(file_name: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """Delete a custom node file and unregister it from the registry and the palette."""
    if not file_name.endswith(".py"):
        file_name += ".py"

    safe_name = re.sub(r"[^a-zA-Z0-9_.]", "_", file_name)
    file_path = storage.user_defined_nodes_directory / safe_name

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Node file '{safe_name}' not found")

    removed = registry.remove_file(safe_name)
    if removed is not None:
        logger.info(f"Unregistered custom node '{removed.node_key}' from {safe_name}")
    else:
        logger.warning(f"File '{safe_name}' was not present in the custom node registry")

    try:
        file_path.unlink()
        logger.info(f"Deleted custom node file: {file_path}")
    except Exception as e:
        logger.error(f"Failed to delete custom node file: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}") from e

    return {"success": True, "file_name": safe_name, "message": f"Node '{safe_name}' deleted successfully"}


@router.post("/rescan", summary="Rescan the custom nodes directory")
def rescan_custom_nodes(current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """Reload every custom node file from disk (picks up manually dropped files, no restart needed)."""
    entries = registry.scan()
    broken = [entry for entry in entries if entry.error]
    return {
        "total": len(entries),
        "loaded": len(entries) - len(broken),
        "broken": [{"file_name": entry.file_name, "error": entry.error} for entry in broken],
    }


# ==================== Custom Icon Endpoints ====================


class IconInfo(BaseModel):
    """Info about a custom icon file."""

    file_name: str
    is_custom: bool = True


ALLOWED_ICON_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp"}
MAX_ICON_SIZE = 5 * 1024 * 1024  # 5MB


@router.get("/list-icons", summary="List all available icons", response_model=list[IconInfo])
def list_icons(current_user=Depends(get_current_active_user)) -> list[IconInfo]:
    """
    List all icon files available for custom nodes.
    Returns icons from the user_defined_nodes/icons directory.
    """
    icons_dir = storage.user_defined_nodes_icons
    icons: list[IconInfo] = []

    if not icons_dir.exists():
        return icons

    for file_path in icons_dir.iterdir():
        if file_path.is_file() and file_path.suffix.lower() in ALLOWED_ICON_EXTENSIONS:
            icons.append(IconInfo(file_name=file_path.name, is_custom=True))

    icons.sort(key=lambda x: x.file_name.lower())
    return icons


@router.post("/upload-icon", summary="Upload a custom icon")
async def upload_icon(file: UploadFile = File(...), current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """
    Upload a new icon file to the user_defined_nodes/icons directory.

    Accepts PNG, JPG, JPEG, SVG, GIF, and WebP files up to 5MB.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_ICON_EXTENSIONS:
        raise HTTPException(
            status_code=400, detail=f"Invalid file type. Allowed types: {', '.join(ALLOWED_ICON_EXTENSIONS)}"
        )

    content = await file.read()

    if len(content) > MAX_ICON_SIZE:
        raise HTTPException(
            status_code=400, detail=f"File too large. Maximum size is {MAX_ICON_SIZE // (1024 * 1024)}MB"
        )

    # Sanitize filename - preserve hyphens, dots, and underscores
    safe_name = re.sub(r"[^a-zA-Z0-9_.\-]", "_", file.filename)
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid file name")

    icons_dir = storage.user_defined_nodes_icons

    icons_dir.mkdir(parents=True, exist_ok=True)

    file_path = icons_dir / safe_name

    try:
        with open(file_path, "wb") as f:
            f.write(content)
        logger.info(f"Uploaded icon: {file_path} (size: {len(content)} bytes)")
    except Exception as e:
        logger.error(f"Failed to save icon: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save icon: {str(e)}") from e

    return {
        "success": True,
        "file_name": safe_name,
        "path": str(file_path),
        "message": f"Icon '{safe_name}' uploaded successfully",
    }


@router.get("/icon/{file_name}", summary="Get a custom icon file")
def get_icon(file_name: str, current_user=Depends(get_current_active_user)) -> FileResponse:
    """
    Retrieve a custom icon file by name.
    Returns the icon file for display in the UI.
    """
    # Sanitize file name - preserve hyphens, dots, and underscores
    safe_name = re.sub(r"[^a-zA-Z0-9_.\-]", "_", file_name)

    icons_dir = storage.user_defined_nodes_icons
    file_path = icons_dir / safe_name

    if not file_path.exists():
        logger.warning(f"Icon not found: {file_path}")
        raise HTTPException(status_code=404, detail=f"Icon '{safe_name}' not found at {file_path}")

    if file_path.suffix.lower() not in ALLOWED_ICON_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Invalid file type")

    content_type_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".svg": "image/svg+xml",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }
    content_type = content_type_map.get(file_path.suffix.lower(), "application/octet-stream")

    return FileResponse(path=file_path, media_type=content_type, filename=safe_name)


@router.delete("/delete-icon/{file_name}", summary="Delete a custom icon")
def delete_icon(file_name: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """
    Delete a custom icon file from the icons directory.
    """
    # Sanitize file name - preserve hyphens, dots, and underscores
    safe_name = re.sub(r"[^a-zA-Z0-9_.\-]", "_", file_name)

    icons_dir = storage.user_defined_nodes_icons
    file_path = icons_dir / safe_name

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Icon '{safe_name}' not found")

    try:
        file_path.unlink()
        logger.info(f"Deleted icon: {file_path}")
    except Exception as e:
        logger.error(f"Failed to delete icon: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete icon: {str(e)}") from e

    return {"success": True, "file_name": safe_name, "message": f"Icon '{safe_name}' deleted successfully"}
