"""
Main API router and endpoint definitions for the Flowfile application.

This module sets up the FastAPI router, defines all the API endpoints for interacting
with flows, nodes, files, and other core components of the application. It handles
the logic for creating, reading, updating, and deleting these resources.
"""

import asyncio
import inspect
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Body, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse, Response

# External dependencies
from polars_expr_transformer.function_overview import get_all_expressions, get_expression_overview
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler

# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import check_if_has_default_setting, nodes_list
from flowfile_core.configs.settings import is_electron_mode
from flowfile_core.database.connection import get_db, get_db_context

# File handling
from flowfile_core.fileExplorer.funcs import (
    FileInfo,
    SecureFileExplorer,
    get_files_from_directory,
    resolve_managed_flow_path,
    validate_path_under_cwd,
)
from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.flowfile.catalog_helpers import (
    FlowNameNamespaceCollision,
    FlowPathNamespaceCollision,
    auto_register_flow,
    find_registration_by_name,
    find_registration_by_path,
    find_registration_by_registration_id,
    register_flow_in_namespace,
    resolve_source_registration_id,
)
from flowfile_core.flowfile.code_generator.code_generator import (
    UnsupportedNodeError,
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_all_database_connections_interface,
    get_database_connection,
    store_database_connection,
    update_database_connection,
)
from flowfile_core.flowfile.extensions import get_instant_func_results
from flowfile_core.flowfile.flow_graph import add_connection, delete_connection
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    create_engine_from_db_settings,
    create_sql_source_from_db_settings,
)
from flowfile_core.run_lock import get_flow_run_lock
from flowfile_core.schemas import input_schema, output_model, schemas
from flowfile_core.schemas.history_schema import HistoryActionType, HistoryState, OperationResponse, UndoRedoResult
from flowfile_core.utils import excel_file_manager
from flowfile_core.utils.fileManager import create_dir
from flowfile_core.utils.utils import camel_case_to_snake_case
from shared.storage_config import storage

router = APIRouter(dependencies=[Depends(get_current_active_user)])

_MANAGED_FLOW_STEM_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def get_node_model(setting_name_ref: str):
    """(Internal) Retrieves a node's Pydantic model from the input_schema module by its name."""
    logger.info("Getting node model for: " + setting_name_ref)
    for ref_name, ref in inspect.getmodule(input_schema).__dict__.items():
        if ref_name.lower() == setting_name_ref:
            return ref
    logger.error(f"Could not find node model for: {setting_name_ref}")
    return None


@router.post("/upload/")
async def upload_file(file: UploadFile = File(...)) -> JSONResponse:
    """Uploads a file to the server's 'uploads' directory.

    Args:
        file: The file to be uploaded.

    Returns:
        A JSON response containing the filename and the path where it was saved.
    """
    safe_name = Path(file.filename).name.replace("..", "")
    if not safe_name:
        raise HTTPException(400, "Invalid filename")
    uploads_dir = Path("uploads")
    uploads_dir.mkdir(exist_ok=True)
    file_location = uploads_dir / safe_name
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    return JSONResponse(content={"filename": safe_name, "filepath": str(file_location)})


@router.get("/files/files_in_local_directory/", response_model=list[FileInfo], tags=["file manager"])
async def get_local_files(directory: str) -> list[FileInfo]:
    """Retrieves a list of files from a specified local directory.

    Args:
        directory: The absolute path of the directory to scan.

    Returns:
        A list of `FileInfo` objects for each item in the directory.

    Raises:
        HTTPException: 404 if the directory does not exist.
        HTTPException: 403 if access is denied (path outside sandbox).
    """
    # Validate path is within sandbox before proceeding
    explorer = SecureFileExplorer(start_path=storage.user_data_directory, sandbox_root=storage.user_data_directory)
    validated_path = explorer.get_absolute_path(directory)
    if validated_path is None:
        raise HTTPException(403, "Access denied or directory does not exist")
    if not validated_path.exists() or not validated_path.is_dir():
        raise HTTPException(404, "Directory does not exist")
    files = get_files_from_directory(str(validated_path), sandbox_root=storage.user_data_directory)
    if files is None:
        raise HTTPException(403, "Access denied or directory does not exist")
    return files


@router.get("/files/default_path/", response_model=str, tags=["file manager"])
async def get_default_path() -> str:
    """Returns the default starting path for the file browser (user data directory)."""
    return str(storage.user_data_directory)


@router.get("/files/catalog_flows_directory/", response_model=str, tags=["file manager"])
async def get_catalog_flows_directory() -> str:
    """Returns the managed flows directory used for catalog-tab saves.

    On local this resolves to ``~/.flowfile/flows``; in Docker mode to
    ``/data/user/flows``.  The frontend uses this to build the target path
    for flows saved via the Catalog tab, so they always land in the managed
    location regardless of where the file browser was last navigated.
    """
    return str(storage.flows_directory)


@router.get("/files/directory_contents/", response_model=list[FileInfo], tags=["file manager"])
async def get_directory_contents(
    directory: str, file_types: list[str] = None, include_hidden: bool = False
) -> list[FileInfo]:
    """Gets the contents of a directory path.

    Args:
        directory: The absolute path to the directory.
        file_types: An optional list of file extensions to filter by.
        include_hidden: If True, includes hidden files and directories.

    Returns:
        A list of `FileInfo` objects representing the directory's contents.
    """

    # In Electron mode, allow browsing the entire filesystem (no sandbox).
    # In other modes, sandbox to the user data directory.
    sandbox_root = None if is_electron_mode() else storage.user_data_directory
    try:
        directory_explorer = SecureFileExplorer(directory, sandbox_root)
        return directory_explorer.list_contents(show_hidden=include_hidden, file_types=file_types)
    except PermissionError:
        raise HTTPException(403, "Access denied: path is outside the allowed directory") from None
    except Exception as e:
        logger.error(e)
        raise HTTPException(404, "Could not access the directory") from e


@router.post("/files/create_directory", response_model=output_model.OutputDir, tags=["file manager"])
def create_directory(new_directory: input_schema.NewDirectory) -> bool:
    """Creates a new directory at the specified path.

    Args:
        new_directory: An `input_schema.NewDirectory` object with the path and name.

    Returns:
        `True` if the directory was created successfully.
    """
    result, error = create_dir(new_directory)
    if result:
        return True
    else:
        raise error


@router.post("/flow/register/", tags=["editor"])
def register_flow(flow_data: schemas.FlowSettings, current_user=Depends(get_current_active_user)) -> int:
    """Registers a new flow session with the application for the current user.

    Args:
        flow_data: The `FlowSettings` for the new flow.

    Returns:
        The ID of the newly registered flow.
    """
    user_id = current_user.id if current_user else None
    return flow_file_handler.register_flow(flow_data, user_id=user_id)


@router.get("/active_flowfile_sessions/", response_model=list[schemas.FlowSettingsResponse])
async def get_active_flow_file_sessions(
    current_user=Depends(get_current_active_user),
) -> list[schemas.FlowSettingsResponse]:
    """Retrieves a list of all currently active flow sessions for the current user."""
    user_id = current_user.id if current_user else None
    return [flow_file_handler.get_flow_info_with_runtime(flf.flow_id) for flf in flow_file_handler.get_user_flows(user_id)]


@router.post("/node/trigger_fetch_data", tags=["editor"])
async def trigger_fetch_node_data(flow_id: int, node_id: int, background_tasks: BackgroundTasks):
    """Fetches and refreshes the data for a specific node."""
    flow = flow_file_handler.get_flow(flow_id)
    lock = get_flow_run_lock(flow_id)
    async with lock:
        if flow.flow_settings.is_running:
            raise HTTPException(422, "Flow is already running")
        try:
            flow.validate_if_node_can_be_fetched(node_id)
        except Exception as e:
            raise HTTPException(422, str(e)) from e
        background_tasks.add_task(flow.trigger_fetch_node, node_id)
    return JSONResponse(
        content={"message": "Data started", "flow_id": flow_id, "node_id": node_id}, status_code=status.HTTP_200_OK
    )


def _run_and_track(flow, user_id: int | None):
    """Wrapper that runs a flow and persists the run record to the database.

    Uses a two-phase pattern:
    1. Create a run record BEFORE execution (makes run visible as "active")
    2. Update the record AFTER execution with results

    This runs in a BackgroundTask. If DB persistence fails, the run still
    completed but won't appear in the run history. Failures are logged at
    ERROR level so they're visible in logs.
    """
    flow_name = getattr(flow.flow_settings, "name", None) or getattr(flow, "__name__", "unknown")

    # Resolve source_registration_id before execution so kernel nodes
    # (e.g. publish_global) can reference the catalog registration.
    resolve_source_registration_id(flow)
    logger.debug(
        f"source_registration_id for flow '{flow_name}': {getattr(flow.flow_settings, 'source_registration_id', None)}"
    )

    # Phase 1: Create run record before execution
    run_id = None
    try:
        # Build snapshot (non-critical if fails)
        snapshot_yaml = None
        try:
            snapshot_data = flow.get_flowfile_data()
            snapshot_yaml = snapshot_data.model_dump_json()
        except Exception as snap_err:
            logger.warning(f"Flow '{flow_name}': snapshot serialization failed: {snap_err}")

        with get_db_context() as db:
            reg_id = getattr(flow.flow_settings, "source_registration_id", None)
            flow_path = flow.flow_settings.path or flow.flow_settings.save_location
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            db_run = service.start_run(
                registration_id=reg_id,
                flow_name=flow_name,
                flow_path=flow_path,
                user_id=user_id if user_id is not None else 0,
                number_of_nodes=len(flow.nodes),
                run_type="in_designer_run",
                flow_snapshot=snapshot_yaml,
            )
            run_id = db_run.id
            logger.info(f"Flow '{flow_name}' run started: run_id={run_id}")
    except Exception as exc:
        logger.error(f"Failed to create run record for flow '{flow_name}': {exc}", exc_info=True)

    # Execute the flow
    run_info = flow.run_graph()
    if run_info is None:
        logger.error(f"Flow '{flow_name}' returned no run_info - run tracking skipped")
        return

    # Phase 2: Update run record with results
    try:
        # Serialise node results (non-critical if fails)
        node_results = None
        try:
            node_results = json.dumps(
                [nr.model_dump(mode="json") for nr in (run_info.node_step_result or [])],
            )
        except Exception as node_err:
            logger.warning(f"Flow '{flow_name}': node results serialization failed: {node_err}")

        if run_id is not None:
            with get_db_context() as db:
                service = CatalogService(SQLAlchemyCatalogRepository(db))
                service.complete_run(
                    run_id=run_id,
                    success=run_info.success,
                    nodes_completed=run_info.nodes_completed,
                    node_results_json=node_results,
                )

            logger.info(
                f"Flow '{flow_name}' run completed: success={run_info.success}, "
                f"nodes={run_info.nodes_completed}/{run_info.number_of_nodes}"
            )
        else:
            # Fallback: create the full record if phase 1 failed
            with get_db_context() as db:
                reg_id = getattr(flow.flow_settings, "source_registration_id", None)
                flow_path = flow.flow_settings.path or flow.flow_settings.save_location
                service = CatalogService(SQLAlchemyCatalogRepository(db))
                service.create_completed_run(
                    registration_id=reg_id,
                    flow_name=flow_name,
                    flow_path=flow_path,
                    user_id=user_id if user_id is not None else 0,
                    started_at=run_info.start_time,
                    ended_at=run_info.end_time,
                    success=run_info.success,
                    nodes_completed=run_info.nodes_completed,
                    number_of_nodes=run_info.number_of_nodes,
                    run_type=run_info.run_type,
                    node_results_json=node_results,
                    flow_snapshot=snapshot_yaml,
                )
    except Exception as exc:
        logger.error(
            f"Failed to update run record for flow '{flow_name}'. "
            f"The flow {'succeeded' if run_info.success else 'failed'} but run history may be incomplete. "
            f"Error: {exc}",
            exc_info=True,
        )


@router.post("/flow/run/", tags=["editor"])
async def run_flow(
    flow_id: int, background_tasks: BackgroundTasks, current_user=Depends(get_current_active_user)
) -> JSONResponse:
    """Executes a flow in a background task.

    Args:
        flow_id: The ID of the flow to execute.
        background_tasks: FastAPI's background task runner.

    Returns:
        A JSON response indicating that the flow has started.
    """
    logger.info("starting to run...")
    flow = flow_file_handler.get_flow(flow_id)
    lock = get_flow_run_lock(flow_id)
    user_id = current_user.id if current_user else None
    async with lock:
        if flow.flow_settings.is_running:
            raise HTTPException(422, "Flow is already running")
        background_tasks.add_task(_run_and_track, flow, user_id)
    return JSONResponse(content={"message": "Data started", "flow_id": flow_id}, status_code=status.HTTP_200_OK)


@router.post("/flow/cancel/", tags=["editor"])
def cancel_flow(flow_id: int):
    """Cancels a currently running flow execution."""
    flow = flow_file_handler.get_flow(flow_id)
    if not flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is not running")
    flow.cancel()


@router.post("/flow/apply_standard_layout/", tags=["editor"])
def apply_standard_layout(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")

    # Capture history BEFORE the layout change
    flow.capture_history_snapshot(HistoryActionType.APPLY_LAYOUT, "Apply standard layout")

    flow.apply_layout()


@router.get("/flow/run_status/", tags=["editor"], response_model=output_model.RunInformation)
def get_run_status(flow_id: int, response: Response):
    """Retrieves the run status information for a specific flow.

    Returns a 202 Accepted status while the flow is running, and 200 OK when finished.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if flow.flow_settings.is_running:
        response.status_code = status.HTTP_202_ACCEPTED
    else:
        response.status_code = status.HTTP_200_OK
    return flow.get_run_info()


@router.post("/transform/manual_input", tags=["transform"])
def add_manual_input(manual_input: input_schema.NodeManualInput):
    flow = flow_file_handler.get_flow(manual_input.flow_id)
    flow.add_datasource(manual_input)


@router.post("/transform/add_input/", tags=["transform"])
def add_flow_input(input_data: input_schema.NodeDatasource):
    flow = flow_file_handler.get_flow(input_data.flow_id)
    try:
        flow.add_datasource(input_data)
    except Exception:
        input_data.file_ref = os.path.join("db_data", input_data.file_ref)
        flow.add_datasource(input_data)


@router.post("/editor/copy_node", tags=["editor"], response_model=OperationResponse)
def copy_node(
    node_id_to_copy_from: int, flow_id_to_copy_from: int, node_promise: input_schema.NodePromise
) -> OperationResponse:
    """Copies an existing node's settings to a new node promise.

    Args:
        node_id_to_copy_from: The ID of the node to copy the settings from.
        flow_id_to_copy_from: The ID of the flow containing the source node.
        node_promise: A `NodePromise` representing the new node to be created.

    Returns:
        OperationResponse with current history state.
    """
    try:
        flow_to_copy_from = flow_file_handler.get_flow(flow_id_to_copy_from)
        flow = (
            flow_to_copy_from
            if flow_id_to_copy_from == node_promise.flow_id
            else flow_file_handler.get_flow(node_promise.flow_id)
        )
        node_to_copy = flow_to_copy_from.get_node(node_id_to_copy_from)
        logger.info(f"Copying data {node_promise.node_type}")

        if flow.flow_settings.is_running:
            raise HTTPException(422, "Flow is running")

        # Capture history BEFORE the change
        flow.capture_history_snapshot(
            HistoryActionType.COPY_NODE, f"Copy {node_promise.node_type} node", node_id=node_promise.node_id
        )

        if flow.get_node(node_promise.node_id) is not None:
            flow.delete_node(node_promise.node_id)

        if node_promise.node_type == "explore_data":
            flow.add_initial_node_analysis(node_promise)
            return OperationResponse(success=True, history=flow.get_history_state())

        flow.copy_node(node_promise, node_to_copy.setting_input, node_to_copy.node_type)

        return OperationResponse(success=True, history=flow.get_history_state())

    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e


@router.post("/editor/add_node/", tags=["editor"], response_model=OperationResponse)
def add_node(
    flow_id: int, node_id: int, node_type: str, pos_x: int | float = 0, pos_y: int | float = 0
) -> OperationResponse | None:
    """Adds a new, unconfigured node (a "promise") to the flow graph.

    Args:
        flow_id: The ID of the flow to add the node to.
        node_id: The client-generated ID for the new node.
        node_type: The type of the node to add (e.g., 'filter', 'join').
        pos_x: The X coordinate for the node's position in the UI.
        pos_y: The Y coordinate for the node's position in the UI.

    Returns:
        OperationResponse with current history state.
    """
    if isinstance(pos_x, float):
        pos_x = int(pos_x)
    if isinstance(pos_y, float):
        pos_y = int(pos_y)
    flow = flow_file_handler.get_flow(flow_id)
    logger.info(f"Adding a promise for {node_type}")
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")

    node = flow.get_node(node_id)
    if node is not None:
        flow.delete_node(node_id)
    node_promise = input_schema.NodePromise(
        flow_id=flow_id, node_id=node_id, cache_results=False, pos_x=pos_x, pos_y=pos_y, node_type=node_type
    )
    if node_type == "explore_data":
        flow.add_initial_node_analysis(node_promise)
    else:
        # Capture state BEFORE adding node (for batched history)
        pre_snapshot = flow.get_flowfile_data() if flow.flow_settings.track_history else None

        logger.info("Adding node")
        # Add node without individual history tracking
        flow.add_node_promise(node_promise, track_history=False)

        if check_if_has_default_setting(node_type):
            logger.info(f"Found standard settings for {node_type}, trying to upload them")
            setting_name_ref = "node" + node_type.replace("_", "")
            node_model = get_node_model(setting_name_ref)

            # Temporarily disable history tracking for initial settings
            original_track_history = flow.flow_settings.track_history
            flow.flow_settings.track_history = False
            try:
                add_func = getattr(flow, "add_" + node_type)
                initial_settings = node_model(
                    flow_id=flow_id, node_id=node_id, cache_results=False, pos_x=pos_x, pos_y=pos_y, node_type=node_type
                )
                add_func(initial_settings)
            finally:
                flow.flow_settings.track_history = original_track_history

        # Capture batched history entry for the whole add_node operation
        if pre_snapshot is not None and flow.flow_settings.track_history:
            flow._history_manager.capture_if_changed(
                flow,
                pre_snapshot,
                HistoryActionType.ADD_NODE,
                f"Add {node_type} node",
                node_id,
            )
            logger.info(f"History: Captured batched 'Add {node_type} node' entry")

    logger.info(f"History state after add_node: {flow.get_history_state()}")
    return OperationResponse(success=True, history=flow.get_history_state())


@router.post("/editor/delete_node/", tags=["editor"], response_model=OperationResponse)
def delete_node(flow_id: int | None, node_id: int) -> OperationResponse:
    """Deletes a node from the flow graph.

    Returns:
        OperationResponse with current history state.
    """
    logger.info("Deleting node")
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")

    # Capture history BEFORE the change
    node = flow.get_node(node_id)
    node_type = node.node_type if node else "unknown"
    flow.capture_history_snapshot(HistoryActionType.DELETE_NODE, f"Delete {node_type} node", node_id=node_id)

    flow.delete_node(node_id)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.post("/editor/delete_connection/", tags=["editor"], response_model=OperationResponse)
def delete_node_connection(flow_id: int, node_connection: input_schema.NodeConnection = None) -> OperationResponse:
    """Deletes a connection (edge) between two nodes.

    Returns:
        OperationResponse with current history state.
    """
    flow_id = int(flow_id)
    logger.info(
        f"Deleting connection node {node_connection.output_connection.node_id} "
        f"to node {node_connection.input_connection.node_id}"
    )
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")

    # Capture history BEFORE the change
    from_id = node_connection.output_connection.node_id
    to_id = node_connection.input_connection.node_id
    flow.capture_history_snapshot(HistoryActionType.DELETE_CONNECTION, f"Delete connection {from_id} -> {to_id}")

    delete_connection(flow, node_connection)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.post("/db_connection_lib", tags=["db_connections"])
def create_db_connection(
    input_connection: input_schema.FullDatabaseConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Creates and securely stores a new database connection."""
    logger.info(f"Creating database connection {input_connection.connection_name}")
    try:
        store_database_connection(db, input_connection, current_user.id)
    except ValueError:
        raise HTTPException(422, "Connection name already exists") from None
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Database connection created successfully"}


@router.put("/db_connection_lib", tags=["db_connections"])
def update_db_connection(
    input_connection: input_schema.FullDatabaseConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Updates an existing database connection."""
    logger.info(f"Updating database connection {input_connection.connection_name}")
    try:
        update_database_connection(db, input_connection, current_user.id)
    except ValueError:
        raise HTTPException(404, "Database connection not found") from None
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Database connection updated successfully"}


@router.delete("/db_connection_lib", tags=["db_connections"])
def delete_db_connection(
    connection_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)
):
    """Deletes a stored database connection."""
    logger.info(f"Deleting database connection {connection_name}")
    db_connection = get_database_connection(db, connection_name, current_user.id)
    if db_connection is None:
        raise HTTPException(404, "Database connection not found")
    delete_database_connection(db, connection_name, current_user.id)
    return {"message": "Database connection deleted successfully"}


@router.get(
    "/db_connection_lib", tags=["db_connections"], response_model=list[input_schema.FullDatabaseConnectionInterface]
)
def get_db_connections(
    db: Session = Depends(get_db), current_user=Depends(get_current_active_user)
) -> list[input_schema.FullDatabaseConnectionInterface]:
    """Retrieves all stored database connections for the current user (without passwords)."""
    return get_all_database_connections_interface(db, current_user.id)


@router.post("/editor/connect_node/", tags=["editor"], response_model=OperationResponse)
def connect_node(flow_id: int, node_connection: input_schema.NodeConnection) -> OperationResponse:
    """Creates a connection (edge) between two nodes in the flow graph.

    Returns:
        OperationResponse with current history state.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        logger.info("could not find the flow")
        raise HTTPException(404, "could not find the flow")
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")

    # Capture history BEFORE the change
    from_id = node_connection.output_connection.node_id
    to_id = node_connection.input_connection.node_id
    flow.capture_history_snapshot(HistoryActionType.ADD_CONNECTION, f"Connect {from_id} -> {to_id}")

    add_connection(flow, node_connection)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.get("/editor/expression_doc", tags=["editor"], response_model=list[output_model.ExpressionsOverview])
def get_expression_doc() -> list[output_model.ExpressionsOverview]:
    """Retrieves documentation for available Polars expressions."""
    return get_expression_overview()


@router.get("/editor/expressions", tags=["editor"], response_model=list[str])
def get_expressions() -> list[str]:
    """Retrieves a list of all available Flowfile expression names."""
    return get_all_expressions()


@router.get("/editor/flow", tags=["editor"], response_model=schemas.FlowSettingsResponse)
def get_flow(flow_id: int):
    """Retrieves the settings for a specific flow (including runtime dirty state)."""
    flow_id = int(flow_id)
    result = get_flow_settings(flow_id)
    return result


@router.get("/editor/laziness_check", tags=["editor"])
def check_flow_laziness(flow_id: int):
    """Check whether a flow supports fully lazy execution for virtual tables."""
    flow = flow_file_handler.get_flow(int(flow_id))
    if flow is None:
        raise HTTPException(404, "Flow not found")
    is_lazy, reasons = flow.check_flow_laziness()
    return {"is_optimizable": is_lazy, "blockers": reasons}


@router.get("/editor/code_to_polars", tags=[], response_model=str)
def get_generated_code(flow_id: int) -> str:
    """Generates and returns a Python script with Polars code representing the flow."""
    flow_id = int(flow_id)
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    try:
        return export_flow_to_polars(flow)
    except UnsupportedNodeError as e:
        raise HTTPException(422, str(e)) from e


@router.get("/editor/code_to_flowframe", tags=[], response_model=str)
def get_generated_flowframe_code(flow_id: int) -> str:
    """Generates and returns a Python script with FlowFrame code representing the flow."""
    flow_id = int(flow_id)
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    try:
        return export_flow_to_flowframe(flow)
    except UnsupportedNodeError as e:
        raise HTTPException(422, str(e)) from e


@router.post("/editor/create_flow/", tags=["editor"])
def create_flow(flow_path: str = None, name: str = None, current_user=Depends(get_current_active_user)):
    """Creates a new, empty flow file at the specified path and registers a session for it."""
    if flow_path is not None and name is None:
        name = Path(flow_path).stem
    elif flow_path is not None and name is not None:
        if name not in flow_path and (flow_path.endswith(".yaml") or flow_path.endswith(".yml")):
            raise HTTPException(422, "The name must be part of the flow path when a full path is provided")
        elif name in flow_path and not (flow_path.endswith(".yaml") or flow_path.endswith(".yml")):
            flow_path = str(Path(flow_path) / (name + ".yaml"))
        elif name not in flow_path and (name.endswith(".yaml") or name.endswith(".yml")):
            flow_path = str(Path(flow_path) / name)
        elif name not in flow_path and not (name.endswith(".yaml") or name.endswith(".yml")):
            flow_path = str(Path(flow_path) / (name + ".yaml"))
    if flow_path is not None:
        # Validate path is within allowed sandbox
        flow_path = validate_path_under_cwd(flow_path)
        flow_path_ref = Path(flow_path)
        if not flow_path_ref.parent.exists():
            raise HTTPException(422, "The directory does not exist")
    user_id = current_user.id if current_user else None
    flow_id = flow_file_handler.add_flow(name=name, flow_path=flow_path, user_id=user_id)
    flow = flow_file_handler.get_flow(flow_id)
    if flow and flow.flow_settings:
        auto_register_flow(flow.flow_settings.path, name or flow.flow_settings.name, user_id)
        resolve_source_registration_id(flow)
    return flow_id


@router.post("/editor/close_flow/", tags=["editor"])
def close_flow(flow_id: int, current_user=Depends(get_current_active_user)) -> None:
    """Closes an active flow session for the current user."""
    user_id = current_user.id if current_user else None
    flow_file_handler.delete_flow(flow_id, user_id=user_id)


# ==================== History/Undo-Redo Endpoints ====================


@router.post("/editor/undo/", tags=["editor"], response_model=UndoRedoResult)
def undo_action(flow_id: int) -> UndoRedoResult:
    """Undo the last action on the flow graph.

    Args:
        flow_id: The ID of the flow to undo.

    Returns:
        UndoRedoResult indicating success or failure.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")
    return flow.undo()


@router.post("/editor/redo/", tags=["editor"], response_model=UndoRedoResult)
def redo_action(flow_id: int) -> UndoRedoResult:
    """Redo the last undone action on the flow graph.

    Args:
        flow_id: The ID of the flow to redo.

    Returns:
        UndoRedoResult indicating success or failure.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")
    return flow.redo()


@router.get("/editor/history_status/", tags=["editor"], response_model=HistoryState)
def get_history_status(flow_id: int) -> HistoryState:
    """Get the current state of the history system for a flow.

    Args:
        flow_id: The ID of the flow to get history status for.

    Returns:
        HistoryState with information about available undo/redo operations.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    return flow.get_history_state()


@router.post("/editor/history_clear/", tags=["editor"])
def clear_history(flow_id: int):
    """Clear all history for a flow.

    Args:
        flow_id: The ID of the flow to clear history for.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    flow._history_manager.clear()
    return {"message": "History cleared successfully"}


# ==================== End History Endpoints ====================


@router.post("/update_settings/", tags=["transform"], response_model=OperationResponse)
def add_generic_settings(
    input_data: dict[str, Any], node_type: str, current_user=Depends(get_current_active_user)
) -> OperationResponse:
    """A generic endpoint to update the settings of any node.

    This endpoint dynamically determines the correct Pydantic model and update
    function based on the `node_type` parameter.

    Returns:
        OperationResponse with current history state.
    """
    input_data["user_id"] = current_user.id
    node_type = camel_case_to_snake_case(node_type)
    flow_id = int(input_data.get("flow_id"))
    node_id = int(input_data.get("node_id"))
    logger.info(f"Updating the data for flow: {flow_id}, node {node_id}")
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, "Flow is running")
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    add_func = getattr(flow, "add_" + node_type)
    parsed_input = None
    setting_name_ref = "node" + node_type.replace("_", "")

    if add_func is None:
        raise HTTPException(404, "could not find the function")
    try:
        ref = get_node_model(setting_name_ref)
        if ref:
            parsed_input = ref(**input_data)
    except Exception as e:
        raise HTTPException(421, str(e)) from e
    if parsed_input is None:
        raise HTTPException(404, "could not find the interface")
    try:
        # History capture is handled by the decorator on each add_* method
        add_func(parsed_input)
    except Exception as e:
        logger.error(e)
        raise HTTPException(419, str(f"error: {e}")) from e

    return OperationResponse(success=True, history=flow.get_history_state())


@router.get("/files/available_flow_files", tags=["editor"], response_model=list[FileInfo])
def get_list_of_saved_flows(path: str):
    """Scans a directory for saved flow files (`.flowfile`)."""
    try:
        # Validate path is within sandbox before proceeding
        explorer = SecureFileExplorer(start_path=storage.user_data_directory, sandbox_root=storage.user_data_directory)
        validated_path = explorer.get_absolute_path(path)
        if validated_path is None:
            return []
        return get_files_from_directory(
            str(validated_path), types=["flowfile"], sandbox_root=storage.user_data_directory
        )
    except Exception:
        return []


@router.get("/node_list", response_model=list[schemas.NodeTemplate])
def get_node_list() -> list[schemas.NodeTemplate]:
    """Retrieves the list of all available node types and their templates."""
    return nodes_list


@router.get("/node", response_model=output_model.NodeData, tags=["editor"])
def get_node(flow_id: int, node_id: int, get_data: bool = False):
    """Retrieves the complete state and data preview for a single node."""
    logging.info(f"Getting node {node_id} from flow {flow_id}")
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(422, "Not found")
    v = node.get_node_data(flow_id=flow.flow_id, include_example=get_data)
    return v


@router.get("/node/input_names", tags=["editor"])
def get_node_input_names(flow_id: int, node_id: int) -> list[output_model.NodeInputNameInfo]:
    """Returns the named inputs available for a kernel node.

    Each entry contains the input name (derived from the source node's
    ``node_reference`` or fallback ``df_{id}``), the source node ID, and
    its type. The frontend uses this for autocomplete and display.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(404, "Node not found")

    result: list[output_model.NodeInputNameInfo] = []
    for source_node in node.all_inputs:
        ref = getattr(source_node.setting_input, "node_reference", None)
        name = ref if ref else f"df_{source_node.node_id}"
        result.append(
            output_model.NodeInputNameInfo(
                name=name,
                source_node_id=source_node.node_id,
                source_node_type=source_node.node_type,
            )
        )
    return result


@router.post("/node/description/", tags=["editor"])
def update_description_node(flow_id: int, node_id: int, description: str = Body(...)):
    """Updates the description text for a specific node."""
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except Exception:
        raise HTTPException(404, "Could not find the node") from None
    node.setting_input.description = description
    return True


@router.get("/node/description", response_model=output_model.NodeDescriptionResponse, tags=["editor"])
def get_description_node(flow_id: int, node_id: int):
    """Retrieves the description text for a specific node.

    Returns the user-provided description if set, otherwise falls back
    to an auto-generated description based on the node's configuration.
    The response includes an `is_auto_generated` flag so the frontend
    knows whether to refresh the description after settings changes.
    """
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except Exception:
        raise HTTPException(404, "Could not find the node") from None
    if node is None:
        raise HTTPException(404, "Could not find the node")
    user_description = node.setting_input.description if hasattr(node.setting_input, "description") else ""
    if user_description:
        return output_model.NodeDescriptionResponse(description=user_description, is_auto_generated=False)
    if hasattr(node.setting_input, "get_default_description"):
        auto_desc = node.setting_input.get_default_description()
        return output_model.NodeDescriptionResponse(description=auto_desc, is_auto_generated=True)
    return output_model.NodeDescriptionResponse(description="", is_auto_generated=True)


@router.post("/node/reference/", tags=["editor"])
def update_reference_node(flow_id: int, node_id: int, reference: str = Body(...)):
    """Updates the reference identifier for a specific node.

    The reference must be:
    - Lowercase only
    - No spaces allowed
    - Unique across all nodes in the flow
    """
    try:
        flow = flow_file_handler.get_flow(flow_id)
        node = flow.get_node(node_id)
    except Exception:
        raise HTTPException(404, "Could not find the node") from None
    if node is None:
        raise HTTPException(404, "Could not find the node")

    # Handle empty reference (allow clearing)
    if reference == "" or reference is None:
        node.setting_input.node_reference = None
        return True

    # Validate: lowercase only, no spaces
    if " " in reference:
        raise HTTPException(422, "Reference cannot contain spaces")
    if reference != reference.lower():
        raise HTTPException(422, "Reference must be lowercase")

    # Validate: unique across all nodes in the flow
    for other_node in flow.nodes:
        if other_node.node_id != node_id:
            other_ref = getattr(other_node.setting_input, "node_reference", None)
            if other_ref and other_ref == reference:
                raise HTTPException(422, f'Reference "{reference}" is already used by another node')

    node.setting_input.node_reference = reference
    return True


@router.get("/node/reference", tags=["editor"])
def get_reference_node(flow_id: int, node_id: int):
    """Retrieves the reference identifier for a specific node."""
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except Exception:
        raise HTTPException(404, "Could not find the node") from None
    if node is None:
        raise HTTPException(404, "Could not find the node")
    return node.setting_input.node_reference or ""


@router.get("/node/validate_reference", tags=["editor"])
def validate_node_reference(flow_id: int, node_id: int, reference: str):
    """Validates if a reference is valid and unique for a node.

    Returns:
        Dict with 'valid' (bool) and 'error' (str or None) fields.
    """
    try:
        flow = flow_file_handler.get_flow(flow_id)
    except Exception:
        raise HTTPException(404, "Could not find the flow") from None

    # Handle empty reference (always valid - means use default)
    if reference == "" or reference is None:
        return {"valid": True, "error": None}

    # Validate: lowercase only
    if reference != reference.lower():
        return {"valid": False, "error": "Reference must be lowercase"}

    # Validate: no spaces
    if " " in reference:
        return {"valid": False, "error": "Reference cannot contain spaces"}

    # Validate: unique across all nodes in the flow
    for other_node in flow.nodes:
        if other_node.node_id != node_id:
            other_ref = getattr(other_node.setting_input, "node_reference", None)
            if other_ref and other_ref == reference:
                return {"valid": False, "error": f'Reference "{reference}" is already used by another node'}

    return {"valid": True, "error": None}


@router.get("/node/data", response_model=output_model.TableExample, tags=["editor"])
def get_table_example(flow_id: int, node_id: int):
    """Retrieves a data preview (schema and sample rows) for a node's output."""
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    return node.get_table_example(True)


@router.get("/node/downstream_node_ids", response_model=list[int], tags=["editor"])
async def get_downstream_node_ids(flow_id: int, node_id: int) -> list[int]:
    """Gets a list of all node IDs that are downstream dependencies of a given node."""
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    return list(node.get_all_dependent_node_ids())


@router.get("/import_flow/", tags=["editor"], response_model=int)
def import_saved_flow(flow_path: str, current_user=Depends(get_current_active_user)) -> int:
    """Imports a flow from a saved `.yaml` and registers it as a new session for the current user."""
    validated_path = validate_path_under_cwd(flow_path)
    if not os.path.exists(validated_path):
        raise HTTPException(404, "File not found")
    user_id = current_user.id if current_user else None
    flow_id = flow_file_handler.import_flow(Path(validated_path), user_id=user_id)
    flow = flow_file_handler.get_flow(flow_id)
    if flow and flow.flow_settings:
        auto_register_flow(validated_path, flow.flow_settings.name, user_id)
        resolve_source_registration_id(flow)
    return flow_id


def _save_flow_impl(
    flow_id: int,
    flow_path: str | None,
    namespace_id: int | None,
    current_user,
):
    """Shared implementation for GET and POST ``/save_flow``.

    If ``flow_path`` is omitted, the flow is saved silently to its existing path.

    When saving to a new path ("Save As"), the flow is treated as a new flow:
    a new flowfile_id is generated, the old handler entry is replaced, and a
    fresh catalog registration is created.  The new flow_id is returned so the
    frontend can switch to it.

    When ``namespace_id`` is provided, the flow is registered in that catalog
    namespace (instead of the default namespace).
    """
    if flow_path is not None:
        flow_path = validate_path_under_cwd(flow_path)
    flow = flow_file_handler.get_flow(flow_id)
    current_path = flow.flow_settings.path or flow.flow_settings.save_location

    # If no explicit path provided, use the current path (silent save)
    if flow_path is None:
        flow_path = current_path
    if not flow_path:
        raise HTTPException(422, "No save path specified and flow has no existing path")

    # Re-validate: current_path can be set via POST /flow_settings without validation.
    flow_path = validate_path_under_cwd(flow_path)
    normalized_current = validate_path_under_cwd(current_path) if current_path else None

    is_new_path = bool(normalized_current) and flow_path != normalized_current

    if is_new_path:
        user_id = current_user.id if current_user else None

        def _register(fp: str, n: str, uid: int | None) -> None:
            register_flow_in_namespace(fp, n, uid, namespace_id)

        try:
            return flow_file_handler.save_as_flow(
                flow_id=flow_id,
                new_path=flow_path,
                user_id=user_id,
                on_catalog_register=_register,
                on_resolve_registration=resolve_source_registration_id,
            )
        except FlowPathNamespaceCollision as err:
            raise HTTPException(status_code=409, detail=str(err)) from err

    resolve_source_registration_id(flow)
    flow.save_flow(flow_path=flow_path)  # save_flow itself calls mark_as_saved()

    # If namespace_id provided, ensure the flow is registered in that namespace
    if namespace_id is not None:
        user_id = current_user.id if current_user else None
        try:
            register_flow_in_namespace(flow_path, flow.flow_settings.name, user_id, namespace_id)
        except FlowPathNamespaceCollision as err:
            raise HTTPException(status_code=409, detail=str(err)) from err
    return flow_id


@router.get("/save_flow", tags=["editor"])
def save_flow(
    response: Response,
    flow_id: int,
    flow_path: str = None,
    namespace_id: int = None,
    current_user=Depends(get_current_active_user),
):
    """Deprecated GET variant of ``/save_flow``.  Prefer POST.

    Kept for backward compatibility with older frontends/clients. Emits a
    ``Deprecation: true`` response header.
    """
    logger.warning("GET /save_flow is deprecated; use POST /save_flow instead")
    response.headers["Deprecation"] = "true"
    return _save_flow_impl(flow_id, flow_path, namespace_id, current_user)


@router.post("/save_flow", tags=["editor"])
def save_flow_post(
    flow_id: int,
    flow_path: str = None,
    namespace_id: int = None,
    current_user=Depends(get_current_active_user),
):
    """Saves the current state of a flow to a ``.yaml``.

    See :func:`_save_flow_impl` for semantics.
    """
    return _save_flow_impl(flow_id, flow_path, namespace_id, current_user)


@router.post("/save_flow_to_catalog", tags=["editor"])
def save_flow_to_catalog(
    flow_id: int,
    flow_name: str,
    namespace_id: int,
    current_user=Depends(get_current_active_user),
):
    """Save a flow into the managed catalog flows directory with a collision-free filename.

    The file is always written to ``{flows_dir}/{flow_id}_{sanitized_name}.yaml`` so
    two flows with the same user-chosen name in different namespaces cannot overwrite
    each other. Returns the (possibly new) flow id so the frontend can switch to it.
    """
    stem = flow_name.strip()
    if not stem:
        raise HTTPException(422, "flow_name must not be empty")
    # Reject path separators and parent-traversal before any sanitization so
    # callers can't launder ``../evil`` through ``Path(...).name``.
    if "/" in stem or "\\" in stem or ".." in stem:
        raise HTTPException(status_code=403, detail="invalid managed flow filename")
    stem = stem.rsplit(".yaml", 1)[0].rsplit(".yml", 1)[0].rsplit(".json", 1)[0]
    if not _MANAGED_FLOW_STEM_RE.fullmatch(stem):
        raise HTTPException(status_code=403, detail="invalid managed flow filename")

    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")

    filename = f"{int(flow_id)}_{stem}.yaml"
    flow_path = resolve_managed_flow_path(filename)

    current_path = flow.flow_settings.path or flow.flow_settings.save_location
    normalized_current = validate_path_under_cwd(current_path) if current_path else None
    is_new_path = bool(normalized_current) and flow_path != normalized_current

    # Overwrite guard: if the resolved target file is already registered to a
    # different flow, or exists on disk without any registration, refuse.
    source_registration_id = getattr(flow.flow_settings, "source_registration_id", None)

    # Pre-save name-collision check — reject BEFORE writing any YAML so a
    # failed save doesn't leave orphaned files on disk.  Two flows with the
    # same display name in one namespace is confusing in the catalog picker;
    # the correct path is to overwrite the existing entry instead.
    existing_by_name = find_registration_by_name(stem, namespace_id)
    if existing_by_name is not None and existing_by_name.id != source_registration_id:
        raise HTTPException(
            status_code=409,
            detail=(
                f"A flow named '{stem}' already exists in this namespace. "
                "Select it in the catalog picker to overwrite, or choose a different name."
            ),
        )

    existing_reg = find_registration_by_path(flow_path)
    if existing_reg is not None and existing_reg.id != source_registration_id:
        raise HTTPException(
            status_code=409,
            detail=f"Target file {flow_path} is already registered to another flow",
        )
    if existing_reg is None and os.path.exists(flow_path):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Target file {flow_path} exists but is not catalog-registered; "
                "refusing to overwrite"
            ),
        )

    user_id = current_user.id if current_user else None

    # Always register under the user-typed ``stem`` rather than the filename
    # (``{flow_id}_{stem}``) so the catalog picker shows exactly what the user
    # typed — and so the name-collision check below compares apples to apples.
    def _register(fp: str, _n: str, uid: int | None) -> None:
        register_flow_in_namespace(fp, stem, uid, namespace_id)

    if is_new_path:
        try:
            new_flow_id = flow_file_handler.save_as_flow(
                flow_id=flow_id,
                new_path=flow_path,
                user_id=user_id,
                on_catalog_register=_register,
                on_resolve_registration=resolve_source_registration_id,
            )
        except FlowPathNamespaceCollision as err:
            raise HTTPException(status_code=409, detail=str(err)) from err
        except FlowNameNamespaceCollision as err:
            raise HTTPException(status_code=409, detail=str(err)) from err

        # If we renamed within the managed flows directory, unlink the old file.
        if normalized_current and normalized_current != flow_path:
            managed_root = str(Path(storage.flows_directory).resolve()) + os.sep
            if normalized_current.startswith(managed_root):
                try:
                    os.unlink(normalized_current)
                except OSError:
                    logger.info(
                        f"Could not unlink old managed flow file {normalized_current}",
                        exc_info=True,
                    )
        return new_flow_id

    resolve_source_registration_id(flow)
    flow.save_flow(flow_path=flow_path)
    try:
        register_flow_in_namespace(flow_path, stem, user_id, namespace_id)
    except FlowPathNamespaceCollision as err:
        raise HTTPException(status_code=409, detail=str(err)) from err
    except FlowNameNamespaceCollision as err:
        raise HTTPException(status_code=409, detail=str(err)) from err
    return flow_id


@router.post("/overwrite_flow_in_catalog", tags=["editor"])
def overwrite_flow_in_catalog(
    flow_id: int,
    target_registration_id: int,
    current_user=Depends(get_current_active_user),
):
    """Overwrite an existing catalog flow's YAML with the contents of another flow.

    Unlike ``/save_flow_to_catalog``, this intentionally writes over an existing
    registration.  The target registration's name and namespace are preserved;
    only the file contents on disk change.  Primary use case: reverting a flow
    to an older version by loading that version and overwriting the canonical
    catalog entry.

    Returns the (possibly new) flow id so the frontend can switch to the target.
    """
    user_id = current_user.id if current_user else None
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")

    target = find_registration_by_registration_id(target_registration_id)
    if target is None:
        raise HTTPException(404, "Target catalog registration not found")

    # Overwrite is destructive — gate strictly on ownership even though
    # ``update_flow`` itself does not.
    if user_id is None or user_id != target.owner_id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to overwrite this catalog flow",
        )

    target_path = validate_path_under_cwd(target.flow_path)
    managed_root = str(Path(storage.flows_directory).resolve()) + os.sep

    current_path = flow.flow_settings.path or flow.flow_settings.save_location
    normalized_current = validate_path_under_cwd(current_path) if current_path else None

    # Same-path case: current flow already lives at the target path, so just
    # re-save in place and keep the registration pointer fresh.
    if normalized_current == target_path:
        resolve_source_registration_id(flow)
        flow.save_flow(flow_path=target_path)
        _touch_flow_registration(target_registration_id)
        return flow_id

    new_flow_id = flow_file_handler.save_as_flow(
        flow_id=flow_id,
        new_path=target_path,
        user_id=user_id,
        on_catalog_register=None,  # registration already exists; preserve it
        on_resolve_registration=resolve_source_registration_id,
    )

    # If the source flow lived inside the managed dir on a different file,
    # unlink the abandoned file so we don't leak orphaned YAML.  We only clean
    # up files under the managed root; user-owned paths elsewhere are left
    # alone since we don't want to silently delete files the user manages.
    if (
        normalized_current
        and normalized_current != target_path
        and normalized_current.startswith(managed_root)
    ):
        try:
            os.unlink(normalized_current)
        except OSError:
            logger.info(
                f"Could not unlink old managed flow file {normalized_current}",
                exc_info=True,
            )

    _touch_flow_registration(target_registration_id)
    return new_flow_id


def _touch_flow_registration(registration_id: int) -> None:
    """Bump ``updated_at`` on a FlowRegistration row after overwrite."""
    from datetime import datetime

    from flowfile_core.database.models import FlowRegistration

    with get_db_context() as db:
        reg = db.get(FlowRegistration, registration_id)
        if reg is None:
            return
        reg.updated_at = datetime.utcnow()
        db.commit()


@router.get("/flow_data", tags=["manager"])
def get_flow_frontend_data(flow_id: int | None = 1):
    """Retrieves the data needed to render the flow graph in the frontend."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    return flow.get_frontend_data()


@router.get("/flow_settings", tags=["manager"], response_model=schemas.FlowSettingsResponse)
def get_flow_settings(flow_id: int | None = 1) -> schemas.FlowSettingsResponse:
    """Retrieves the main settings for a flow (including dirty-state info)."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    return flow_file_handler.get_flow_info_with_runtime(flow_id)


@router.post("/flow_settings", tags=["manager"])
def update_flow_settings(flow_settings: schemas.FlowSettings):
    """Updates the main settings for a flow."""
    flow = flow_file_handler.get_flow(flow_settings.flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    flow.flow_settings = flow_settings


@router.get("/flow_data/v2", tags=["manager"])
def get_vue_flow_data(flow_id: int) -> schemas.VueFlowInput:
    """Retrieves the flow data formatted for the Vue-based frontend."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "could not find the flow")
    data = flow.get_vue_flow_input()
    return data


@router.get("/flow/artifacts", tags=["editor"])
def get_flow_artifacts(flow_id: int):
    """Returns artifact visualization data for the canvas.

    Includes per-node artifact summaries (for badges/tooltips) and
    artifact edges (for dashed-line connections between publisher and
    consumer nodes).
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    ctx = flow.artifact_context
    return {
        "nodes": ctx.get_node_summaries(),
        "edges": ctx.get_artifact_edges(),
    }


@router.get("/flow/node_upstream_ids", tags=["editor"])
def get_node_upstream_ids(flow_id: int, node_id: int):
    """Return the transitive upstream node IDs for a given node.

    Used by the frontend to determine which artifacts are actually
    reachable (via the DAG) from a specific python_script node.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    return {"upstream_node_ids": flow._get_upstream_node_ids(node_id)}


@router.get("/flow/node_available_artifacts", tags=["editor"])
def get_node_available_artifacts(flow_id: int, node_id: int, kernel_id: str | None = None):
    """Return available artifact metadata for a node.

    Used by the frontend to populate artifact selector UI components.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, "Could not find the flow")
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(404, "Could not find the node")
    resolved_kernel_id = kernel_id or getattr(node.setting_input, "kernel_id", None)
    if not resolved_kernel_id:
        return {"artifacts": []}

    upstream_ids = flow._get_upstream_node_ids(node_id)
    available = flow.artifact_context.compute_available(
        node_id=node_id,
        kernel_id=resolved_kernel_id,
        upstream_node_ids=upstream_ids,
    )
    return {"artifacts": [ref.to_dict() for ref in available.values()]}


@router.get("/analysis_data/graphic_walker_input", tags=["analysis"], response_model=input_schema.NodeExploreData)
def get_graphic_walker_input(flow_id: int, node_id: int):
    """Gets the data and configuration for the Graphic Walker data exploration tool."""
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node.results.analysis_data_generator is None:
        logger.error("The data is not refreshed and available for analysis")
        raise HTTPException(422, "The data is not refreshed and available for analysis")
    return AnalyticsProcessor.process_graphic_walker_input(node)


@router.get("/custom_functions/instant_result", tags=[])
async def get_instant_function_result(flow_id: int, node_id: int, func_string: str):
    """Executes a simple, instant function on a node's data and returns the result."""
    try:
        node = flow_file_handler.get_node(flow_id, node_id)
        result = await asyncio.to_thread(get_instant_func_results, node, func_string)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/get_xlsx_sheet_names", tags=["excel_reader"], response_model=list[str])
async def get_excel_sheet_names(path: str) -> list[str] | None:
    """Retrieves the sheet names from an Excel file."""
    validated_path = validate_path_under_cwd(path)
    sheet_names = excel_file_manager.get_sheet_names(validated_path)
    if sheet_names:
        return sheet_names
    else:
        raise HTTPException(404, "File not found")


@router.post("/validate_db_settings")
async def validate_db_settings(
    database_settings: input_schema.DatabaseSettings, current_user=Depends(get_current_active_user)
):
    """Validates that a connection can be made to a database with the given settings."""
    # Validate the query settings
    try:
        sql_source = create_sql_source_from_db_settings(database_settings, user_id=current_user.id)
        sql_source.validate()
        return {"message": "Query settings are valid"}
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.post("/db_schemas", tags=["db_connections"], response_model=list[str])
async def get_db_schemas(
    database_settings: input_schema.DatabaseSettings, current_user=Depends(get_current_active_user)
) -> list[str]:
    """Returns available schema names for the given database connection."""
    try:
        engine = create_engine_from_db_settings(database_settings, user_id=current_user.id)
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(engine)
        schemas = sorted(inspector.get_schema_names())
        engine.dispose()
        return schemas
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.post("/db_tables", tags=["db_connections"], response_model=list[str])
async def get_db_tables(
    database_settings: input_schema.DatabaseSettings, current_user=Depends(get_current_active_user)
) -> list[str]:
    """Returns available table names for the given database connection and optional schema.

    When schema_name is provided, returns plain table names.
    When schema_name is not provided, returns schema-qualified names (schema.table) across all
    accessible schemas (skipping any that the user cannot read).
    """
    try:
        engine = create_engine_from_db_settings(database_settings, user_id=current_user.id)
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(engine)
        schema = database_settings.schema_name if database_settings.schema_name else None

        if schema:
            tables = sorted(inspector.get_table_names(schema=schema))
        else:
            tables = []
            for s in inspector.get_schema_names():
                if s == "information_schema":
                    continue
                try:
                    schema_tables = inspector.get_table_names(schema=s)
                except Exception:
                    # Skip schemas we don't have access to (e.g. performance_schema)
                    continue
                for t in schema_tables:
                    tables.append(f"{s}.{t}")
            tables.sort()
        engine.dispose()
        return tables
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


# =============================================================================
# Template Endpoints
# =============================================================================


@router.get("/templates/", tags=["templates"])
def list_templates():
    """Returns metadata for all available flow templates."""
    from flowfile_core.templates import get_all_template_metas

    return get_all_template_metas()


@router.get("/templates/ensure_available/", tags=["templates"])
def ensure_templates_available():
    """Downloads template flow YAMLs from GitHub if not already cached locally.

    Called by the frontend on first visit to the templates page to ensure
    templates are available even when running from a PyPI install (no repo checkout).
    """
    from flowfile_core.templates import get_flow_yaml_filenames
    from flowfile_core.templates.data_downloader import ensure_flow_yamls

    try:
        ensure_flow_yamls(get_flow_yaml_filenames())
        return {"status": "ok"}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/templates/{template_id}/create", tags=["templates"])
def create_from_template(template_id: str, current_user=Depends(get_current_active_user)) -> int:
    """Instantiates a template as a new flow session.

    Downloads required CSV data files from GitHub if not already cached locally,
    then creates a flow from the template definition.
    """
    import yaml

    from flowfile_core.templates import get_template_flowfile_data, get_template_required_files
    from flowfile_core.templates.data_downloader import ensure_template_data

    try:
        required_files = get_template_required_files(template_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    try:
        resolved_files = ensure_template_data(required_files)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    # Use the directory where the data files actually live
    data_dir = next(iter(resolved_files.values())).parent
    flowfile_data = get_template_flowfile_data(template_id, data_dir)

    # Write to a unique temp YAML and import via existing flow import path
    import uuid

    from shared.storage_config import storage

    flows_dir = storage.flows_directory
    user_id = current_user.id if current_user else None

    flow_stem = flowfile_data.flowfile_name.replace(" ", "_").lower()
    temp_path = flows_dir / f"{flow_stem}_{uuid.uuid4().hex[:8]}.yaml"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)
        flow_id = flow_file_handler.import_flow(temp_path, user_id=user_id)
    finally:
        temp_path.unlink(missing_ok=True)

    flow = flow_file_handler.get_flow(flow_id)
    if flow and flow.flow_settings:
        auto_register_flow(str(flows_dir / f"{flow_stem}.yaml"), flow.flow_settings.name, user_id)
    return flow_id
