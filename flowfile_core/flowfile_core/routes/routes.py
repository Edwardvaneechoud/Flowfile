"""
Main API router and endpoint definitions for the Flowfile application.

This module sets up the FastAPI router, defines all the API endpoints for interacting
with flows, nodes, files, and other core components of the application. It handles
the logic for creating, reading, updating, and deleting these resources.
"""

import asyncio
import inspect
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Body, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse, Response

# External dependencies
from polars_expr_transformer.function_overview import get_all_expressions, get_expression_overview
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler

# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import check_if_has_default_setting, nodes_list
from flowfile_core.database.connection import get_db

# File handling
from flowfile_core.fileExplorer.funcs import (
    FileInfo,
    SecureFileExplorer,
    get_files_from_directory,
    validate_file_path,
    validate_path_under_cwd,
)
from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_all_database_connections_interface,
    get_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.extensions import get_instant_func_results
from flowfile_core.flowfile.flow_graph import add_connection, delete_connection
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import create_sql_source_from_db_settings
from flowfile_core.run_lock import get_flow_run_lock
from flowfile_core.schemas import input_schema, output_model, schemas
from flowfile_core.schemas.history_schema import HistoryActionType, HistoryState, UndoRedoResult, OperationResponse
from flowfile_core.utils import excel_file_manager
from flowfile_core.utils.fileManager import create_dir
from flowfile_core.utils.utils import camel_case_to_snake_case
from shared.storage_config import storage

router = APIRouter(dependencies=[Depends(get_current_active_user)])

# Initialize services
file_explorer = SecureFileExplorer(
    start_path=storage.user_data_directory,
    sandbox_root=storage.user_data_directory
)


def get_node_model(setting_name_ref: str):
    """(Internal) Retrieves a node's Pydantic model from the input_schema module by its name."""
    logger.info("Getting node model for: " + setting_name_ref)
    for ref_name, ref in inspect.getmodule(input_schema).__dict__.items():
        if ref_name.lower() == setting_name_ref:
            return ref
    logger.error(f"Could not find node model for: {setting_name_ref}")


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
        raise HTTPException(400, 'Invalid filename')
    uploads_dir = Path("uploads")
    uploads_dir.mkdir(exist_ok=True)
    file_location = uploads_dir / safe_name
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    return JSONResponse(content={"filename": safe_name, "filepath": str(file_location)})


@router.get('/files/files_in_local_directory/', response_model=list[FileInfo], tags=['file manager'])
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
    explorer = SecureFileExplorer(
        start_path=storage.user_data_directory,
        sandbox_root=storage.user_data_directory
    )
    validated_path = explorer.get_absolute_path(directory)
    if validated_path is None:
        raise HTTPException(403, 'Access denied or directory does not exist')
    if not validated_path.exists() or not validated_path.is_dir():
        raise HTTPException(404, 'Directory does not exist')
    files = get_files_from_directory(str(validated_path), sandbox_root=storage.user_data_directory)
    if files is None:
        raise HTTPException(403, 'Access denied or directory does not exist')
    return files


@router.get('/files/tree/', response_model=List[FileInfo], tags=['file manager'])
async def get_current_files() -> List[FileInfo]:
    """Gets the contents of the file explorer's current directory."""
    f = file_explorer.list_contents()
    return f


@router.post('/files/navigate_up/', response_model=str, tags=['file manager'])
async def navigate_up() -> str:
    """Navigates the file explorer one directory level up."""
    file_explorer.navigate_up()
    return str(file_explorer.current_path)


@router.post('/files/navigate_into/', response_model=str, tags=['file manager'])
async def navigate_into_directory(directory_name: str) -> str:
    """Navigates the file explorer into a specified subdirectory."""
    file_explorer.navigate_into(directory_name)
    return str(file_explorer.current_path)


@router.post('/files/navigate_to/', tags=['file manager'])
async def navigate_to_directory(directory_name: str) -> str:
    """Navigates the file explorer to an absolute directory path."""
    file_explorer.navigate_to(directory_name)
    return str(file_explorer.current_path)


@router.get('/files/current_path/', response_model=str, tags=['file manager'])
async def get_current_path() -> str:
    """Returns the current absolute path of the file explorer."""
    return str(file_explorer.current_path)


@router.get('/files/directory_contents/', response_model=List[FileInfo], tags=['file manager'])
async def get_directory_contents(directory: str, file_types: List[str] = None,
                                 include_hidden: bool = False) -> List[FileInfo]:
    """Gets the contents of an arbitrary directory path.

    Args:
        directory: The absolute path to the directory.
        file_types: An optional list of file extensions to filter by.
        include_hidden: If True, includes hidden files and directories.

    Returns:
        A list of `FileInfo` objects representing the directory's contents.
    """
    directory_explorer = SecureFileExplorer(directory, storage.user_data_directory)
    try:
        return directory_explorer.list_contents(show_hidden=include_hidden, file_types=file_types)
    except Exception as e:
        logger.error(e)
        HTTPException(404, 'Could not access the directory')


@router.get('/files/current_directory_contents/', response_model=List[FileInfo], tags=['file manager'])
async def get_current_directory_contents(file_types: List[str] = None, include_hidden: bool = False) -> List[FileInfo]:
    """Gets the contents of the file explorer's current directory."""
    return file_explorer.list_contents(file_types=file_types, show_hidden=include_hidden)


@router.post('/files/create_directory', response_model=output_model.OutputDir, tags=['file manager'])
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


@router.get("/active_flowfile_sessions/", response_model=list[schemas.FlowSettings])
async def get_active_flow_file_sessions(current_user=Depends(get_current_active_user)) -> list[schemas.FlowSettings]:
    """Retrieves a list of all currently active flow sessions for the current user."""
    user_id = current_user.id if current_user else None
    return [flf.flow_settings for flf in flow_file_handler.get_user_flows(user_id)]


@router.post("/node/trigger_fetch_data", tags=['editor'])
async def trigger_fetch_node_data(flow_id: int, node_id: int, background_tasks: BackgroundTasks):
    """Fetches and refreshes the data for a specific node."""
    flow = flow_file_handler.get_flow(flow_id)
    lock = get_flow_run_lock(flow_id)
    async with lock:
        if flow.flow_settings.is_running:
            raise HTTPException(422, 'Flow is already running')
        try:
            flow.validate_if_node_can_be_fetched(node_id)
        except Exception as e:
            raise HTTPException(422, str(e))
        background_tasks.add_task(flow.trigger_fetch_node, node_id)
    return JSONResponse(content={"message": "Data started",
                                 "flow_id": flow_id,
                                 "node_id": node_id}, status_code=status.HTTP_200_OK)


@router.post('/flow/run/', tags=['editor'])
async def run_flow(flow_id: int, background_tasks: BackgroundTasks) -> JSONResponse:
    """Executes a flow in a background task.

    Args:
        flow_id: The ID of the flow to execute.
        background_tasks: FastAPI's background task runner.

    Returns:
        A JSON response indicating that the flow has started.
    """
    logger.info('starting to run...')
    flow = flow_file_handler.get_flow(flow_id)
    lock = get_flow_run_lock(flow_id)
    async with lock:
        if flow.flow_settings.is_running:
            raise HTTPException(422, 'Flow is already running')
        background_tasks.add_task(flow.run_graph)
    return JSONResponse(content={"message": "Data started", "flow_id": flow_id}, status_code=status.HTTP_200_OK)


@router.post('/flow/cancel/', tags=['editor'])
def cancel_flow(flow_id: int):
    """Cancels a currently running flow execution."""
    flow = flow_file_handler.get_flow(flow_id)
    if not flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is not running')
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


@router.get('/flow/run_status/', tags=['editor'],
            response_model=output_model.RunInformation)
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


@router.post('/transform/manual_input', tags=['transform'])
def add_manual_input(manual_input: input_schema.NodeManualInput):
    flow = flow_file_handler.get_flow(manual_input.flow_id)
    flow.add_datasource(manual_input)


@router.post('/transform/add_input/', tags=['transform'])
def add_flow_input(input_data: input_schema.NodeDatasource):
    flow = flow_file_handler.get_flow(input_data.flow_id)
    try:
        flow.add_datasource(input_data)
    except:
        input_data.file_ref = os.path.join('db_data', input_data.file_ref)
        flow.add_datasource(input_data)


@router.post('/editor/copy_node', tags=['editor'], response_model=OperationResponse)
def copy_node(node_id_to_copy_from: int, flow_id_to_copy_from: int, node_promise: input_schema.NodePromise) -> OperationResponse:
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
        flow = (flow_to_copy_from
                if flow_id_to_copy_from == node_promise.flow_id
                else flow_file_handler.get_flow(node_promise.flow_id)
                )
        node_to_copy = flow_to_copy_from.get_node(node_id_to_copy_from)
        logger.info(f"Copying data {node_promise.node_type}")

        if flow.flow_settings.is_running:
            raise HTTPException(422, "Flow is running")

        # Capture history BEFORE the change
        flow.capture_history_snapshot(
            HistoryActionType.COPY_NODE,
            f"Copy {node_promise.node_type} node",
            node_id=node_promise.node_id
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
        raise HTTPException(422, str(e))


@router.post('/editor/add_node/', tags=['editor'], response_model=OperationResponse)
def add_node(flow_id: int, node_id: int, node_type: str, pos_x: int = 0, pos_y: int = 0) -> OperationResponse:
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
    flow = flow_file_handler.get_flow(flow_id)
    logger.info(f'Adding a promise for {node_type}')
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')

    node = flow.get_node(node_id)
    if node is not None:
        flow.delete_node(node_id)
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, cache_results=False, pos_x=pos_x,
                                            pos_y=pos_y,
                                            node_type=node_type)
    if node_type == 'explore_data':
        flow.add_initial_node_analysis(node_promise)
    else:
        # Capture state BEFORE adding node (for batched history)
        pre_snapshot = flow.get_flowfile_data() if flow.flow_settings.track_history else None

        logger.info("Adding node")
        # Add node without individual history tracking
        flow.add_node_promise(node_promise, track_history=False)

        if check_if_has_default_setting(node_type):
            logger.info(f'Found standard settings for {node_type}, trying to upload them')
            setting_name_ref = 'node' + node_type.replace('_', '')
            node_model = get_node_model(setting_name_ref)

            # Temporarily disable history tracking for initial settings
            original_track_history = flow.flow_settings.track_history
            flow.flow_settings.track_history = False
            try:
                add_func = getattr(flow, 'add_' + node_type)
                initial_settings = node_model(flow_id=flow_id, node_id=node_id, cache_results=False,
                                              pos_x=pos_x, pos_y=pos_y, node_type=node_type)
                add_func(initial_settings)
            finally:
                flow.flow_settings.track_history = original_track_history

        # Capture batched history entry for the whole add_node operation
        if pre_snapshot is not None and flow.flow_settings.track_history:
            from flowfile_core.schemas.history_schema import HistoryActionType
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


@router.post('/editor/delete_node/', tags=['editor'], response_model=OperationResponse)
def delete_node(flow_id: Optional[int], node_id: int) -> OperationResponse:
    """Deletes a node from the flow graph.

    Returns:
        OperationResponse with current history state.
    """
    logger.info('Deleting node')
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')

    # Capture history BEFORE the change
    node = flow.get_node(node_id)
    node_type = node.node_type if node else "unknown"
    flow.capture_history_snapshot(HistoryActionType.DELETE_NODE, f"Delete {node_type} node", node_id=node_id)

    flow.delete_node(node_id)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.post('/editor/delete_connection/', tags=['editor'], response_model=OperationResponse)
def delete_node_connection(flow_id: int, node_connection: input_schema.NodeConnection = None) -> OperationResponse:
    """Deletes a connection (edge) between two nodes.

    Returns:
        OperationResponse with current history state.
    """
    flow_id = int(flow_id)
    logger.info(
        f'Deleting connection node {node_connection.output_connection.node_id} to node {node_connection.input_connection.node_id}')
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')

    # Capture history BEFORE the change
    from_id = node_connection.output_connection.node_id
    to_id = node_connection.input_connection.node_id
    flow.capture_history_snapshot(
        HistoryActionType.DELETE_CONNECTION,
        f"Delete connection {from_id} -> {to_id}"
    )

    delete_connection(flow, node_connection)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.post("/db_connection_lib", tags=['db_connections'])
def create_db_connection(input_connection: input_schema.FullDatabaseConnection,
                         current_user=Depends(get_current_active_user),
                         db: Session = Depends(get_db)
                         ):
    """Creates and securely stores a new database connection."""
    logger.info(f'Creating database connection {input_connection.connection_name}')
    try:
        store_database_connection(db, input_connection, current_user.id)
    except ValueError:
        raise HTTPException(422, 'Connection name already exists')
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e))
    return {"message": "Database connection created successfully"}


@router.delete('/db_connection_lib', tags=['db_connections'])
def delete_db_connection(connection_name: str,
                         current_user=Depends(get_current_active_user),
                         db: Session = Depends(get_db)
                         ):
    """Deletes a stored database connection."""
    logger.info(f'Deleting database connection {connection_name}')
    db_connection = get_database_connection(db, connection_name, current_user.id)
    if db_connection is None:
        raise HTTPException(404, 'Database connection not found')
    delete_database_connection(db, connection_name, current_user.id)
    return {"message": "Database connection deleted successfully"}


@router.get('/db_connection_lib', tags=['db_connections'],
            response_model=List[input_schema.FullDatabaseConnectionInterface])
def get_db_connections(
        db: Session = Depends(get_db),
        current_user=Depends(get_current_active_user)) -> List[input_schema.FullDatabaseConnectionInterface]:
    """Retrieves all stored database connections for the current user (without passwords)."""
    return get_all_database_connections_interface(db, current_user.id)


@router.post('/editor/connect_node/', tags=['editor'], response_model=OperationResponse)
def connect_node(flow_id: int, node_connection: input_schema.NodeConnection) -> OperationResponse:
    """Creates a connection (edge) between two nodes in the flow graph.

    Returns:
        OperationResponse with current history state.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        logger.info('could not find the flow')
        raise HTTPException(404, 'could not find the flow')
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')

    # Capture history BEFORE the change
    from_id = node_connection.output_connection.node_id
    to_id = node_connection.input_connection.node_id
    flow.capture_history_snapshot(
        HistoryActionType.ADD_CONNECTION,
        f"Connect {from_id} -> {to_id}"
    )

    add_connection(flow, node_connection)

    return OperationResponse(success=True, history=flow.get_history_state())


@router.get('/editor/expression_doc', tags=['editor'], response_model=List[output_model.ExpressionsOverview])
def get_expression_doc() -> List[output_model.ExpressionsOverview]:
    """Retrieves documentation for available Polars expressions."""
    return get_expression_overview()


@router.get('/editor/expressions', tags=['editor'], response_model=List[str])
def get_expressions() -> List[str]:
    """Retrieves a list of all available Flowfile expression names."""
    return get_all_expressions()


@router.get('/editor/flow', tags=['editor'], response_model=schemas.FlowSettings)
def get_flow(flow_id: int):
    """Retrieves the settings for a specific flow."""
    flow_id = int(flow_id)
    result = get_flow_settings(flow_id)
    return result


@router.get("/editor/code_to_polars", tags=[], response_model=str)
def get_generated_code(flow_id: int) -> str:
    """Generates and returns a Python script with Polars code representing the flow."""
    flow_id = int(flow_id)
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    return export_flow_to_polars(flow)


@router.post("/editor/create_flow/", tags=["editor"])
def create_flow(flow_path: str = None, name: str = None, current_user=Depends(get_current_active_user)):
    """Creates a new, empty flow file at the specified path and registers a session for it."""
    if flow_path is not None and name is None:
        name = Path(flow_path).stem
    elif flow_path is not None and name is not None:
        if name not in flow_path and (flow_path.endswith(".yaml") or flow_path.endswith(".yml")):
            raise HTTPException(422, 'The name must be part of the flow path when a full path is provided')
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
    return flow_file_handler.add_flow(name=name, flow_path=flow_path, user_id=user_id)


@router.post("/editor/close_flow/", tags=["editor"])
def close_flow(flow_id: int, current_user=Depends(get_current_active_user)) -> None:
    """Closes an active flow session for the current user."""
    user_id = current_user.id if current_user else None
    flow_file_handler.delete_flow(flow_id, user_id=user_id)


# ==================== History/Undo-Redo Endpoints ====================

@router.post('/editor/undo/', tags=['editor'], response_model=UndoRedoResult)
def undo_action(flow_id: int) -> UndoRedoResult:
    """Undo the last action on the flow graph.

    Args:
        flow_id: The ID of the flow to undo.

    Returns:
        UndoRedoResult indicating success or failure.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'Could not find the flow')
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    return flow.undo()


@router.post('/editor/redo/', tags=['editor'], response_model=UndoRedoResult)
def redo_action(flow_id: int) -> UndoRedoResult:
    """Redo the last undone action on the flow graph.

    Args:
        flow_id: The ID of the flow to redo.

    Returns:
        UndoRedoResult indicating success or failure.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'Could not find the flow')
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    return flow.redo()


@router.get('/editor/history_status/', tags=['editor'], response_model=HistoryState)
def get_history_status(flow_id: int) -> HistoryState:
    """Get the current state of the history system for a flow.

    Args:
        flow_id: The ID of the flow to get history status for.

    Returns:
        HistoryState with information about available undo/redo operations.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'Could not find the flow')
    return flow.get_history_state()


@router.post('/editor/history_clear/', tags=['editor'])
def clear_history(flow_id: int):
    """Clear all history for a flow.

    Args:
        flow_id: The ID of the flow to clear history for.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'Could not find the flow')
    flow._history_manager.clear()
    return {"message": "History cleared successfully"}


# ==================== End History Endpoints ====================


@router.post('/update_settings/', tags=['transform'], response_model=OperationResponse)
def add_generic_settings(input_data: Dict[str, Any], node_type: str, current_user=Depends(get_current_active_user)) -> OperationResponse:
    """A generic endpoint to update the settings of any node.

    This endpoint dynamically determines the correct Pydantic model and update
    function based on the `node_type` parameter.

    Returns:
        OperationResponse with current history state.
    """
    input_data['user_id'] = current_user.id
    node_type = camel_case_to_snake_case(node_type)
    flow_id = int(input_data.get('flow_id'))
    node_id = int(input_data.get('node_id'))
    logger.info(f'Updating the data for flow: {flow_id}, node {node_id}')
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    add_func = getattr(flow, 'add_' + node_type)
    parsed_input = None
    setting_name_ref = 'node' + node_type.replace('_', '')

    if add_func is None:
        raise HTTPException(404, 'could not find the function')
    try:
        ref = get_node_model(setting_name_ref)
        if ref:
            parsed_input = ref(**input_data)
    except Exception as e:
        raise HTTPException(421, str(e))
    if parsed_input is None:
        raise HTTPException(404, 'could not find the interface')
    try:
        # History capture is handled by the decorator on each add_* method
        add_func(parsed_input)
    except Exception as e:
        logger.error(e)
        raise HTTPException(419, str(f'error: {e}'))

    return OperationResponse(success=True, history=flow.get_history_state())


@router.get('/files/available_flow_files', tags=['editor'], response_model=List[FileInfo])
def get_list_of_saved_flows(path: str):
    """Scans a directory for saved flow files (`.flowfile`)."""
    try:
        # Validate path is within sandbox before proceeding
        explorer = SecureFileExplorer(
            start_path=storage.user_data_directory,
            sandbox_root=storage.user_data_directory
        )
        validated_path = explorer.get_absolute_path(path)
        if validated_path is None:
            return []
        return get_files_from_directory(str(validated_path), types=['flowfile'], sandbox_root=storage.user_data_directory)
    except:
        return []


@router.get('/node_list', response_model=List[schemas.NodeTemplate])
def get_node_list() -> List[schemas.NodeTemplate]:
    """Retrieves the list of all available node types and their templates."""
    return nodes_list


@router.get('/node', response_model=output_model.NodeData, tags=['editor'])
def get_node(flow_id: int, node_id: int, get_data: bool = False):
    """Retrieves the complete state and data preview for a single node."""
    logging.info(f'Getting node {node_id} from flow {flow_id}')
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(422, 'Not found')
    v = node.get_node_data(flow_id=flow.flow_id, include_example=get_data)
    return v


@router.post('/node/description/', tags=['editor'])
def update_description_node(flow_id: int, node_id: int, description: str = Body(...)):
    """Updates the description text for a specific node."""
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except:
        raise HTTPException(404, 'Could not find the node')
    node.setting_input.description = description
    return True


@router.get('/node/description', tags=['editor'])
def get_description_node(flow_id: int, node_id: int):
    """Retrieves the description text for a specific node."""
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except:
        raise HTTPException(404, 'Could not find the node')
    if node is None:
        raise HTTPException(404, 'Could not find the node')
    return node.setting_input.description


@router.get('/node/data', response_model=output_model.TableExample, tags=['editor'])
def get_table_example(flow_id: int, node_id: int):
    """Retrieves a data preview (schema and sample rows) for a node's output."""
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    return node.get_table_example(True)


@router.get('/node/downstream_node_ids', response_model=List[int], tags=['editor'])
async def get_downstream_node_ids(flow_id: int, node_id: int) -> List[int]:
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
    return flow_file_handler.import_flow(Path(validated_path), user_id=user_id)


@router.get('/save_flow', tags=['editor'])
def save_flow(flow_id: int, flow_path: str = None):
    """Saves the current state of a flow to a `.yaml`."""
    if flow_path is not None:
        flow_path = validate_path_under_cwd(flow_path)
    flow = flow_file_handler.get_flow(flow_id)
    flow.save_flow(flow_path=flow_path)


@router.get('/flow_data', tags=['manager'])
def get_flow_frontend_data(flow_id: Optional[int] = 1):
    """Retrieves the data needed to render the flow graph in the frontend."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    return flow.get_frontend_data()


@router.get('/flow_settings', tags=['manager'], response_model=schemas.FlowSettings)
def get_flow_settings(flow_id: Optional[int] = 1) -> schemas.FlowSettings:
    """Retrieves the main settings for a flow."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    return flow.flow_settings


@router.post('/flow_settings', tags=['manager'])
def update_flow_settings(flow_settings: schemas.FlowSettings):
    """Updates the main settings for a flow."""
    flow = flow_file_handler.get_flow(flow_settings.flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    flow.flow_settings = flow_settings


@router.get('/flow_data/v2', tags=['manager'])
def get_vue_flow_data(flow_id: int) -> schemas.VueFlowInput:
    """Retrieves the flow data formatted for the Vue-based frontend."""
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    data = flow.get_vue_flow_input()
    return data


@router.get('/analysis_data/graphic_walker_input', tags=['analysis'], response_model=input_schema.NodeExploreData)
def get_graphic_walker_input(flow_id: int, node_id: int):
    """Gets the data and configuration for the Graphic Walker data exploration tool."""
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node.results.analysis_data_generator is None:
        logger.error('The data is not refreshed and available for analysis')
        raise HTTPException(422, 'The data is not refreshed and available for analysis')
    return AnalyticsProcessor.process_graphic_walker_input(node)


@router.get('/custom_functions/instant_result', tags=[])
async def get_instant_function_result(flow_id: int, node_id: int, func_string: str):
    """Executes a simple, instant function on a node's data and returns the result."""
    try:
        node = flow_file_handler.get_node(flow_id, node_id)
        result = await asyncio.to_thread(get_instant_func_results, node, func_string)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/api/get_xlsx_sheet_names', tags=['excel_reader'], response_model=list[str])
async def get_excel_sheet_names(path: str) -> list[str] | None:
    """Retrieves the sheet names from an Excel file."""
    validated_path = validate_path_under_cwd(path)
    sheet_names = excel_file_manager.get_sheet_names(validated_path)
    if sheet_names:
        return sheet_names
    else:
        raise HTTPException(404, 'File not found')


@router.post("/validate_db_settings")
async def validate_db_settings(
        database_settings: input_schema.DatabaseSettings,
        current_user=Depends(get_current_active_user)
):
    """Validates that a connection can be made to a database with the given settings."""
    # Validate the query settings
    try:
        sql_source = create_sql_source_from_db_settings(database_settings, user_id=current_user.id)
        sql_source.validate()
        return {"message": "Query settings are valid"}
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
