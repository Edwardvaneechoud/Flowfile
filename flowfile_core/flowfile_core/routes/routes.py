import inspect
import logging
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from fastapi import APIRouter, File, UploadFile, BackgroundTasks, HTTPException, status, Body, Depends
from fastapi.responses import JSONResponse, Response
# External dependencies
from polars_expr_transformer.function_overview import get_all_expressions, get_expression_overview

# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.node_store import nodes
from flowfile_core.configs.settings import IS_RUNNING_IN_DOCKER
# File handling
from flowfile_core.fileExplorer.funcs import (
    FileExplorer,
    FileInfo,
    get_files_from_directory
)
from flowfile_core.flowfile.FlowfileFlow import add_connection, delete_connection
from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.flowfile.extensions import get_instant_func_results
# Flow handling
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.models import AirbyteConfigTemplate
# Airbyte
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.settings import (
    airbyte_config_handler,
    AirbyteHandler
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import create_sql_source_from_db_settings
from flowfile_core.run_lock import get_flow_run_lock
# Schema and models
from flowfile_core.schemas import input_schema, schemas, output_model
from flowfile_core.utils import excel_file_manager
from flowfile_core.utils.fileManager import create_dir, remove_paths
from flowfile_core.utils.utils import camel_case_to_snake_case
from flowfile_core import flow_file_handler
from flowfile_core.flowfile.database_connection_manager.db_connections import (store_database_connection,
                                                                               get_database_connection,
                                                                               delete_database_connection,
                                                                               get_all_database_connections_interface)
from flowfile_core.database.connection import get_db



# Router setup
router = APIRouter(dependencies=[Depends(get_current_active_user)])

# Initialize services
file_explorer = FileExplorer('/app/shared' if IS_RUNNING_IN_DOCKER else None)


def get_node_model(setting_name_ref: str):
    for ref_name, ref in inspect.getmodule(input_schema).__dict__.items():
        if ref_name.lower() == setting_name_ref:
            return ref


@router.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    file_location = f"uploads/{file.filename}"
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    return JSONResponse(content={"filename": file.filename, "filepath": file_location})


@router.get('/files/files_in_local_directory/', response_model=List[FileInfo], tags=['file manager'])
async def get_local_files(directory: str) -> List[FileInfo]:
    files = get_files_from_directory(directory)
    if files is None:
        raise HTTPException(404, 'Directory does not exist')
    return files


@router.get('/files/tree/', response_model=List[FileInfo], tags=['file manager'])
async def get_current_files() -> List[FileInfo]:
    f = file_explorer.list_contents()
    return f


@router.post('/files/navigate_up/', response_model=str, tags=['file manager'])
async def navigate_up() -> str:
    file_explorer.navigate_up()
    return str(file_explorer.current_path)


@router.post('/files/navigate_into/', response_model=str, tags=['file manager'])
async def navigate_into_directory(directory_name: str) -> str:
    file_explorer.navigate_into(directory_name)
    return str(file_explorer.current_path)


@router.post('/files/navigate_to/', tags=['file manager'])
async def navigate_to_directory(directory_name: str) -> str:
    file_explorer.navigate_to(directory_name)
    return str(file_explorer.current_path)


@router.get('/files/current_path/', response_model=str, tags=['file manager'])
async def get_current_path() -> str:
    return str(file_explorer.current_path)


@router.get('/files/directory_contents/', response_model=List[FileInfo], tags=['file manager'])
async def get_directory_contents(directory: str, file_types: List[str] = None,
                                 include_hidden: bool = False) -> List[FileInfo]:
    directory_explorer = FileExplorer(directory)
    try:
        return directory_explorer.list_contents(show_hidden=include_hidden, file_types=file_types)
    except Exception as e:
        logger.error(e)
        HTTPException(404, 'Could not access the directory')


@router.get('/files/current_directory_contents/', response_model=List[FileInfo], tags=['file manager'])
async def get_current_directory_contents(file_types: List[str] = None, include_hidden: bool = False) -> List[FileInfo]:
    return file_explorer.list_contents(file_types=file_types, show_hidden=include_hidden)


@router.post('/files/create_directory', response_model=output_model.OutputDir, tags=['file manager'])
def create_directory(new_directory: input_schema.NewDirectory) -> bool:
    result, error = create_dir(new_directory)
    if result:
        return True
    else:
        raise error


@router.post('/flow/register/', tags=['editor'])
def register_flow(flow_data: schemas.FlowSettings):
    return flow_file_handler.register_flow(flow_data)


@router.get('/active_flowfile_sessions/', response_model=List[schemas.FlowSettings])
async def get_active_flow_file_sessions() -> List[schemas.FlowSettings]:
    return [flf.flow_settings for flf in flow_file_handler.flowfile_flows]


@router.post('/flow/run/', tags=['editor'])
async def run_flow(flow_id: int, background_tasks: BackgroundTasks):
    logger.info('starting to run...')
    flow = flow_file_handler.get_flow(flow_id)
    lock = get_flow_run_lock(flow_id)
    async with lock:
        if flow.flow_settings.is_running:
            raise HTTPException(422, 'Flow is already running')
        background_tasks.add_task(flow.run_graph)
    JSONResponse(content={"message": "Data started", "flow_id": flow_id}, status_code=status.HTTP_202_ACCEPTED)


@router.post('/flow/cancel/', tags=['editor'])
def cancel_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if not flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is not running')
    flow.cancel()


@router.get('/flow/run_status/', tags=['editor'],
            response_model=output_model.RunInformation)
def get_run_status(flow_id: int, response: Response):
    flow = flow_file_handler.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if flow.flow_settings.is_running:
        response.status_code = status.HTTP_202_ACCEPTED
        return flow.get_run_info()
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


@router.post('/editor/copy_node', tags=['editor'])
def copy_node(node_id_to_copy_from: int, flow_id_to_copy_from: int, node_promise: input_schema.NodePromise):
    """
    Add a node to the flow.
    Parameters
    ----------
    node_id_to_copy_from: int, the id of the node to copy
    flow_id_to_copy_from: int, the id of the flow to copy from
    node_promise: NodePromise, the node promise that contains all the data
    Returns
    -------
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

        if flow.get_node(node_promise.node_id) is not None:
            flow.delete_node(node_promise.node_id)

        if node_promise.node_type == "explore_data":
            flow.add_initial_node_analysis(node_promise)
            return

        flow.copy_node(node_promise, node_to_copy.setting_input, node_to_copy.node_type)

    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e))


@router.post('/editor/add_node/', tags=['editor'])
def add_node(flow_id: int, node_id: int, node_type: str, pos_x: int = 0, pos_y: int = 0):
    """
    Add a node to the flow.
    Parameters
    ----------
    flow_id: int, the flow id
    node_id: int, the node id
    node_type: str, the node type
    pos_x: int, the x position of the node
    pos_y: int, the y position of the node

    Returns
    -------

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
        return
    else:
        logger.info("Adding node")
        flow.add_node_promise(node_promise)

    if nodes.check_if_has_default_setting(node_type):
        logger.info(f'Found standard settings for {node_type}, trying to upload them')
        setting_name_ref = 'node' + node_type.replace('_', '')
        node_model = get_node_model(setting_name_ref)
        add_func = getattr(flow, 'add_' + node_type)
        initial_settings = node_model(flow_id=flow_id, node_id=node_id, cache_results=False,
                                      pos_x=pos_x, pos_y=pos_y, node_type=node_type)
        add_func(initial_settings)


@router.post('/editor/delete_node/', tags=['editor'])
def delete_node(flow_id: Optional[int], node_id: int):
    logger.info('Deleting node')
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    flow.delete_node(node_id)


@router.post('/editor/delete_connection/', tags=['editor'])
def delete_node_connection(flow_id: int, node_connection: input_schema.NodeConnection = None):
    flow_id = int(flow_id)
    logger.info(
        f'Deleting connection node {node_connection.output_connection.node_id} to node {node_connection.input_connection.node_id}')
    flow = flow_file_handler.get_flow(flow_id)
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    delete_connection(flow, node_connection)


@router.post("/db_connection_lib", tags=['db_connections'])
def create_db_connection(input_connection: input_schema.FullDatabaseConnection,
                         current_user=Depends(get_current_active_user),
                         db: Session = Depends(get_db)
                         ):
    """
    Create a database connection.
    """
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
    """
    Delete a database connection.
    """
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
    return get_all_database_connections_interface(db, current_user.id)


@router.post('/editor/connect_node/', tags=['editor'])
def connect_node(flow_id: int, node_connection: input_schema.NodeConnection):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        logger.info('could not find the flow')
        raise HTTPException(404, 'could not find the flow')
    if flow.flow_settings.is_running:
        raise HTTPException(422, 'Flow is running')
    add_connection(flow, node_connection)


@router.get('/editor/expression_doc', tags=['editor'], response_model=List[output_model.ExpressionsOverview])
def get_expression_doc() -> List[output_model.ExpressionsOverview]:
    return get_expression_overview()


@router.get('/editor/expressions', tags=['editor'], response_model=List[str])
def get_expressions() -> List[str]:
    return get_all_expressions()


@router.get('/editor/flow', tags=['editor'], response_model=schemas.FlowSettings)
def get_flow(flow_id: int):
    flow_id = int(flow_id)
    result = get_flow_settings(flow_id)
    return result


@router.post('/editor/create_flow/', tags=['editor'])
def create_flow(flow_path: str):
    flow_path = Path(flow_path)
    logger.info('Creating flow')
    return flow_file_handler.add_flow(name=flow_path.stem, flow_path=str(flow_path))


@router.post('/editor/close_flow/', tags=['editor'])
def close_flow(flow_id: int) -> None:
    flow_file_handler.delete_flow(flow_id)


@router.get('/airbyte/available_connectors', tags=['airbyte'])
def get_available_connectors():
    return airbyte_config_handler.available_connectors


@router.get('/airbyte/available_configs', tags=['airbyte'])
def get_available_configs() -> List[str]:
    """
    Get the available configurations for the airbyte connectors
    Returns: List of available configurations
    """
    return airbyte_config_handler.available_configs


@router.get('/airbyte/config_template', tags=['airbyte'], response_model=AirbyteConfigTemplate)
def get_config_spec(connector_name: str):
    a = airbyte_config_handler.get_config('source-' + connector_name)
    return a


@router.post('/airbyte/set_airbyte_configs_for_streams', tags=['airbyte'])
def set_airbyte_configs_for_streams(airbyte_config: input_schema.AirbyteConfig):
    logger.info('Setting airbyte config, update_style = ')
    logger.info(f'Setting config for {airbyte_config.source_name}')
    logger.debug(f'Config: {airbyte_config.mapped_config_spec}')
    airbyte_handler = AirbyteHandler(airbyte_config=airbyte_config)
    try:
        _ = airbyte_handler.get_available_streams()
    except Exception as e:
        raise HTTPException(404, str(e))


@router.post('/update_settings/', tags=['transform'])
def add_generic_settings(input_data: Dict[str, Any], node_type: str, current_user=Depends(get_current_active_user)):
    input_data['user_id'] = current_user.id
    node_type = camel_case_to_snake_case(node_type)
    flow_id = int(input_data.get('flow_id'))
    logger.info(f'Updating the data for flow: {flow_id}, node {input_data["node_id"]}')
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
        add_func(parsed_input)
    except Exception as e:
        logger.error(e)
        raise HTTPException(419, str(f'error: {e}'))


@router.get('/files/available_flow_files', tags=['editor'], response_model=List[FileInfo])
def get_list_of_saved_flows(path: str):
    try:
        return get_files_from_directory(path, types=['flowfile'])
    except:
        return []

@router.get('/node_list', response_model=List[nodes.NodeTemplate])
def get_node_list() -> List[nodes.NodeTemplate]:
    return nodes.nodes_list


# @router.post('/reset')
# def reset():
#     flow_file_handler.delete_flow(1)
#     register_flow(schemas.FlowSettings(flow_id=1))


@router.post('/files/remove_items', tags=['file manager'])
def remove_items(remove_items_input: input_schema.RemoveItemsInput):
    result, error = remove_paths(remove_items_input)
    if result:
        return result
    else:
        raise error


@router.get('/node', response_model=output_model.NodeData, tags=['editor'])
def get_node(flow_id: int, node_id: int, get_data: bool = False):
    logging.info(f'Getting node {node_id} from flow {flow_id}')
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(422, 'Not found')
    v = node.get_node_data(flow_id=flow.flow_id, include_example=get_data)
    return v


@router.post('/node/description/', tags=['editor'])
def update_description_node(flow_id: int, node_id: int, description: str = Body(...)):
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except:
        raise HTTPException(404, 'Could not find the node')
    node.setting_input.description = description
    return True


@router.get('/node/description', tags=['editor'])
def get_description_node(flow_id: int, node_id: int):
    try:
        node = flow_file_handler.get_flow(flow_id).get_node(node_id)
    except:
        raise HTTPException(404, 'Could not find the node')
    if node is None:
        raise HTTPException(404, 'Could not find the node')
    return node.setting_input.description


@router.get('/node/data', response_model=output_model.TableExample, tags=['editor'])
def get_table_example(flow_id: int, node_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    return node.get_table_example(True)


@router.get('/node/downstream_node_ids', response_model=List[int], tags=['editor'])
async def get_downstream_node_ids(flow_id: int, node_id: int) -> List[int]:
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    return list(node.get_all_dependent_node_ids())


@router.get('/import_flow/', tags=['editor'], response_model=int)
def import_saved_flow(flow_path: str) -> int:
    flow_path = Path(flow_path)
    if not flow_path.exists():
        raise HTTPException(404, 'File not found')
    return flow_file_handler.import_flow(flow_path)


@router.get('/save_flow', tags=['editor'])
def save_flow(flow_id: int, flow_path: str = None):
    flow = flow_file_handler.get_flow(flow_id)
    flow.save_flow(flow_path=flow_path)


@router.get('/flow_data', tags=['manager'])
def get_flow_frontend_data(flow_id: Optional[int] = 1):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    return flow.get_frontend_data()


@router.get('/flow_settings', tags=['manager'], response_model=schemas.FlowSettings)
def get_flow_settings(flow_id: Optional[int] = 1) -> schemas.FlowSettings:
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    return flow.flow_settings


@router.post('/flow_settings', tags=['manager'])
def update_flow_settings(flow_settings: schemas.FlowSettings):
    flow = flow_file_handler.get_flow(flow_settings.flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    flow.flow_settings = flow_settings


@router.get('/flow_data/v2', tags=['manager'])
def get_vue_flow_data(flow_id: int) -> schemas.VueFlowInput:
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(404, 'could not find the flow')
    data = flow.get_vue_flow_input()
    return data


@router.get('/analysis_data/graphic_walker_input', tags=['analysis'], response_model=input_schema.NodeExploreData)
def get_graphic_walker_input(flow_id: int, node_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(node_id)
    if node.results.analysis_data_generator is None:
        logger.error('The data is not refreshed and available for analysis')
        raise HTTPException(422, 'The data is not refreshed and available for analysis')
    return AnalyticsProcessor.process_graphic_walker_input(node)


@router.get('/custom_functions/instant_result', tags=[])
async def get_instant_function_result(flow_id: int, node_id: int, func_string: str):
    try:
        node = flow_file_handler.get_node(flow_id, node_id)
    except Exception as e:
        raise HTTPException(404, str(e))
    return get_instant_func_results(node, func_string)


@router.get('/api/get_xlsx_sheet_names', tags=['excel_reader'], response_model=List[str])
async def get_excel_sheet_names(path: str) -> List[str] | None:
    sheet_names = excel_file_manager.get_sheet_names(path)
    if sheet_names:
        return sheet_names
    else:
        raise HTTPException(404, 'File not found')


@router.post("/validate_db_settings")
async def validate_db_settings(
        database_settings: input_schema.DatabaseSettings,
        current_user=Depends(get_current_active_user)
):
    """
    Validate the query settings for a database connection.
    """
    # Validate the query settings
    try:
        sql_source = create_sql_source_from_db_settings(database_settings, user_id=current_user.id)
        sql_source.validate()
        return {"message": "Query settings are valid"}
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
