import os
from flowfile_core.schemas.input_schema import NewDirectory, RemoveItem, RemoveItemsInput
from typing import Tuple, Optional
from flowfile_core.configs import logger

local_database_connection = None


def create_dir(new_directory: NewDirectory) -> Tuple[bool, Optional[Exception]]:
    full_path: str = os.path.join(new_directory.source_path,new_directory.dir_name)
    try:
        os.mkdir(full_path)
        logger.info("Successfully created a new folder")
        return True, None
    except Exception as e:
        return False, e


def remove_path(path: str) -> Tuple[bool, Optional[Exception]]:
    try:
        os.remove(path)
        logger.info(f"Succesfully removed {path}")
        return True, None
    except Exception as e:
        return False, e


def remove_item(item_to_remove: RemoveItem):
    if item_to_remove.id >= 0:
        os.remove(item_to_remove.path)

    elif os.path.isfile(item_to_remove.path):
        os.remove(item_to_remove.path)
    elif os.path.isdir(item_to_remove.path):
        os.rmdir(item_to_remove.path)


def remove_paths(remove_items: RemoveItemsInput) -> Tuple[bool, Optional[Exception]]:
    try:
        for path in remove_items.paths:
            remove_item(path)
        logger.info(f'Successfully removed {remove_items.paths}')
        return True, None
    except Exception as e:
        return False, e
