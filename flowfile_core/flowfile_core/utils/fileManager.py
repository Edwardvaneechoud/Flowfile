import os

from fastapi import HTTPException

from flowfile_core.configs import logger, settings
from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
from flowfile_core.schemas.input_schema import NewDirectory, RemoveItem, RemoveItemsInput
from shared.path_utils import contained_path
from shared.storage_config import storage

local_database_connection = None


def _check_folder_name(name: str) -> None:
    """Refuse a name that is not one plain folder name (a 400), so a request creates exactly one folder."""
    if not name.strip() or name in (".", "..") or any(c in name for c in ("/", "\\", "\0")):
        raise HTTPException(400, "A folder name must be one name, without / or \\")


def create_dir(new_directory: NewDirectory) -> tuple[bool, Exception | None]:
    """Create ``dir_name`` in ``source_path``.

    The error is a 403 for a target outside the allowed roots (off the desktop also outside the user data
    directory, the only place the file browser lists there), a 400 for a name that is not one folder name, or
    the ``OSError`` of the ``mkdir``.
    """
    try:
        _check_folder_name(new_directory.dir_name)
        full_path = validate_path_under_cwd(os.path.join(new_directory.source_path, new_directory.dir_name))
        if not settings.is_electron_mode():
            full_path = contained_path(storage.user_data_directory, full_path)
            if full_path is None:
                raise HTTPException(403, "Access denied: path is outside the allowed directory")
        os.mkdir(full_path)
        logger.info("Successfully created a new folder")
        return True, None
    except Exception as e:
        return False, e


def remove_path(path: str) -> tuple[bool, Exception | None]:
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


def remove_paths(remove_items: RemoveItemsInput) -> tuple[bool, Exception | None]:
    try:
        for path in remove_items.paths:
            remove_item(path)
        logger.info(f"Successfully removed {remove_items.paths}")
        return True, None
    except Exception as e:
        return False, e
