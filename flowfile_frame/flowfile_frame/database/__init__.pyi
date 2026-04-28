# This file was auto-generated to provide type information for flowfile_frame.database
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from flowfile_frame.database.connection_manager import create_database_connection, create_database_connection_if_not_exists, del_database_connection, get_all_available_database_connections, get_database_connection_by_name
from flowfile_frame.database.frame_helpers import add_read_from_database, add_write_to_database, read_database, write_database

