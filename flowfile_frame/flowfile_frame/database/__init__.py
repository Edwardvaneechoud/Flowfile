"""Database module for flowfile_frame.

This module provides functions for:
- Managing database connections (create, list, delete)
- Reading from databases
- Writing to databases
"""

from flowfile_frame.database.connection_manager import (
    create_database_connection,
    create_database_connection_if_not_exists,
    del_database_connection,
    get_all_available_database_connections,
    get_database_connection_by_name,
)
from flowfile_frame.database.frame_helpers import (
    add_read_from_database,
    add_write_to_database,
    read_database,
    write_database,
)

__all__ = [
    # Connection management
    "create_database_connection",
    "create_database_connection_if_not_exists",
    "del_database_connection",
    "get_all_available_database_connections",
    "get_database_connection_by_name",
    # FlowGraph helpers
    "add_read_from_database",
    "add_write_to_database",
    # Direct read/write
    "read_database",
    "write_database",
]
