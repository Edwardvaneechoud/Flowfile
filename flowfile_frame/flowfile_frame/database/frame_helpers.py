"""Database helper functions for FlowFrame operations.

This module provides functions for reading from and writing to databases,
similar to how cloud_storage/frame_helpers.py handles cloud storage operations.
"""

from typing import Literal

import polars as pl

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_frame.database.connection_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id


def add_read_from_database(
    flow_graph: FlowGraph,
    *,
    connection_name: str,
    table_name: str | None = None,
    schema_name: str | None = None,
    query: str | None = None,
    description: str | None = None,
) -> int:
    """Add a database reader node to the flow graph.

    Either table_name or query must be provided. If both are provided,
    query takes precedence.

    Args:
        flow_graph: The flow graph to add the node to.
        connection_name: Name of the stored database connection to use.
        table_name: Name of the table to read from.
        schema_name: Database schema name (e.g., 'public' for PostgreSQL).
        query: SQL query to execute instead of reading a table.
        description: Optional description for the node.

    Returns:
        int: The node ID of the created database reader node.

    Raises:
        ValueError: If neither table_name nor query is provided.
    """
    if table_name is None and query is None:
        raise ValueError("Either 'table_name' or 'query' must be provided")

    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    # Determine query mode
    query_mode: Literal["table", "query"] = "query" if query else "table"

    settings = input_schema.NodeDatabaseReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        description=description,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference",
            database_connection_name=connection_name,
            query_mode=query_mode,
            table_name=table_name,
            schema_name=schema_name,
            query=query,
        ),
    )

    flow_graph.add_database_reader(settings)
    return node_id


def add_write_to_database(
    flow_graph: FlowGraph,
    depends_on_node_id: int,
    *,
    connection_name: str,
    table_name: str,
    schema_name: str | None = None,
    if_exists: Literal["append", "replace", "fail"] = "append",
    description: str | None = None,
) -> int:
    """Add a database writer node to the flow graph.

    Args:
        flow_graph: The flow graph to add the node to.
        depends_on_node_id: The node ID that this writer depends on.
        connection_name: Name of the stored database connection to use.
        table_name: Name of the table to write to.
        schema_name: Database schema name (e.g., 'public' for PostgreSQL).
        if_exists: What to do if the table already exists:
            - 'append': Add rows to existing table
            - 'replace': Drop and recreate table
            - 'fail': Raise an error
        description: Optional description for the node.

    Returns:
        int: The node ID of the created database writer node.
    """
    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    settings = input_schema.NodeDatabaseWriter(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
        database_write_settings=input_schema.DatabaseWriteSettings(
            connection_mode="reference",
            database_connection_name=connection_name,
            table_name=table_name,
            schema_name=schema_name,
            if_exists=if_exists,
        ),
    )

    flow_graph.add_database_writer(settings)
    return node_id


def read_database(
    connection_name: str,
    *,
    table_name: str | None = None,
    schema_name: str | None = None,
    query: str | None = None,
) -> pl.LazyFrame:
    """Read data from a database using a stored connection.

    This is a convenience function for reading data directly without
    needing to create a FlowGraph.

    Either table_name or query must be provided. If both are provided,
    query takes precedence.

    Args:
        connection_name: Name of the stored database connection to use.
        table_name: Name of the table to read from.
        schema_name: Database schema name (e.g., 'public' for PostgreSQL).
        query: SQL query to execute instead of reading a table.

    Returns:
        pl.LazyFrame: The data read from the database.

    Raises:
        ValueError: If neither table_name nor query is provided.
        ValueError: If the connection is not found.
    """
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
    from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
    from flowfile_core.flowfile.sources.external_sources.sql_source.utils import construct_sql_uri
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    if table_name is None and query is None:
        raise ValueError("Either 'table_name' or 'query' must be provided")

    user_id = get_current_user_id()
    connection = get_local_database_connection(connection_name, user_id)

    if connection is None:
        raise ValueError(f"Database connection '{connection_name}' not found")

    # Construct the connection string
    connection_string = construct_sql_uri(
        database_type=connection.database_type,
        host=connection.host,
        port=connection.port,
        database=connection.database,
        username=connection.username,
        password=decrypt_secret(connection.password.get_secret_value()),
        url=connection.url,
    )

    # Create SQL source and read data
    sql_source = SqlSource(
        connection_string=connection_string,
        query=query,
        table_name=table_name,
        schema_name=schema_name,
    )

    return sql_source.get_data()


def write_database(
    df: pl.DataFrame | pl.LazyFrame,
    connection_name: str,
    table_name: str,
    *,
    schema_name: str | None = None,
    if_exists: Literal["append", "replace", "fail"] = "append",
) -> None:
    """Write data to a database using a stored connection.

    This is a convenience function for writing data directly without
    needing to create a FlowGraph.

    Args:
        df: The DataFrame or LazyFrame to write.
        connection_name: Name of the stored database connection to use.
        table_name: Name of the table to write to.
        schema_name: Database schema name (e.g., 'public' for PostgreSQL).
        if_exists: What to do if the table already exists:
            - 'append': Add rows to existing table
            - 'replace': Drop and recreate table
            - 'fail': Raise an error

    Raises:
        ValueError: If the connection is not found.
    """
    from sqlalchemy import create_engine

    from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
    from flowfile_core.flowfile.sources.external_sources.sql_source.utils import construct_sql_uri
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    user_id = get_current_user_id()
    connection = get_local_database_connection(connection_name, user_id)

    if connection is None:
        raise ValueError(f"Database connection '{connection_name}' not found")

    # Collect if LazyFrame
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    # Construct the connection string
    connection_string = construct_sql_uri(
        database_type=connection.database_type,
        host=connection.host,
        port=connection.port,
        database=connection.database,
        username=connection.username,
        password=decrypt_secret(connection.password.get_secret_value()),
        url=connection.url,
    )

    # Write to database using pandas (polars doesn't have direct SQL write support)
    engine = create_engine(connection_string)
    pandas_df = df.to_pandas()

    pandas_df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists=if_exists,
        index=False,
    )
