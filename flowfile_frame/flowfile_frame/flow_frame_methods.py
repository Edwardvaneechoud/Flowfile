import logging
import os
from typing import Any, Iterable, List, Literal, Optional, Tuple, Union, Dict, Callable
from pathlib import Path

import io
import polars as pl
from polars._typing import (SchemaDict, IO,PolarsDataType,
                            Sequence, CsvEncoding)

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema, transform_schema

from flowfile_frame.expr import col

from flowfile_frame.utils import create_flow_graph
from flowfile_frame.flow_frame import generate_node_id, FlowFrame
from flowfile_frame.config import logger

def sum(expr):
    """Sum aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.sum()


def mean(expr):
    """Mean aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.mean()


def min(expr):
    """Min aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.min()


def max(expr):
    """Max aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.max()


def count(expr):
    """Count aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.count()


def read_csv(
        source: Union[str, Path, IO[bytes], bytes, List[Union[str, Path, IO[bytes], bytes]]],
        *,
        flow_graph: Optional[Any] = None, # Using Any for FlowGraph placeholder
        separator: str = ',',
        convert_to_absolute_path: bool = True,
        description: Optional[str] = None,
        has_header: bool = True,
        new_columns: Optional[List[str]] = None,
        comment_prefix: Optional[str] = None,
        quote_char: Optional[str] = '"',
        skip_rows: int = 0,
        skip_lines: int = 0,
        schema: Optional[SchemaDict] = None,
        schema_overrides: Optional[Union[SchemaDict, Sequence[PolarsDataType]]] = None,
        null_values: Optional[Union[str, List[str], Dict[str, str]]] = None,
        missing_utf8_is_empty_string: bool = False,
        ignore_errors: bool = False,
        try_parse_dates: bool = False,
        infer_schema: bool = True,
        infer_schema_length: Optional[int] = 100,
        n_rows: Optional[int] = None,
        encoding: CsvEncoding = 'utf8',
        low_memory: bool = False,
        rechunk: bool = False,
        storage_options: Optional[Dict[str, Any]] = None,
        skip_rows_after_header: int = 0,
        row_index_name: Optional[str] = None,
        row_index_offset: int = 0,
        eol_char: str = '\n',
        raise_if_empty: bool = True,
        truncate_ragged_lines: bool = False,
        decimal_comma: bool = False,
        glob: bool = True,
        cache: bool = True,
        with_column_names: Optional[Callable[[List[str]], List[str]]] = None,
        **other_options: Any
) -> FlowFrame:
    """
    Read a CSV file into a FlowFrame.

    This function uses the native FlowGraph implementation when the parameters
    fall within the supported range, and falls back to using Polars' scan_csv implementation
    for more advanced features.

    Args:
        source: Path(s) to CSV file(s), or a file-like object.
        flow_graph: if you want to add it to an existing graph
        separator: Single byte character to use as separator in the file.
        convert_to_absolute_path: If the path needs to be set to a fixed location
        description: if you want to add a readable name in the frontend (advised)

        # Polars.scan_csv aligned parameters
        has_header: Indicate if the first row of the dataset is a header or not.
        new_columns: Rename columns after selection.
        comment_prefix: String that indicates a comment line if found at beginning of line.
        quote_char: Character used for quoting. None to disable.
        skip_rows: Start reading after this many rows.
        skip_lines: Skip this many lines by newline char only.
        schema: Schema to use when reading the CSV.
        schema_overrides: Schema overrides for specific columns.
        null_values: Values to interpret as null.
        missing_utf8_is_empty_string: Treat missing utf8 values as empty strings.
        ignore_errors: Try to keep reading lines if some parsing errors occur.
        try_parse_dates: Try to automatically parse dates.
        infer_schema: Boolean flag. If False, `infer_schema_length` for Polars is set to 0.
        infer_schema_length: Number of rows to use for schema inference. Polars default is 100.
        n_rows: Stop reading after this many rows.
        encoding: Character encoding to use.
        low_memory: Reduce memory usage at the cost of performance.
        rechunk: Ensure data is in contiguous memory layout after parsing.
        storage_options: Options for fsspec for cloud storage.
        skip_rows_after_header: Skip rows after header.
        row_index_name: Name of the row index column.
        row_index_offset: Start value for the row index.
        eol_char: End of line character.
        raise_if_empty: Raise error if file is empty.
        truncate_ragged_lines: Truncate lines with too many values.
        decimal_comma: Parse floats with decimal comma.
        glob: Use glob pattern for file path (if source is a string).
        cache: Cache the result after reading (Polars default True).
        with_column_names: Apply a function over the column names.
        other_options: Any other options to pass to polars.scan_csv (e.g. retries, file_cache_ttl).

    Returns:
        A FlowFrame with the CSV data.
    """
    node_id = generate_node_id() # Assuming generate_node_id is defined
    if flow_graph is None:
        flow_graph = create_flow_graph() # Assuming create_flow_graph is defined
    flow_id = flow_graph.flow_id

    current_source_path_for_native = None
    if isinstance(source, (str, os.PathLike)):
        current_source_path_for_native = str(source)
        if '~' in current_source_path_for_native:
            current_source_path_for_native = os.path.expanduser(current_source_path_for_native)
    elif isinstance(source, list) and all(isinstance(s, (str, os.PathLike)) for s in source):
        current_source_path_for_native = str(source[0]) if source else None
        if current_source_path_for_native and '~' in current_source_path_for_native:
             current_source_path_for_native = os.path.expanduser(current_source_path_for_native)
    elif isinstance(source, (io.BytesIO, io.StringIO)):
        logger.warning("Read from bytes io from csv not supported, converting data to raw data")
        return from_dict(pl.read_csv(source), flow_graph=flow_graph, description=description)
    actual_infer_schema_length: Optional[int]
    if not infer_schema:
        actual_infer_schema_length = 0
    else:
        actual_infer_schema_length = infer_schema_length
    can_use_native = (
            current_source_path_for_native is not None and
            comment_prefix is None and
            skip_lines == 0 and
            schema is None and
            schema_overrides is None and
            null_values is None and
            not missing_utf8_is_empty_string and
            not try_parse_dates and
            n_rows is None and
            not low_memory and
            not rechunk and
            storage_options is None and
            skip_rows_after_header == 0 and
            row_index_name is None and
            row_index_offset == 0 and
            eol_char == '\n' and
            not decimal_comma and
            new_columns is None and
            glob is True
    )
    if can_use_native and current_source_path_for_native:
        received_table = input_schema.ReceivedTable(
            file_type='csv',
            path=current_source_path_for_native,
            name=Path(current_source_path_for_native).name,
            delimiter=separator,
            has_headers=has_header,
            encoding=encoding,
            starting_from_line=skip_rows,
            quote_char=quote_char if quote_char is not None else '"',
            infer_schema_length=actual_infer_schema_length if actual_infer_schema_length is not None else 10000,
            truncate_ragged_lines=truncate_ragged_lines,
            ignore_errors=ignore_errors,
            row_delimiter=eol_char
        )
        if convert_to_absolute_path:
            try:
                received_table.set_absolute_filepath()
                received_table.path = received_table.abs_file_path
            except Exception as e:
                logger.warning(f"Could not determine absolute path for {current_source_path_for_native}: {e}")

        read_node_description = description or f"Read CSV from {Path(current_source_path_for_native).name}"
        read_node = input_schema.NodeRead(
            flow_id=flow_id,
            node_id=node_id,
            received_file=received_table,
            pos_x=100,
            pos_y=100,
            is_setup=True,
            description=read_node_description
        )
        flow_graph.add_read(read_node)
        result_frame = FlowFrame(
            data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
            flow_graph=flow_graph,
            node_id=node_id
        )
        return result_frame
    else:
        polars_source_arg = source
        polars_code = _build_polars_code_args(
            source=polars_source_arg,
            separator=separator,
            has_header=has_header,
            new_columns=new_columns,
            comment_prefix=comment_prefix,
            quote_char=quote_char,
            skip_rows=skip_rows,
            skip_lines=skip_lines,
            schema=schema,
            schema_overrides=schema_overrides,
            null_values=null_values,
            missing_utf8_is_empty_string=missing_utf8_is_empty_string,
            ignore_errors=ignore_errors,
            try_parse_dates=try_parse_dates,
            infer_schema_length=actual_infer_schema_length,
            n_rows=n_rows,
            encoding=encoding,
            low_memory=low_memory,
            rechunk=rechunk,
            storage_options=storage_options,
            skip_rows_after_header=skip_rows_after_header,
            row_index_name=row_index_name,
            row_index_offset=row_index_offset,
            eol_char=eol_char,
            raise_if_empty=raise_if_empty,
            truncate_ragged_lines=truncate_ragged_lines,
            decimal_comma=decimal_comma,
            glob=glob,
            cache=cache,
            with_column_names=with_column_names,
            **other_options
        )
        polars_code_node_description = description or "Read CSV with Polars scan_csv"
        if isinstance(source, (str, os.PathLike)):
            polars_code_node_description = description or f"Read CSV with Polars scan_csv from {Path(source).name}"
        elif isinstance(source, list) and source and isinstance(source[0], (str, os.PathLike)):
            polars_code_node_description = description or f"Read CSV with Polars scan_csv from {Path(source[0]).name} (and possibly others)"

        # Assuming input_schema.NodePolarsCode, transform_schema.PolarsCodeInput are defined
        polars_code_settings = input_schema.NodePolarsCode(
            flow_id=flow_id,
            node_id=node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=polars_code),
            is_setup=True,
            description=polars_code_node_description
        )
        flow_graph.add_polars_code(polars_code_settings)
        return FlowFrame(
            data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
            flow_graph=flow_graph,
            node_id=node_id,
        )

def _build_polars_code_args(
    source: Union[str, Path, IO[bytes], bytes, List[Union[str, Path, IO[bytes], bytes]]],
    separator: str,
    has_header: bool,
    new_columns: Optional[List[str]],
    comment_prefix: Optional[str],
    quote_char: Optional[str],
    skip_rows: int,
    skip_lines: int,
    schema: Optional[SchemaDict],
    schema_overrides: Optional[Union[SchemaDict, Sequence[PolarsDataType]]],
    null_values: Optional[Union[str, List[str], Dict[str, str]]],
    missing_utf8_is_empty_string: bool,
    ignore_errors: bool,
    try_parse_dates: bool,
    infer_schema_length: Optional[int],
    n_rows: Optional[int],
    encoding: CsvEncoding,
    low_memory: bool,
    rechunk: bool,
    storage_options: Optional[Dict[str, Any]],
    skip_rows_after_header: int,
    row_index_name: Optional[str],
    row_index_offset: int,
    eol_char: str,
    raise_if_empty: bool,
    truncate_ragged_lines: bool,
    decimal_comma: bool,
    glob: bool,
    cache: bool,
    with_column_names: Optional[Callable[[List[str]], List[str]]],
    **other_options: Any
) -> str:
    source_repr: str
    if isinstance(source, (str, Path)):
        source_repr = repr(str(source))
    elif isinstance(source, list):
        source_repr = repr([str(p) for p in source])
    elif isinstance(source, bytes):
        source_repr = "source_bytes_obj"
    elif hasattr(source, 'read'):
        source_repr = "source_file_like_obj"
    else:
        source_repr = repr(source)

    param_mapping = {
        'has_header': (True, lambda x: str(x)),
        'separator': (',', lambda x: repr(str(x))),
        'comment_prefix': (None, lambda x: repr(str(x)) if x is not None else 'None'),
        'quote_char': ('"', lambda x: repr(str(x)) if x is not None else 'None'),
        'skip_rows': (0, str),
        'skip_lines': (0, str),
        'schema': (None, lambda x: repr(x) if x is not None else 'None'),
        'schema_overrides': (None, lambda x: repr(x) if x is not None else 'None'),
        'null_values': (None, lambda x: repr(x) if x is not None else 'None'),
        'missing_utf8_is_empty_string': (False, str),
        'ignore_errors': (False, str),
        'cache': (True, str),
        'with_column_names': (None, lambda x: repr(x) if x is not None else 'None'),
        'infer_schema_length': (100, lambda x: str(x) if x is not None else 'None'),
        'n_rows': (None, lambda x: str(x) if x is not None else 'None'),
        'encoding': ('utf8', lambda x: repr(str(x))),
        'low_memory': (False, str),
        'rechunk': (False, str),
        'skip_rows_after_header': (0, str),
        'row_index_name': (None, lambda x: repr(str(x)) if x is not None else 'None'),
        'row_index_offset': (0, str),
        'try_parse_dates': (False, str),
        'eol_char': ('\n', lambda x: repr(str(x))),
        'new_columns': (None, lambda x: repr(x) if x is not None else 'None'),
        'raise_if_empty': (True, str),
        'truncate_ragged_lines': (False, str),
        'decimal_comma': (False, str),
        'glob': (True, str),
        'storage_options': (None, lambda x: repr(x) if x is not None else 'None'),
    }

    all_vars = locals()
    kwargs_list = []

    for param_name_key, (default_value, format_func) in param_mapping.items():
        value = all_vars.get(param_name_key)
        formatted_value = format_func(value)
        kwargs_list.append(f"{param_name_key}={formatted_value}")

    if other_options:
        for k, v in other_options.items():
            kwargs_list.append(f"{k}={repr(v)}")

    kwargs_str = ",\n    ".join(kwargs_list)

    if kwargs_str:
        polars_code = f"output_df = pl.scan_csv(\n    {source_repr},\n    {kwargs_str}\n)"
    else:
        polars_code = f"output_df = pl.scan_csv({source_repr})"

    return polars_code


def read_parquet(file_path, *, flow_graph: FlowGraph = None, description: str = None,
                 convert_to_absolute_path: bool = True, **options) -> FlowFrame:
    """
    Read a Parquet file into a FlowFrame.

    Args:
        file_path: Path to Parquet file
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
        convert_to_absolute_path: If the path needs to be set to a fixed location
        **options: Options for polars.read_parquet

    Returns:
        A FlowFrame with the Parquet data
    """
    if '~' in file_path:
        file_path = os.path.expanduser(file_path)
    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id

    received_table = input_schema.ReceivedTable(
        file_type='parquet',
        path=file_path,
        name=Path(file_path).name,
    )
    if convert_to_absolute_path:
        received_table.path = received_table.abs_file_path

    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    flow_graph.add_read(read_node)

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def from_dict(data, *, flow_graph: FlowGraph = None, description: str = None) -> FlowFrame:
    """
    Create a FlowFrame from a dictionary or list of dictionaries.

    Args:
        data: Dictionary of lists or list of dictionaries
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
    Returns:
        A FlowFrame with the data
    """
    # Create new node ID
    node_id = generate_node_id()

    if not flow_graph:
        flow_graph = create_flow_graph()
    flow_id = flow_graph.flow_id

    input_node = input_schema.NodeManualInput(
        flow_id=flow_id,
        node_id=node_id,
        raw_data=FlowDataEngine(data).to_pylist(),
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    # Add to graph
    flow_graph.add_manual_input(input_node)

    # Return new frame
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def concat(frames: List['FlowFrame'],
                  how: str = 'vertical',
                  rechunk: bool = False,
                  parallel: bool = True,
                  description: str = None) -> 'FlowFrame':
    """
    Concatenate multiple FlowFrames into one.

    Parameters
    ----------
    frames : List[FlowFrame]
        List of FlowFrames to concatenate
    how : str, default 'vertical'
        How to combine the FlowFrames (see concat method documentation)
    rechunk : bool, default False
        Whether to ensure contiguous memory in result
    parallel : bool, default True
        Whether to use parallel processing for the operation
    description : str, optional
        Description of this operation

    Returns
    -------
    FlowFrame
        A new FlowFrame with the concatenated data
    """
    if not frames:
        raise ValueError("No frames provided to concat_frames")
    if len(frames) == 1:
        return frames[0]
    # Use first frame's concat method with remaining frames
    first_frame = frames[0]
    remaining_frames = frames[1:]

    return first_frame.concat(remaining_frames, how=how,
                              rechunk=rechunk, parallel=parallel,
                              description=description)


def scan_csv(
        source: Union[str, Path, IO[bytes], bytes, List[Union[str, Path, IO[bytes], bytes]]],
        *,
        flow_graph: Optional[Any] = None,  # Using Any for FlowGraph placeholder
        separator: str = ',',
        convert_to_absolute_path: bool = True,
        description: Optional[str] = None,
        has_header: bool = True,
        new_columns: Optional[List[str]] = None,
        comment_prefix: Optional[str] = None,
        quote_char: Optional[str] = '"',
        skip_rows: int = 0,
        skip_lines: int = 0,
        schema: Optional[SchemaDict] = None,
        schema_overrides: Optional[Union[SchemaDict, Sequence[PolarsDataType]]] = None,
        null_values: Optional[Union[str, List[str], Dict[str, str]]] = None,
        missing_utf8_is_empty_string: bool = False,
        ignore_errors: bool = False,
        try_parse_dates: bool = False,
        infer_schema: bool = True,
        infer_schema_length: Optional[int] = 100,
        n_rows: Optional[int] = None,
        encoding: CsvEncoding = 'utf8',
        low_memory: bool = False,
        rechunk: bool = False,
        storage_options: Optional[Dict[str, Any]] = None,
        skip_rows_after_header: int = 0,
        row_index_name: Optional[str] = None,
        row_index_offset: int = 0,
        eol_char: str = '\n',
        raise_if_empty: bool = True,
        truncate_ragged_lines: bool = False,
        decimal_comma: bool = False,
        glob: bool = True,
        cache: bool = True,
        with_column_names: Optional[Callable[[List[str]], List[str]]] = None,
        **other_options: Any
) -> FlowFrame:
    """
    Scan a CSV file into a FlowFrame. This function is an alias for read_csv.

    This method is the same as read_csv but is provided for compatibility with
    the polars API where scan_csv returns a LazyFrame.

    See read_csv for full documentation.
    """
    return read_csv(
        source=source,
        flow_graph=flow_graph,
        separator=separator,
        convert_to_absolute_path=convert_to_absolute_path,
        description=description,
        has_header=has_header,
        new_columns=new_columns,
        comment_prefix=comment_prefix,
        quote_char=quote_char,
        skip_rows=skip_rows,
        skip_lines=skip_lines,
        schema=schema,
        schema_overrides=schema_overrides,
        null_values=null_values,
        missing_utf8_is_empty_string=missing_utf8_is_empty_string,
        ignore_errors=ignore_errors,
        try_parse_dates=try_parse_dates,
        infer_schema=infer_schema,
        infer_schema_length=infer_schema_length,
        n_rows=n_rows,
        encoding=encoding,
        low_memory=low_memory,
        rechunk=rechunk,
        storage_options=storage_options,
        skip_rows_after_header=skip_rows_after_header,
        row_index_name=row_index_name,
        row_index_offset=row_index_offset,
        eol_char=eol_char,
        raise_if_empty=raise_if_empty,
        truncate_ragged_lines=truncate_ragged_lines,
        decimal_comma=decimal_comma,
        glob=glob,
        cache=cache,
        with_column_names=with_column_names,
        **other_options
    )


def scan_parquet(
        file_path,
        *,
        flow_graph: FlowGraph = None,
        description: str = None,
        convert_to_absolute_path: bool = True,
        **options
) -> FlowFrame:
    """
    Scan a Parquet file into a FlowFrame. This function is an alias for read_parquet.

    This method is the same as read_parquet but is provided for compatibility with
    the polars API where scan_parquet returns a LazyFrame.

    See read_parquet for full documentation.
    """
    return read_parquet(
        file_path=file_path,
        flow_graph=flow_graph,
        description=description,
        convert_to_absolute_path=convert_to_absolute_path,
        **options
    )

