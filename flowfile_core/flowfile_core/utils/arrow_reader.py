import pyarrow as pa
from typing import List, Iterator, Tuple, Callable
from flowfile_core.configs import logger


def open_validated_file(file_path: str, n: int) -> pa.OSFile:
    """
    Validate and open an Arrow file with input parameter checking.

    This function performs validation on the input parameters and opens the Arrow file
    in binary read mode. It includes checks for file existence and parameter types.

    Args:
        file_path (str): Path to the Arrow file to be opened.
        n (int): Number of rows to be read. Used for validation purposes.
            Must be non-negative.

    Returns:
        pa.OSFile: An open PyArrow file object ready for reading.

    Raises:
        ValueError: If n is negative.
        TypeError: If file_path is not a string.
        FileNotFoundError: If the specified file does not exist.

    Example:
        >>> file = open_validated_file("data.arrow", 1000)
        >>> # Use the file object
        >>> file.close()
    """
    logger.debug(f"Attempting to open file: {file_path} with n={n}")
    if n < 0:
        logger.error(f"Invalid negative row count requested: {n}")
        raise ValueError("Number of rows must be non-negative")
    if not isinstance(file_path, str):
        logger.error(f"Invalid file_path type: {type(file_path)}")
        raise TypeError("file_path must be a string")
    try:
        file = pa.OSFile(file_path, 'rb')
        logger.info(f"Successfully opened file: {file_path}")
        return file
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Could not find file: {file_path}")


def create_reader(source: pa.OSFile) -> pa.ipc.RecordBatchFileReader:
    """
    Create a RecordBatchFileReader from an open Arrow file.

    This function initializes a reader that can process Arrow IPC file formats.
    It handles the creation of the reader and validates the file format.

    Args:
        source (pa.OSFile): An open PyArrow file object.

    Returns:
        pa.ipc.RecordBatchFileReader: A reader object for processing Arrow record batches.

    Raises:
        ValueError: If the file is not a valid Arrow format.

    Example:
        >>> with open_validated_file("data.arrow", 1000) as source:
        ...     reader = create_reader(source)
        ...     # Use the reader
    """
    try:
        reader = pa.ipc.open_file(source)
        logger.debug(f"Created reader with {reader.num_record_batches} batches")
        return reader
    except pa.ArrowInvalid:
        logger.error("Failed to create reader: Invalid Arrow file format")
        raise ValueError("Invalid Arrow file format")


def iter_batches(reader: pa.ipc.RecordBatchFileReader, n: int, rows_collected: int) -> Iterator[pa.RecordBatch]:
    """
    Iterator over record batches with row limit handling.

    Yields record batches from the reader, handling the case where the last batch
    needs to be sliced to meet the requested row count. This function provides
    efficient batch-wise processing of Arrow data.

    Args:
        reader (pa.ipc.RecordBatchFileReader): The Arrow file reader.
        n (int): Maximum number of rows to read in total.
        rows_collected (int): Number of rows already collected before this iteration.

    Yields:
        pa.RecordBatch: Record batches containing the data, with the last batch
            potentially sliced to meet the row count requirement.

    Example:
        >>> reader = create_reader(source)
        >>> for batch in iter_batches(reader, 1000, 0):
        ...     # Process each batch
        ...     process_batch(batch)
    """
    logger.debug(f"Starting batch iteration: target={n}, collected={rows_collected}")
    for i in range(reader.num_record_batches):
        batch = reader.get_record_batch(i)
        batch_rows = batch.num_rows
        logger.debug(f"Processing batch {i}: {batch_rows} rows")

        if rows_collected + batch_rows <= n:
            yield batch
        else:
            remaining_rows = n - rows_collected
            logger.debug(f"Slicing final batch to {remaining_rows} rows")
            yield batch.slice(0, remaining_rows)
            break


def collect_batches(reader: pa.ipc.RecordBatchFileReader, n: int) -> Tuple[List[pa.RecordBatch], int]:
    """
    Collect record batches from a reader up to a specified number of rows.

    This function aggregates record batches while respecting the total row count
    limit. It's useful for building a complete dataset from multiple batches.

    Args:
        reader (pa.ipc.RecordBatchFileReader): The Arrow file reader.
        n (int): Maximum number of rows to collect.

    Returns:
        Tuple[List[pa.RecordBatch], int]: A tuple containing:
            - List of collected record batches
            - Total number of rows collected

    Example:
        >>> reader = create_reader(source)
        >>> batches, row_count = collect_batches(reader, 1000)
        >>> print(f"Collected {row_count} rows in {len(batches)} batches")
    """
    logger.debug(f"Collecting batches up to {n} rows")
    batches: List[pa.RecordBatch] = []
    rows_collected = 0

    for batch in iter_batches(reader, n, rows_collected):
        batches.append(batch)
        rows_collected += batch.num_rows
        logger.debug(f"Collected batch: total rows now {rows_collected}")
        if rows_collected >= n:
            break

    logger.info(f"Finished collecting {len(batches)} batches with {rows_collected} total rows")
    return batches, rows_collected


def read(file_path: str) -> pa.Table:
    """
    Read an entire Arrow file into a Table.

    This function provides a simple interface to read all data from an Arrow file.
    It handles file opening, reading, and proper resource cleanup.

    Args:
        file_path (str): Path to the Arrow file to read.

    Returns:
        pa.Table: PyArrow Table containing all data from the file.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the file is not a valid Arrow format.

    Example:
        >>> table = read("data.arrow")
        >>> print(f"Read {len(table)} rows")
        >>> print(f"Columns: {table.column_names}")
    """
    logger.info(f"Reading entire file: {file_path}")
    with open_validated_file(file_path, 0) as source:
        reader = create_reader(source)
        batches, total_rows = collect_batches(reader, float('inf'))
        table = pa.Table.from_batches(batches)  # type: ignore
        logger.info(f"Successfully read {total_rows} rows from {file_path}")
        return table


def read_top_n(file_path: str, n: int = 1000, strict: bool = False) -> pa.Table:
    """
    Read the first N rows from an Arrow file.

    This function provides efficient reading of a limited number of rows from an
    Arrow file. It's useful for data sampling and preview operations.

    Args:
        file_path (str): Path to the Arrow file to read.
        n (int, optional): Number of rows to read. Defaults to 1000.
        strict (bool, optional): If True, raises an error when fewer than n rows
            are available. Defaults to False.

    Returns:
        pa.Table: PyArrow Table containing up to n rows of data.

    Raises:
        ValueError: If strict=True and fewer than n rows are available,
            or if n is negative.
        FileNotFoundError: If the file doesn't exist.

    Example:
        >>> # Read first 1000 rows
        >>> table = read_top_n("data.arrow")
        >>> # Read exactly 500 rows with strict checking
        >>> table = read_top_n("data.arrow", n=500, strict=True)
    """
    logger.info(f"Reading top {n} rows from {file_path} (strict={strict})")
    with open_validated_file(file_path, n) as source:
        reader = create_reader(source)
        batches, rows_collected = collect_batches(reader, n)

        if strict and rows_collected < n:
            logger.error(f"Strict mode: requested {n} rows but only {rows_collected} available")
            raise ValueError(f"Requested {n} rows but only {rows_collected} available")

        table = pa.Table.from_batches(batches)  # type: ignore
        logger.info(f"Successfully read {rows_collected} rows from {file_path}")
        return table


def get_read_top_n(file_path: str, n: int = 1000, strict: bool = False) -> Callable[[], pa.Table]:
    """
    Create a callable that reads the first N rows from an Arrow file.

    This function returns a closure that can be called later to read data.
    It's useful for creating reusable data reading functions with fixed parameters.

    Args:
        file_path (str): Path to the Arrow file to read.
        n (int, optional): Number of rows to read. Defaults to 1000.
        strict (bool, optional): If True, raises an error when fewer than n rows
            are available. Defaults to False.

    Returns:
        Callable[[], pa.Table]: A function that when called, reads and returns
            the data as a PyArrow Table.

    Example:
        >>> # Create a reader function for the first 500 rows
        >>> reader_func = get_read_top_n("data.arrow", n=500)
        >>> # Later, use the function to read the data
        >>> table = reader_func()
    """
    logger.info(f"Creating reader function for {file_path} with n={n}, strict={strict}")
    return lambda: read_top_n(file_path, n, strict)