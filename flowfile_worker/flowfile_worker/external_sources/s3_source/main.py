"""Cloud storage writer module for FlowFile Worker.

Thin wrapper around shared.cloud_storage for worker-specific dispatch.
"""

from logging import Logger

import polars as pl

from flowfile_worker.external_sources.s3_source.models import CloudStorageWriteSettings
from shared.cloud_storage.writers import write_to_cloud


def write_df_to_cloud(df: pl.LazyFrame, settings: CloudStorageWriteSettings, logger: Logger) -> None:
    """Write a Polars LazyFrame to an object in cloud storage.

    Supports writing to S3, Azure ADLS, and Google Cloud Storage. Currently supports
    'overwrite' write mode. The 'append' mode is not yet implemented for most formats.

    Args:
        df: Polars LazyFrame to write to cloud storage.
        settings: Cloud storage write settings containing connection details and write options.
        logger: Logger instance for logging operations.

    Raises:
        ValueError: If the specified file format is not supported.
        NotImplementedError: If 'append' write mode is used for non-delta formats.
        Exception: If writing to cloud storage fails.
    """
    connection = settings.connection
    write_settings = settings.write_settings
    logger.info(f"Writing to {connection.storage_type} storage: {write_settings.resource_path}")

    storage_options = connection.get_storage_options()
    use_pyarrow = connection.should_use_pyarrow_for_gcs()

    logger.info(f"storage options: {storage_options}")
    logger.info(f"write settings: {write_settings}")
    logger.info(f"resource path: {write_settings.resource_path}")

    write_to_cloud(
        df=df,
        resource_path=write_settings.resource_path,
        storage_options=storage_options,
        file_format=write_settings.file_format,
        write_mode=write_settings.write_mode,
        compression=write_settings.parquet_compression,
        separator=write_settings.csv_delimiter,
        use_pyarrow=use_pyarrow,
        logger=logger,
    )
