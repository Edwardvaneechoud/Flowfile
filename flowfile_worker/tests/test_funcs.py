from logging import getLogger
from multiprocessing import Queue

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.external_sources.s3_source.models import (
    CloudStorageWriteSettings,
    WriteSettings,
)
from flowfile_worker.funcs import write_to_cloud_storage

logger = getLogger(__name__)


try:
    # noinspection PyUnresolvedReferences
    from test_utils.s3.fixtures import get_minio_client
    from tests.utils import cloud_storage_connection_settings, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    # noinspection PyUnresolvedReferences


def test_write_to_cloud_storage(cloud_storage_connection_settings):
    write_settings = WriteSettings(
        resource_path="s3://worker-test-bucket/func_test_write.parquet",
        file_format="parquet",
        write_mode="overwrite",
        parquet_compression="snappy",
    )
    cloud_write_settings = CloudStorageWriteSettings(
        connection=cloud_storage_connection_settings,
        write_settings=write_settings,
    )
    lf = pl.LazyFrame({'value': [i for i in range(1000)]})
    try:
        write_to_cloud_storage(polars_serializable_object=lf.serialize(),
                               progress=mp_context.Value('i', 0),
                               error_message=mp_context.Array('c', 1024),
                               queue=Queue(maxsize=1),
                               file_path="",
                               cloud_write_settings=cloud_write_settings
                               )
    except Exception as e:
        pytest.fail(f"Write to cloud storage failed: {e}")
