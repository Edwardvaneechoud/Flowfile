"""The ``merge_delta`` op as the cloud Delta writer dispatches it: a ``{"connection": ...}`` storage payload
with an owner-encrypted secret plus ``enable_cdf``. Run against a local path, so it proves the payload
contract (decrypt + options + change-feed creation) without object storage.
"""

from multiprocessing import Queue

import polars as pl

from flowfile_worker import mp_context
from flowfile_worker.funcs import merge_delta
from flowfile_worker.secrets import encrypt_secret
from shared.delta_utils import get_change_data_feed_floor, is_change_data_feed_enabled


def test_merge_delta_with_cloud_connection_payload_creates_a_tracked_table(tmp_path):
    output_path = str(tmp_path / "tracked")
    progress, error_message, queue = mp_context.Value("i", 0), mp_context.Array("c", 1024), Queue(maxsize=1)
    payload = {
        "connection": {
            "storage_type": "s3",
            "auth_method": "access_key",
            "connection_name": "minio-test",
            "aws_region": "us-east-1",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": encrypt_secret("minioadmin", user_id=1),
            "endpoint_url": "http://localhost:9000",
            "aws_allow_unsafe_html": True,
        }
    }

    merge_delta(
        polars_serializable_object=pl.LazyFrame({"id": [1, 2], "name": ["a", "b"]}).serialize(),
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path="",
        output_path=output_path,
        merge_mode="upsert",
        merge_keys=["id"],
        storage_payload=payload,
        enable_cdf=True,
    )

    assert error_message.value == b"", error_message.value
    assert progress.value == 100
    result = queue.get(timeout=5)
    assert result["table_path"] == output_path
    assert result["row_count"] == 2
    assert is_change_data_feed_enabled(output_path)
    assert get_change_data_feed_floor(output_path) == 0
