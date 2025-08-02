from typing import Optional, Literal

from polars._typing import (CsvEncoding)

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, cloud_storage_schemas
from flowfile_frame.cloud_storage.secret_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id


def add_write_ff_to_cloud_storage(path: str,
                                  flow_graph: Optional[FlowGraph],
                                  depends_on_node_id: int,
                                  *,
                                  connection_name: Optional[str] = None,
                                  write_mode: Literal["overwrite", "append"] = "overwrite",
                                  file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
                                  csv_delimiter: str = ";",
                                  csv_encoding: CsvEncoding = "utf8",
                                  parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
                                  description: Optional[str] = None
                              ) -> int:
    node_id = generate_node_id()
    flow_id = flow_graph.flow_id
    settings = input_schema.NodeCloudStorageWriter(
        flow_id=flow_id,
        node_id=node_id,
        cloud_storage_settings=cloud_storage_schemas.CloudStorageWriteSettings(resource_path=path,
                                                                               connection_name=connection_name,
                                                                               file_format=file_format,
                                                                               write_mode=write_mode,
                                                                               csv_delimiter=csv_delimiter,
                                                                               csv_encoding=csv_encoding,
                                                                               parquet_compression=parquet_compression),
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description)
    flow_graph.add_cloud_storage_writer(settings)
    return node_id
