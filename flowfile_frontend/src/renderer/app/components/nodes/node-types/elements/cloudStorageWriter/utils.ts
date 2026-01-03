import {
  NodeCloudStorageWriter,
  CloudStorageWriteSettings,
} from "../../../baseNode/nodeInput";

/**
 * Creates a default NodeCloudStorageWriter object with initial settings.
 * @param flowId - The ID of the current flow.
 * @param nodeId - The ID of the new node.
 * @returns A NodeCloudStorageWriter object.
 */
export const createNodeCloudStorageWriter = (
  flowId: number,
  nodeId: number
): NodeCloudStorageWriter => {
  const cloudStorageWriteSettings: CloudStorageWriteSettings = {
    auth_mode: "aws-cli", // Default to local credentials
    connection_name: undefined,
    resource_path: "",
    write_mode: "overwrite",
    file_format: "parquet", // Parquet is a common, efficient default
    parquet_compression: "snappy",
    csv_delimiter: ",",
    csv_encoding: "utf8",
  };

  const nodeWriter: NodeCloudStorageWriter = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    cloud_storage_settings: cloudStorageWriteSettings,
    cache_results: false,
  };

  return nodeWriter;
};