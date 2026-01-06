import { NodeCloudStorageReader, CloudStorageReadSettings } from "../../../baseNode/nodeInput";

export const createNodeCloudStorageReader = (
  flowId: number,
  nodeId: number,
): NodeCloudStorageReader => {
  const cloudStorageReadSettings: CloudStorageReadSettings = {
    auth_mode: "aws-cli",
    scan_mode: "directory",
    resource_path: "",
    file_format: undefined,
    csv_has_header: false,
    csv_encoding: "utf8",
    delta_version: undefined,
  };
  const nodePolarsCode: NodeCloudStorageReader = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    cloud_storage_settings: cloudStorageReadSettings,
    cache_results: false,
    fields: [],
  };

  return nodePolarsCode;
};
