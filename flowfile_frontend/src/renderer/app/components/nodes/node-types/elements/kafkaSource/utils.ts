import { NodeKafkaSource, KafkaSourceSettings } from "../../../baseNode/nodeInput";

export const createNodeKafkaSource = (
  flowId: number,
  nodeId: number,
): NodeKafkaSource => {
  const kafkaSettings: KafkaSourceSettings = {
    kafka_connection_id: null,
    kafka_connection_name: null,
    topic_name: "",
    value_format: "json",
    sync_name: "",
    start_offset: "latest",
    max_messages: 100000,
    poll_timeout_seconds: 30,
  };
  const nodeKafkaSource: NodeKafkaSource = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    kafka_settings: kafkaSettings,
    cache_results: false,
    fields: [],
  };

  return nodeKafkaSource;
};
