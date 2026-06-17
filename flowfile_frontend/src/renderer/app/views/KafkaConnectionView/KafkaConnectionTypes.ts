export type KafkaSecurityProtocol = "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL";

export interface KafkaConnectionCreate {
  connection_name: string;
  bootstrap_servers: string;
  security_protocol: KafkaSecurityProtocol;
  sasl_mechanism: string | null;
  sasl_username: string | null;
  sasl_password: string | null;
  ssl_ca_location: string | null;
  ssl_cert_location: string | null;
  ssl_key_pem: string | null;
  schema_registry_url: string | null;
  extra_config: Record<string, string> | null;
}

export interface KafkaConnectionUpdate {
  connection_name?: string | null;
  bootstrap_servers?: string | null;
  security_protocol?: KafkaSecurityProtocol | null;
  sasl_mechanism?: string | null;
  sasl_username?: string | null;
  sasl_password?: string | null;
  ssl_ca_location?: string | null;
  ssl_cert_location?: string | null;
  ssl_key_pem?: string | null;
  schema_registry_url?: string | null;
  extra_config?: Record<string, string> | null;
}

export interface KafkaConnectionOut {
  id: number;
  connection_name: string;
  bootstrap_servers: string;
  security_protocol: string;
  sasl_mechanism: string | null;
  sasl_username: string | null;
  schema_registry_url: string | null;
  created_at: string;
  updated_at: string;
}

export interface KafkaConnectionTestResult {
  success: boolean;
  message: string;
  topics_found: number;
}

export interface KafkaTopicInfo {
  name: string;
  partition_count: number;
}

export interface KafkaSyncCreate {
  sync_name: string;
  kafka_connection_id: number;
  topic_name: string;
  namespace_id: number | null;
  table_name: string;
  write_mode: "append" | "upsert" | "overwrite";
  merge_keys?: string[];
  start_offset: "earliest" | "latest";
}
