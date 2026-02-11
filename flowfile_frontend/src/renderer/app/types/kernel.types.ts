// Kernel management related TypeScript interfaces and types

export type KernelState = "stopped" | "starting" | "restarting" | "idle" | "executing" | "error";

export interface KernelConfig {
  id: string;
  name: string;
  packages: string[];
  cpu_cores: number;
  memory_gb: number;
  gpu: boolean;
}

export interface DockerStatus {
  available: boolean;
  image_available: boolean;
  error: string | null;
}

export interface KernelInfo {
  id: string;
  name: string;
  state: KernelState;
  container_id: string | null;
  port: number | null;
  packages: string[];
  memory_gb: number;
  cpu_cores: number;
  gpu: boolean;
  created_at: string;
  error_message: string | null;
  kernel_version: string | null;
}

export interface DisplayOutput {
  mime_type: string;
  data: string;
  title: string;
}

export interface ExecuteResult {
  success: boolean;
  output_paths: string[];
  artifacts_published: string[];
  artifacts_deleted: string[];
  display_outputs: DisplayOutput[];
  stdout: string;
  stderr: string;
  error: string | null;
  execution_time_ms: number;
}

export interface ExecuteCellRequest {
  node_id: number;
  code: string;
  flow_id: number;
}

export interface KernelMemoryInfo {
  used_bytes: number;
  limit_bytes: number;
  usage_percent: number;
}
