// Kernel management related TypeScript interfaces and types

export type KernelState = "stopped" | "starting" | "idle" | "executing" | "error";

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

export interface ArtifactPersistenceStatus {
  name: string;
  node_id: number;
  type_name: string;
  persisted: boolean;
  in_memory: boolean;
  loaded: boolean;
  size_on_disk?: number;
  persisted_at?: string;
}

export interface PersistenceInfo {
  persistence_enabled: boolean;
  total_artifacts: number;
  persisted_count: number;
  memory_only_count: number;
  disk_usage_bytes: number;
  artifacts: Record<string, ArtifactPersistenceStatus>;
}
