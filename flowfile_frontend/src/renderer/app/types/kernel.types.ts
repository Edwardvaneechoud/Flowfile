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
}
