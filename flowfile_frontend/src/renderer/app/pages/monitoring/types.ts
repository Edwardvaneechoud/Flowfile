export interface ProcessInfo {
  task_id: string;
  pid: number | null;
  status: string;
  start_time: number | null;
  end_time: number | null;
}

export interface HealthStatus {
  status: string;
  timestamp: number;
  service_name: string;
  version: string;
  uptime_seconds: number;
}

export interface SystemMetrics {
  timestamp: number;
  cpu_count: number;
  memory_total_mb: number;
  memory_available_mb: number;
  memory_used_mb: number;
  memory_usage_percent: number;
  disk_usage_percent: number | null;
}

export interface ProcessMetrics {
  timestamp: number;
  total_processes: number;
  running_processes: number;
  completed_tasks: number;
  failed_tasks: number;
  processes: ProcessInfo[];
}

export interface MonitoringOverview {
  health: HealthStatus;
  system: SystemMetrics;
  processes: ProcessMetrics;
  service_info: Record<string, string>;
}