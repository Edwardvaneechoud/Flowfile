export interface ProcessDetails {
  type: string;
  reason: string;
  exitCode: number;
  serviceName?: string;
}

export interface WindowDetails {
  errorCode?: number;
  errorDescription?: string;
}

export interface ShutdownOptions {
  force?: boolean;
  timeout?: number;
}

export type LogLevel = "INFO" | "WARN" | "ERROR" | "DEBUG";

export interface LogEntry {
  level: LogLevel;
  message: string;
  timestamp: string;
  metadata?: Record<string, unknown>;
}
