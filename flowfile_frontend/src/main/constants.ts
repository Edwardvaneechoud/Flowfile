// constants.ts
export const SHUTDOWN_TIMEOUT = 3000;
export const FORCE_KILL_TIMEOUT = 2000;
export const HEALTH_CHECK_TIMEOUT = 1000;
export const SERVICE_START_TIMEOUT = 30000;

// Default service ports (used as preferred ports; actual ports may differ if these are in use)
export const DEFAULT_WORKER_PORT = 63579;
export const DEFAULT_CORE_PORT = 63578;

// Runtime service ports (updated after dynamic port assignment)
export let WORKER_PORT = DEFAULT_WORKER_PORT;
export let CORE_PORT = DEFAULT_CORE_PORT;

export function setRuntimePorts(corePort: number, workerPort: number): void {
  CORE_PORT = corePort;
  WORKER_PORT = workerPort;
}
