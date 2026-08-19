// Axios wrappers for the /system/worker_pool admin endpoints (warm worker
// pool state + runtime resize). Mirrors the api/*.api.ts pattern: TS-side
// camelCase, snake_case mappers at the boundary. Paths match the FastAPI
// decorators exactly (no trailing slash) to avoid the 307 redirect trap.

import axios from "../services/axios.config";

const WORKER_POOL_URL = "/system/worker_pool";

export interface WorkerPoolMember {
  pid: number;
  state: "idle" | "busy";
  tasksServed: number;
  idleSeconds: number;
  rssMb: number | null;
}

export interface WorkerPoolState {
  enabled: boolean;
  size: number;
  idle: number;
  busy: number;
  activeTasks: number;
  tasksCompleted: number;
  members: WorkerPoolMember[];
  maxTasksPerMember: number;
  idleTtlSeconds: number;
  rssLimitMb: number;
  platformDefaultSize: number;
  envOverride: boolean;
  persisted: boolean;
}

interface PyWorkerPoolMember {
  pid: number;
  state: "idle" | "busy";
  tasks_served: number;
  idle_seconds: number;
  rss_mb: number | null;
}

interface PyWorkerPoolState {
  enabled: boolean;
  size: number;
  idle: number;
  busy: number;
  active_tasks: number;
  tasks_completed: number;
  members: PyWorkerPoolMember[];
  max_tasks_per_member: number;
  idle_ttl_seconds: number;
  rss_limit_mb: number;
  platform_default_size: number;
  env_override: boolean;
  persisted: boolean;
}

const fromPyMember = (raw: PyWorkerPoolMember): WorkerPoolMember => ({
  pid: raw.pid,
  state: raw.state,
  tasksServed: raw.tasks_served,
  idleSeconds: raw.idle_seconds,
  rssMb: raw.rss_mb,
});

const fromPy = (raw: PyWorkerPoolState): WorkerPoolState => ({
  enabled: raw.enabled,
  size: raw.size,
  idle: raw.idle,
  busy: raw.busy,
  activeTasks: raw.active_tasks,
  tasksCompleted: raw.tasks_completed,
  members: raw.members.map(fromPyMember),
  maxTasksPerMember: raw.max_tasks_per_member,
  idleTtlSeconds: raw.idle_ttl_seconds,
  rssLimitMb: raw.rss_limit_mb,
  platformDefaultSize: raw.platform_default_size,
  envOverride: raw.env_override,
  persisted: raw.persisted,
});

export async function getWorkerPool(): Promise<WorkerPoolState> {
  const response = await axios.get<PyWorkerPoolState>(WORKER_POOL_URL);
  return fromPy(response.data);
}

export async function setWorkerPool(size: number): Promise<WorkerPoolState> {
  const response = await axios.post<PyWorkerPoolState>(WORKER_POOL_URL, { size });
  return fromPy(response.data);
}
