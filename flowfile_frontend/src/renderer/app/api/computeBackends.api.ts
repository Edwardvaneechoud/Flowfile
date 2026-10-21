// The admin /compute_backends routes; the token and the CA certificate go in the add body once and are never answered.

import axios from "../services/axios.config";

const COMPUTE_BACKENDS_URL = "/compute_backends";

export interface ComputeBackend {
  id: string;
  url: string;
}

export interface ComputeBackendInput {
  name: string;
  url: string;
  token: string;
  /** PEM of the CA to trust for an https worker whose certificate is not public. */
  ca_cert?: string;
}

export interface WorkerCapabilities {
  app_version: string;
  polars_version: string;
  python_version: string;
  protocol_version: number;
  platform: string;
  plugins: Record<string, string>;
  plugin_libs: Record<string, string>;
  operations: string[];
  read_schemes: string[];
  write_schemes: string[];
  credential_resolution: string;
}

/** `worker` is null and `error` says why when the worker's capabilities could not be read. */
export interface HandshakeVerdict {
  id: string;
  ok: boolean;
  core: WorkerCapabilities;
  worker: WorkerCapabilities | null;
  differences: string[];
  error: string | null;
}

export async function listComputeBackends(): Promise<ComputeBackend[]> {
  const response = await axios.get<ComputeBackend[]>(COMPUTE_BACKENDS_URL);
  return response.data;
}

export async function addComputeBackend(input: ComputeBackendInput): Promise<ComputeBackend> {
  const response = await axios.post<ComputeBackend>(COMPUTE_BACKENDS_URL, input);
  return response.data;
}

export async function removeComputeBackend(id: string): Promise<void> {
  await axios.delete(`${COMPUTE_BACKENDS_URL}/${encodeURIComponent(id)}`);
}

export async function handshakeComputeBackend(id: string): Promise<HandshakeVerdict> {
  const response = await axios.post<HandshakeVerdict>(
    `${COMPUTE_BACKENDS_URL}/${encodeURIComponent(id)}/handshake`,
  );
  return response.data;
}
