// The /compute_backends routes: every user lists them, an administrator adds, removes and handshakes.

import axios from "../services/axios.config";

const COMPUTE_BACKENDS_URL = "/compute_backends";

/**
 * A configured worker as every user lists it: `url` is null for a non-administrator, `verdict` until it was
 * checked. `shares_core_disk`: core offers the worker local files whatever it declares.
 */
export interface ComputeBackend {
  id: string;
  url: string | null;
  shares_core_disk: boolean;
  verdict: HandshakeVerdict | null;
}

/** The add body, an administrator's; the token and CA certificate are sent once and never answered. */
export interface ComputeBackendInput {
  name: string;
  url: string;
  token: string;
  /** PEM of the CA to trust for an https worker whose certificate is not public. */
  ca_cert?: string;
  /** The worker sees this server's files: the same host or a shared mount. */
  shares_core_disk?: boolean;
}

export interface WorkerCapabilities {
  app_version: string;
  polars_version: string;
  python_version: string;
  protocol_version: number;
  platform: string;
  polars_runtime: string;
  plugins: Record<string, string>;
  plugin_libs: Record<string, string>;
  operations: string[];
  read_schemes: string[];
  write_schemes: string[];
  credential_resolution: string;
}

/**
 * `ok` false with `error` is a backend refused whole; `backend` is null when its capabilities could not be read.
 * `absent` names what an `ok` backend lacks and why ("python": core's Python functions); `offered` is `backend`
 * with only the plugins it offers.
 */
export interface HandshakeVerdict {
  id: string;
  ok: boolean;
  core: WorkerCapabilities;
  backend: WorkerCapabilities | null;
  offered: WorkerCapabilities | null;
  differences: string[];
  absent: Record<string, string>;
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
