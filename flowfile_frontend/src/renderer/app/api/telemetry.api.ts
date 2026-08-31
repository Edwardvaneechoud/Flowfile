// Axios wrappers for the /telemetry endpoints (anonymous, opt-in usage
// telemetry). TS-side camelCase, snake_case mapper at the boundary. Paths match
// the FastAPI decorators exactly (no trailing slash) to avoid the 307 redirect
// trap.

import axios from "../services/axios.config";

const STATUS_URL = "/telemetry/status";
const CONSENT_URL = "/telemetry/consent";

export interface TelemetryStatus {
  available: boolean;
  consent: boolean | null;
  envKillSwitch: boolean;
  endpointConfigured: boolean;
  canManage: boolean;
}

interface PyTelemetryStatus {
  available: boolean;
  consent: boolean | null;
  env_kill_switch: boolean;
  endpoint_configured: boolean;
  can_manage: boolean;
}

const fromPy = (raw: PyTelemetryStatus): TelemetryStatus => ({
  available: raw.available,
  consent: raw.consent,
  envKillSwitch: raw.env_kill_switch,
  endpointConfigured: raw.endpoint_configured,
  canManage: raw.can_manage,
});

export async function getTelemetryStatus(): Promise<TelemetryStatus> {
  const response = await axios.get<PyTelemetryStatus>(STATUS_URL);
  return fromPy(response.data);
}

export async function setTelemetryConsent(enabled: boolean): Promise<TelemetryStatus> {
  const response = await axios.post<PyTelemetryStatus>(CONSENT_URL, { enabled });
  return fromPy(response.data);
}
