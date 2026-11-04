// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";
import type { ComputeBackend } from "./computeBackends.api";

const CONNECTION_URL = "/hub_client/connection";
const COMPUTE_NODES_SYNC_URL = "/hub_client/compute_nodes/sync";
const COMPUTE_NODES_CHECK_URL = "/hub_client/compute_nodes/check";

export interface HubConnection {
  connected: boolean;
  hubUrl: string | null;
  deviceId: number | null;
  /** The connected device's name; when not connected, the name a new connection would register. */
  deviceName: string | null;
  /** When the stored hub session runs out and the hub has to be connected again (ISO 8601). */
  expiresAt: string | null;
  clientVersion: string;
  contractVersion: string;
  orgName: string | null;
  hubVersion: string | null;
  coreVersion: string | null;
  vendMode: string | null;
  reachable: boolean;
  error: string | null;
  /** After a connect: how many of the hub's compute nodes now fill the backends table; null otherwise. */
  nodesSynced: number | null;
  /** After a connect whose node sync failed: why. The connection itself stands. */
  syncError: string | null;
  /** This core's own name as a node on the hub, connected or not; a sync never lists that node as a backend. */
  nodeName: string | null;
  /** Who is signed in at the hub through this connection (full name, else username); null when not known. */
  account: string | null;
  /** This server joined its hub with keys of its own: it belongs to that hub, named by `hubUrl`. */
  joined: boolean;
  /** On a joined server, the organisation's storage root as the hub shows it; null when not known. */
  organisationStorage: string | null;
}

/** What a node sync left behind: every configured backend, and the hub nodes skipped for a hand-added row. */
export interface HubComputeNodesSync {
  backends: ComputeBackend[];
  skipped: string[];
}

export interface HubConnectInput {
  hubUrl: string;
  username: string;
  password: string;
  deviceName?: string;
}

interface PyHubConnection {
  connected: boolean;
  hub_url: string | null;
  device_id: number | null;
  device_name: string | null;
  expires_at: string | null;
  client_version: string;
  contract_version: string;
  org_name: string | null;
  hub_version: string | null;
  core_version: string | null;
  vend_mode: string | null;
  reachable: boolean;
  error: string | null;
  nodes_synced?: number | null;
  sync_error?: string | null;
  node_name?: string | null;
  account?: string | null;
  joined?: boolean;
  organisation_storage?: string | null;
}

const fromPy = (raw: PyHubConnection): HubConnection => ({
  connected: raw.connected,
  hubUrl: raw.hub_url,
  deviceId: raw.device_id,
  deviceName: raw.device_name,
  expiresAt: raw.expires_at,
  clientVersion: raw.client_version,
  contractVersion: raw.contract_version,
  orgName: raw.org_name,
  hubVersion: raw.hub_version,
  coreVersion: raw.core_version,
  vendMode: raw.vend_mode,
  reachable: raw.reachable,
  error: raw.error,
  nodesSynced: raw.nodes_synced ?? null,
  syncError: raw.sync_error ?? null,
  nodeName: raw.node_name ?? null,
  account: raw.account ?? null,
  joined: raw.joined ?? false,
  organisationStorage: raw.organisation_storage ?? null,
});

export async function getHubConnection(): Promise<HubConnection> {
  const response = await axios.get<PyHubConnection>(CONNECTION_URL);
  return fromPy(response.data);
}

/** Signs in to the hub once and registers this device; the password goes to the hub and is not kept. */
export async function connectHub(input: HubConnectInput): Promise<HubConnection> {
  const response = await axios.post<PyHubConnection>(CONNECTION_URL, {
    hub_url: input.hubUrl,
    username: input.username,
    password: input.password,
    device_name: input.deviceName || null,
  });
  return fromPy(response.data);
}

/** Forgets the hub session on this machine; the hub is not told. */
export async function disconnectHub(): Promise<void> {
  await axios.delete(CONNECTION_URL);
}

/** Reads the hub's compute nodes now and makes the backends table match them. */
export async function syncHubComputeNodes(): Promise<HubComputeNodesSync> {
  const response = await axios.post<HubComputeNodesSync>(COMPUTE_NODES_SYNC_URL);
  return response.data;
}

/** Checks now whether this computer reaches each of the hub's nodes; the hub rows with the outcome filled. */
export async function checkHubComputeNodes(): Promise<ComputeBackend[]> {
  const response = await axios.post<ComputeBackend[]>(COMPUTE_NODES_CHECK_URL);
  return response.data;
}
