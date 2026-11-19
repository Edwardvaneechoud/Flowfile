// Paths match the FastAPI decorators exactly: a trailing slash would answer 307.

import axios from "../services/axios.config";

const CONNECTION_URL = "/hub_client/connection";

export interface HubConnection {
  connected: boolean;
  hub_url: string | null;
  /** The connected device's name; when not connected, the name a new connection would register. */
  device_name: string | null;
  /** When the stored hub session runs out and the hub has to be connected again (ISO 8601). */
  expires_at: string | null;
  org_name: string | null;
  hub_version: string | null;
  reachable: boolean;
  error: string | null;
  /** Who is signed in at the hub through this connection (full name, else username); null when not known. */
  account: string | null;
  /** This server joined its hub with keys of its own: it belongs to that hub, named by `hub_url`. */
  joined: boolean;
  /** On a joined server, the name its hub knows it by; null anywhere else. */
  server_name: string | null;
  /** On a joined server, the organisation's storage root as the hub shows it; null when not known. */
  organisation_storage: string | null;
  /** Whether this machine may sign in to a hub through the system browser (not on a multi-user server). */
  browser_sign_in?: boolean;
}

export interface HubConnectInput {
  hub_url: string;
  username: string;
  password: string;
  device_name?: string;
}

export interface HubBrowserSignInInput {
  hub_url: string;
  device_name?: string;
}

export interface HubBrowserSignIn {
  /** The sign-in's id, to poll its status with. */
  flow_id: string;
  /** The hub's sign-in page, opened in the system browser. */
  authorize_url: string;
}

export interface HubBrowserSignInStatus {
  status: "waiting" | "connected" | "failed";
  detail: string | null;
  connection: HubConnection | null;
}

/** The refusal `kind` core answers when this hub or this machine can only sign in with a username and password. */
export const PASSWORD_ONLY = "password_only";

export async function getHubConnection(): Promise<HubConnection> {
  const response = await axios.get<HubConnection>(CONNECTION_URL);
  return response.data;
}

/**
 * Signs in to the hub once and registers this device; the password goes to the hub and is not kept. Core then
 * fills the backends table from the hub's compute nodes and handshakes each once, behind its answer.
 */
export async function connectHub(input: HubConnectInput): Promise<HubConnection> {
  const response = await axios.post<HubConnection>(CONNECTION_URL, input);
  return response.data;
}

/** Forgets the hub session on this machine; a sign-in through the browser is also ended on the hub. */
export async function disconnectHub(): Promise<void> {
  await axios.delete(CONNECTION_URL);
}

/**
 * Starts a sign-in through the system browser: core opens a one-shot listener on this machine and answers the hub's
 * page to open. A hub or machine that cannot do it answers 400 with `detail.kind` `password_only`.
 */
export async function startBrowserSignIn(input: HubBrowserSignInInput): Promise<HubBrowserSignIn> {
  const response = await axios.post<HubBrowserSignIn>(`${CONNECTION_URL}/browser`, input);
  return response.data;
}

export async function getBrowserSignIn(flowId: string): Promise<HubBrowserSignInStatus> {
  const response = await axios.get<HubBrowserSignInStatus>(
    `${CONNECTION_URL}/browser/${encodeURIComponent(flowId)}`,
  );
  return response.data;
}

/** Stops waiting for a sign-in through the browser and closes its local port. */
export async function cancelBrowserSignIn(flowId: string): Promise<void> {
  await axios.delete(`${CONNECTION_URL}/browser/${encodeURIComponent(flowId)}`);
}
