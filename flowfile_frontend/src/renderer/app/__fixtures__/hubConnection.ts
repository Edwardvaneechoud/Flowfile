import type { HubConnection } from "../api/hubConnection.api";

/** A desktop with no hub: not connected, and the name a new connection would register. */
export const none: HubConnection = {
  connected: false,
  hub_url: null,
  device_name: "ana-laptop",
  expires_at: null,
  org_name: null,
  hub_version: null,
  reachable: false,
  error: null,
  account: null,
  joined: false,
  organisation_storage: null,
};

/** A desktop signed in to its hub. */
export const desktop: HubConnection = {
  ...none,
  connected: true,
  hub_url: "https://hub.acme.test:8443",
  expires_at: "2026-11-08T12:00:00Z",
  org_name: "Acme Data",
  hub_version: "0.3.0",
  reachable: true,
  account: "Ana Lima",
};

/** A server that joined its hub. */
export const server: HubConnection = { ...desktop, joined: true };
