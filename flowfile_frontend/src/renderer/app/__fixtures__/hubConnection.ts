import type { HubConnection } from "../api/hubConnection.api";

/** A desktop with no hub: not connected, and the name a new connection would register. */
export const none: HubConnection = {
  connected: false,
  hubUrl: null,
  deviceName: "ana-laptop",
  expiresAt: null,
  orgName: null,
  hubVersion: null,
  reachable: false,
  error: null,
  account: null,
  joined: false,
  organisationStorage: null,
};

/** A desktop signed in to its hub. */
export const desktop: HubConnection = {
  ...none,
  connected: true,
  hubUrl: "https://hub.acme.test:8443",
  expiresAt: "2026-11-08T12:00:00Z",
  orgName: "Acme Data",
  hubVersion: "0.3.0",
  reachable: true,
  account: "Ana Lima",
};

/** A server that joined its hub. */
export const server: HubConnection = { ...desktop, joined: true };
