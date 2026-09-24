import type { FullCloudStorageConnectionInterface } from "../../../views/CloudConnectionView/CloudConnectionTypes";

// Native <select>/<option> values are strings, so options key off connectionName:
// binding the connection object makes every option stringify to "[object Object]".
export const NO_CONNECTION_VALUE = "";

export function connectionSelectValue(
  connection: FullCloudStorageConnectionInterface | null | undefined,
  unavailableConnection?: string | null,
): string {
  return connection?.connectionName || unavailableConnection || NO_CONNECTION_VALUE;
}

export function resolveConnection(
  connections: FullCloudStorageConnectionInterface[],
  value: string,
): FullCloudStorageConnectionInterface | null {
  if (value === NO_CONNECTION_VALUE) return null;
  // First match wins, mirroring core's own-shadows-shared connection resolution.
  return connections.find((conn) => conn.connectionName === value) ?? null;
}

// A saved connection_name the current list can't resolve — deleted, share revoked,
// or the fetch failed. Surfaced so the picker never silently reads as "no connection".
export function unavailableConnectionName(
  connections: FullCloudStorageConnectionInterface[],
  savedName: string | null | undefined,
): string | null {
  if (!savedName) return null;
  return resolveConnection(connections, savedName) ? null : savedName;
}

export interface NoConnectionChoice {
  label: string;
  warning: string;
  disabled: boolean;
}

/** "No connection" uses the server's own cloud credentials, which multi-user servers refuse. */
export function ambientCredentialsChoice(multiUser: boolean): NoConnectionChoice {
  if (multiUser) {
    return {
      label: "No connection (not available on this server)",
      warning: "This server does not use its own cloud credentials. Select a cloud connection.",
      disabled: true,
    };
  }
  return {
    label: "No connection (this machine's credentials)",
    warning: "Uses the cloud credentials of the machine running Flowfile, not a saved connection.",
    disabled: false,
  };
}
