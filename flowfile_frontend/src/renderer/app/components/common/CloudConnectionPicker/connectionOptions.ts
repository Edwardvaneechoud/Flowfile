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
