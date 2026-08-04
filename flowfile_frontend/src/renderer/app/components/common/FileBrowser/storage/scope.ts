import type { StorageLocation } from "../../../../types";

/**
 * Stable identity for a location, used to scope remembered paths.
 *
 * Kept free of axios/provider imports so it stays unit-testable in the node env.
 */
export function storageScopeKey(location: StorageLocation): string {
  if (location.kind === "local") return "local";
  return `cloud:${location.storageType}:${location.connectionName || "~ambient"}`;
}
