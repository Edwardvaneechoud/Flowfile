import type { StorageLocation } from "../../../../types";
import { createCloudProvider } from "./cloudProvider";
import { localStorageProvider } from "./localProvider";
import type { StorageProvider } from "./provider";

export type { StorageCapabilities, StorageListOptions, StorageProvider } from "./provider";
export { createCloudProvider } from "./cloudProvider";
export { localStorageProvider } from "./localProvider";
export { storageScopeKey } from "./scope";

export const LOCAL_LOCATION: StorageLocation = { kind: "local" };

export function resolveStorageProvider(location: StorageLocation): StorageProvider {
  if (location.kind === "local") return localStorageProvider;
  return createCloudProvider(location.storageType, location.connectionName);
}
