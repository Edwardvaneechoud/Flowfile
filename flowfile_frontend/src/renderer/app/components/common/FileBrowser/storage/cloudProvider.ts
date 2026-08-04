import { browseCloudStorage } from "../../../../api/storageBrowser.api";
import { CANONICAL_SCHEME, isRootPath, joinPath, parentPath } from "../../../../utils/storagePath";
import type { CloudStorageKind } from "../../../../utils/storagePath";
import { CLOUD_CAPABILITIES, type StorageProvider } from "./provider";

/**
 * Object storage, proxied through core.
 *
 * Every request goes to flowfile_core, never to the provider directly: the desktop
 * CSP blocks external origins, and the credentials live server-side by design.
 */
export function createCloudProvider(
  storageType: CloudStorageKind,
  connectionName?: string | null,
): StorageProvider {
  return {
    kind: "cloud",
    scope: `cloud:${storageType}:${connectionName || "~ambient"}`,
    capabilities: CLOUD_CAPABILITIES,

    async list(path, options) {
      const response = await browseCloudStorage({
        path,
        connectionName,
        storageType,
        pageToken: options.pageToken,
      });
      return {
        entries: response.entries,
        path: response.path,
        parentPath: response.parent_path,
        isRoot: response.is_root,
        truncated: response.truncated,
        nextToken: response.next_token,
        rootListingDenied: response.root_listing_denied,
        currentIsDeltaTable: response.current_is_delta_table,
      };
    },

    defaultPath: async () => CANONICAL_SCHEME[storageType],
    parent: parentPath,
    join: joinPath,
    isRoot: isRootPath,
  };
}
