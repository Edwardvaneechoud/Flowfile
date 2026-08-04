import { getDefaultPath, getDirectoryContents } from "../fileSystemApi";
import { isRootPath, joinPath, parentPath } from "../../../../utils/storagePath";
import { LOCAL_CAPABILITIES, type StorageProvider } from "./provider";

/** The local filesystem, over the existing /files/* endpoints. Behaviour is unchanged. */
export const localStorageProvider: StorageProvider = {
  kind: "local",
  scope: "local",
  capabilities: LOCAL_CAPABILITIES,

  async list(path, options) {
    const entries = await getDirectoryContents(path, { include_hidden: options.includeHidden });
    return {
      entries,
      path,
      parentPath: parentPath(path),
      isRoot: isRootPath(path),
      truncated: false,
      nextToken: null,
      rootListingDenied: false,
      currentIsDeltaTable: false,
    };
  },

  defaultPath: () => getDefaultPath(),
  parent: parentPath,
  join: joinPath,
  isRoot: isRootPath,
};
