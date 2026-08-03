import type { StorageListing } from "../../../../types";

/** What a storage backend can meaningfully offer, so the toolbar can hide the rest. */
export interface StorageCapabilities {
  /** Object stores have no hidden-file concept, so the toggle is pointless there. */
  hiddenFiles: boolean;
  /** Only a filesystem reports a creation time; sorting by it elsewhere is noise. */
  createdDate: boolean;
  createFile: boolean;
}

export const LOCAL_CAPABILITIES: StorageCapabilities = {
  hiddenFiles: true,
  createdDate: true,
  createFile: true,
};

/**
 * Object stores report no hidden flag and no creation time, but must still allow
 * naming a new object — that is how the writer targets a path that does not exist yet.
 */
export const CLOUD_CAPABILITIES: StorageCapabilities = {
  hiddenFiles: false,
  createdDate: false,
  createFile: true,
};

export interface StorageListOptions {
  includeHidden: boolean;
  pageToken?: string | null;
}

export interface StorageProvider {
  kind: "local" | "cloud";
  /** Stable key used to scope remembered paths. Changing storage invalidates them. */
  scope: string;
  capabilities: StorageCapabilities;
  list(path: string, options: StorageListOptions): Promise<StorageListing>;
  defaultPath(): Promise<string>;
  parent(path: string): string;
  join(path: string, child: string): string;
  isRoot(path: string): boolean;
}
