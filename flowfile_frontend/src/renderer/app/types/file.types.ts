// File system related TypeScript interfaces and types

import type { CloudStorageKind } from "../utils/storagePath";

export type { CloudStorageKind };

// File Info Types

export interface FileInfo {
  name: string;
  path: string;
  is_directory: boolean;
  size: number;
  file_type: string;
  /** Null for object-store prefixes, which carry no modification time. */
  last_modified: Date | null;
  /** Null for every object store — they have no creation time to report. */
  created_date: Date | null;
  is_hidden: boolean;
  exists?: boolean;
  entry_kind?: "container" | "prefix" | "object";
}

// Directory Contents Params

export interface DirectoryContentsParams {
  file_types?: string[];
  include_hidden?: boolean;
}

/** Which storage the browser is pointed at. Omit for the local filesystem. */
export type StorageLocation =
  | { kind: "local" }
  | { kind: "cloud"; storageType: CloudStorageKind; connectionName?: string | null };

/** One page of a listing, in the shape the browser renders. */
export interface StorageListing {
  entries: FileInfo[];
  path: string;
  parentPath: string | null;
  isRoot: boolean;
  truncated: boolean;
  nextToken: string | null;
  /** The credentials cannot enumerate buckets; the UI should ask for one instead. */
  rootListingDenied: boolean;
  currentIsDeltaTable: boolean;
}
