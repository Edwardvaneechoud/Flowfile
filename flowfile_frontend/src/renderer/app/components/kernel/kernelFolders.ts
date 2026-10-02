// Pure helpers for a kernel's mounted folders; no Vue/axios imports so they stay unit-testable.
import type { MountedFolderEntry } from "@/types";

export function folderPath(entry: MountedFolderEntry): string {
  return typeof entry === "string" ? entry : entry.path;
}

export function isWritable(entry: MountedFolderEntry): boolean {
  return typeof entry !== "string" && entry.writable === true;
}

/** The stored shape: a plain string for a read-only folder, an object only when writable. */
export function folderEntry(path: string, writable: boolean): MountedFolderEntry {
  return writable ? { path, writable: true } : path;
}

/** Trims every path, drops empty rows and normalises each entry's shape for the API. */
export function cleanFolders(entries: MountedFolderEntry[]): MountedFolderEntry[] {
  return entries
    .map((entry) => folderEntry(folderPath(entry).trim(), isWritable(entry)))
    .filter((entry) => folderPath(entry) !== "");
}
