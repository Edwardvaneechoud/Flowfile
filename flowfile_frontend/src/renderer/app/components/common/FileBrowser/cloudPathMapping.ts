// Pure mapping between a browser selection and cloud node settings.

import type { ScanMode } from "@/types";

import { basename } from "../../../utils/storagePath";

/**
 * Extensions worth showing for a format.
 *
 * Delta and Iceberg tables are directories, so filtering by extension would hide
 * exactly what the user came to pick.
 */
export function browseFileTypesForFormat(format?: string | null): string[] {
  switch (format) {
    case "csv":
      return ["csv", "txt"];
    case "parquet":
      return ["parquet"];
    case "json":
      return ["json", "ndjson", "jsonl"];
    default:
      return [];
  }
}

export function extensionForFormat(format?: string | null): string {
  switch (format) {
    case "csv":
      return "csv";
    case "json":
      return "json";
    default:
      return "parquet";
  }
}

/** True when the write target is the directory itself rather than a file in it. */
export function targetsDirectory(format?: string | null): boolean {
  return format === "delta" || format === "iceberg";
}

export function scanModeForSelection(isDirectory: boolean): ScanMode {
  return isDirectory ? "directory" : "single_file";
}

/** Seed for the writer's filename prompt: reuse the current leaf, else a default. */
export function suggestedWriteFileName(
  format?: string | null,
  currentPath?: string | null,
): string {
  const extension = extensionForFormat(format);
  const leaf = currentPath ? basename(currentPath) : "";
  if (leaf && leaf.includes(".")) return leaf;
  return `output.${extension}`;
}

/** Warn when a picked file's extension disagrees with the configured format. */
export function formatMismatchWarning(
  format: string | null | undefined,
  path: string | null | undefined,
): string | null {
  if (!format || !path || targetsDirectory(format)) return null;
  const leaf = basename(path);
  if (!leaf.includes(".")) return null;
  const extension = leaf.slice(leaf.lastIndexOf(".") + 1).toLowerCase();
  const expected = browseFileTypesForFormat(format);
  if (!expected.length || expected.includes(extension)) return null;
  return `Selected file is .${extension} but the file format is set to ${format}.`;
}
