import { describe, it, expect } from "vitest";

import {
  browseFileTypesForFormat,
  extensionForFormat,
  formatMismatchWarning,
  scanModeForSelection,
  suggestedWriteFileName,
  targetsDirectory,
} from "./cloudPathMapping";

describe("browseFileTypesForFormat", () => {
  it("filters to the format's extensions", () => {
    expect(browseFileTypesForFormat("csv")).toEqual(["csv", "txt"]);
    expect(browseFileTypesForFormat("parquet")).toEqual(["parquet"]);
    expect(browseFileTypesForFormat("json")).toEqual(["json", "ndjson", "jsonl"]);
  });

  it.each(["delta", "iceberg", undefined, null])(
    "does not filter for the directory-shaped format %s",
    (format) => {
      // Filtering by extension would hide the table directory itself.
      expect(browseFileTypesForFormat(format)).toEqual([]);
    },
  );
});

describe("extensionForFormat", () => {
  it("maps known formats", () => {
    expect(extensionForFormat("csv")).toBe("csv");
    expect(extensionForFormat("json")).toBe("json");
    expect(extensionForFormat("parquet")).toBe("parquet");
  });

  it("defaults to parquet", () => {
    expect(extensionForFormat(undefined)).toBe("parquet");
    expect(extensionForFormat("delta")).toBe("parquet");
  });
});

describe("targetsDirectory", () => {
  it.each(["delta", "iceberg"])("is true for %s", (format) => {
    expect(targetsDirectory(format)).toBe(true);
  });

  it.each(["csv", "parquet", "json", undefined])("is false for %s", (format) => {
    expect(targetsDirectory(format)).toBe(false);
  });
});

describe("scanModeForSelection", () => {
  it("maps a prefix to a directory scan and an object to a single file", () => {
    expect(scanModeForSelection(true)).toBe("directory");
    expect(scanModeForSelection(false)).toBe("single_file");
  });
});

describe("suggestedWriteFileName", () => {
  it("reuses the current leaf when it looks like a file", () => {
    expect(suggestedWriteFileName("parquet", "s3://bucket/data/report.parquet")).toBe(
      "report.parquet",
    );
  });

  it("falls back to a default when the path is a prefix", () => {
    expect(suggestedWriteFileName("csv", "s3://bucket/data")).toBe("output.csv");
  });

  it("falls back when there is no path at all", () => {
    expect(suggestedWriteFileName("json", "")).toBe("output.json");
    expect(suggestedWriteFileName(undefined, null)).toBe("output.parquet");
  });
});

describe("formatMismatchWarning", () => {
  it("warns when the extension disagrees with the format", () => {
    const warning = formatMismatchWarning("parquet", "s3://bucket/a/data.csv");
    expect(warning).toContain(".csv");
    expect(warning).toContain("parquet");
  });

  it("stays quiet when they agree", () => {
    expect(formatMismatchWarning("parquet", "s3://bucket/a/data.parquet")).toBeNull();
    expect(formatMismatchWarning("csv", "s3://bucket/a/data.txt")).toBeNull();
  });

  it("stays quiet for directory-shaped formats", () => {
    expect(formatMismatchWarning("delta", "s3://bucket/table")).toBeNull();
  });

  it("stays quiet for a prefix with no extension", () => {
    expect(formatMismatchWarning("parquet", "s3://bucket/folder")).toBeNull();
  });

  it("stays quiet when inputs are missing", () => {
    expect(formatMismatchWarning(null, "s3://bucket/a.csv")).toBeNull();
    expect(formatMismatchWarning("csv", null)).toBeNull();
  });
});
