import { describe, it, expect } from "vitest";
import {
  DIRECTORY_CAPABLE_TYPES,
  READ_EXTENSION_MAP,
  READ_EXTENSIONS,
  baseNameOf,
  buildReceivedTable,
  createDefaultSettings,
  detectFileType,
  extensionOf,
  inferScanModeFromPath,
  isDirectoryCapable,
  type ReadFileType,
} from "./readFileTypes";

describe("detectFileType", () => {
  it("maps every supported extension", () => {
    for (const ext of READ_EXTENSIONS) {
      expect(detectFileType(`/data/file.${ext}`)).toBe(READ_EXTENSION_MAP[ext]);
    }
  });

  it("is case insensitive", () => {
    expect(detectFileType("SALES.CSV")).toBe("csv");
  });

  it("strips ${...} parameter references before reading the extension", () => {
    expect(detectFileType("${data_dir}/x.parquet")).toBe("parquet");
  });

  it("detects the type when a ${...} parameter is the whole stem", () => {
    expect(detectFileType("${date}.csv")).toBe("csv");
    expect(detectFileType("/data/${date}.parquet")).toBe("parquet");
    expect(detectFileType("C:\\data\\${run_id}.xlsx")).toBe("excel");
  });

  it("returns null when the extension itself is a parameter", () => {
    expect(detectFileType("file.${ext}")).toBeNull();
  });

  it("returns null for a dotfile", () => {
    expect(detectFileType("/data/.csv")).toBeNull();
  });

  it("returns null for an extensionless basename", () => {
    expect(detectFileType("/data/README")).toBeNull();
  });

  it("returns null for an unsupported extension", () => {
    expect(detectFileType("archive.tar.gz")).toBeNull();
  });

  it("never maps .json — the engine has no json handler", () => {
    expect(detectFileType("x.json")).toBeNull();
  });

  it("reads the extension off a Windows path", () => {
    expect(detectFileType("C:\\Users\\me\\book.xlsx")).toBe("excel");
  });

  it("reads the extension off a glob pattern", () => {
    expect(detectFileType("data/**/*.parquet")).toBe("parquet");
  });
});

describe("isDirectoryCapable", () => {
  const cases: [ReadFileType, boolean][] = [
    ["csv", true],
    ["parquet", true],
    ["ipc", true],
    ["excel", false],
    ["ndjson", false],
    ["avro", false],
  ];

  for (const [fileType, expected] of cases) {
    it(`${expected ? "accepts" : "rejects"} ${fileType}`, () => {
      expect(isDirectoryCapable(fileType)).toBe(expected);
      expect(DIRECTORY_CAPABLE_TYPES.has(fileType)).toBe(expected);
    });
  }

  it("mirrors shared/path_utils.py exactly", () => {
    expect([...DIRECTORY_CAPABLE_TYPES].sort()).toEqual(["csv", "ipc", "parquet"]);
  });

  it("rejects an absent or unknown type", () => {
    expect(isDirectoryCapable(undefined)).toBe(false);
    expect(isDirectoryCapable(null)).toBe(false);
    expect(isDirectoryCapable("json")).toBe(false);
  });
});

describe("inferScanModeFromPath", () => {
  const cases: [string, string][] = [
    ["/data/sales.csv", "single_file"],
    ["C:\\data\\sales.csv", "single_file"],
    ["/data/**/*.csv", "directory"],
    ["/data/*/sales.csv", "directory"],
    ["/data/sales_?.csv", "directory"],
    // "[" is legal in real names and cannot be stat'ed away here, so it never auto-promotes.
    ["/data/sales_[0-9].csv", "single_file"],
    ["/data/[archive]/sales.csv", "single_file"],
    ["/data/sales/", "directory"],
    ["C:\\data\\sales\\", "directory"],
    ["/data/sales", "single_file"],
    ["${data_dir}.csv", "single_file"],
    ["${data_dir}/*.csv", "directory"],
    ["${data_dir}/sales.csv", "single_file"],
  ];

  for (const [path, expected] of cases) {
    it(`reads ${path} as ${expected}`, () => {
      expect(inferScanModeFromPath(path)).toBe(expected);
    });
  }
});

describe("extensionOf", () => {
  it("lowercases and drops the dot", () => {
    expect(extensionOf("/data/SALES.CSV")).toBe("csv");
  });

  it("returns null when the basename has no extension", () => {
    expect(extensionOf("/data/README")).toBeNull();
  });

  it("returns null for a dotfile", () => {
    expect(extensionOf(".env")).toBeNull();
  });
});

describe("baseNameOf", () => {
  it("splits on both separators", () => {
    expect(baseNameOf("/data/sales.csv")).toBe("sales.csv");
    expect(baseNameOf("C:\\data\\sales.csv")).toBe("sales.csv");
  });
});

describe("createDefaultSettings", () => {
  it("uses a tab delimiter for tsv", () => {
    const settings = createDefaultSettings("csv", "tsv");
    expect(settings).toHaveProperty("delimiter", "\t");
  });

  it("uses a comma for plain csv", () => {
    expect(createDefaultSettings("csv")).toHaveProperty("delimiter", ",");
  });

  it("emits the excel file_type literal, never xlsx", () => {
    expect(createDefaultSettings("excel").file_type).toBe("excel");
  });

  it("emits bare settings for the container formats", () => {
    expect(createDefaultSettings("ipc")).toEqual({ file_type: "ipc" });
    expect(createDefaultSettings("ndjson")).toEqual({ file_type: "ndjson" });
    expect(createDefaultSettings("avro")).toEqual({ file_type: "avro" });
  });
});

describe("buildReceivedTable", () => {
  it("keeps the posix basename and absolute path", () => {
    const path = "/data/sales.csv";
    const table = buildReceivedTable({ name: baseNameOf(path), path, fileType: "csv" });
    expect(table).toMatchObject({ name: "sales.csv", path, file_type: "csv" });
    expect(table.table_settings).toHaveProperty("delimiter", ",");
  });

  it("keeps the windows basename and absolute path", () => {
    const path = "C:\\data\\book.xlsx";
    const table = buildReceivedTable({ name: baseNameOf(path), path, fileType: "excel" });
    expect(table).toMatchObject({ name: "book.xlsx", path, file_type: "excel" });
  });

  it("defaults to a single-file scan with no source-path column", () => {
    const table = buildReceivedTable({
      name: "sales.csv",
      path: "/data/sales.csv",
      fileType: "csv",
    });
    expect(table.scan_mode).toBe("single_file");
    expect(table.include_file_paths).toBeNull();
  });

  it("forwards the extension so a tsv gets a tab delimiter", () => {
    const table = buildReceivedTable({
      name: "sales.tsv",
      path: "/data/sales.tsv",
      fileType: "csv",
      ext: "tsv",
    });
    expect(table.table_settings).toHaveProperty("delimiter", "\t");
  });
});
