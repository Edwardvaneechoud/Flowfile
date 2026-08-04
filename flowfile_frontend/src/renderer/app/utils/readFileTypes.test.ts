import { describe, it, expect } from "vitest";
import {
  READ_EXTENSION_MAP,
  READ_EXTENSIONS,
  baseNameOf,
  buildReceivedTable,
  createDefaultSettings,
  detectFileType,
  extensionOf,
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
});

describe("extensionOf", () => {
  it("lowercases and drops the dot", () => {
    expect(extensionOf("/data/SALES.CSV")).toBe("csv");
  });

  it("returns null when the basename has no extension", () => {
    expect(extensionOf("/data/README")).toBeNull();
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
