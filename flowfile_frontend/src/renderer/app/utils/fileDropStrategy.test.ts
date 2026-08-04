import { describe, it, expect } from "vitest";
import {
  pairPaths,
  parseFileUriList,
  resolveDropStrategy,
  shouldBlockStrayDrop,
  summarizeDragTypes,
} from "./fileDropStrategy";

describe("parseFileUriList", () => {
  it("parses a single posix uri", () => {
    expect(parseFileUriList("file:///Users/me/sales.csv")).toEqual(["/Users/me/sales.csv"]);
  });

  it("parses CRLF-separated uris", () => {
    expect(parseFileUriList("file:///a/x.csv\r\nfile:///a/y.csv\r\n")).toEqual([
      "/a/x.csv",
      "/a/y.csv",
    ]);
  });

  it("skips comment lines", () => {
    expect(parseFileUriList("# comment\nfile:///a/x.csv")).toEqual(["/a/x.csv"]);
  });

  it("percent-decodes", () => {
    expect(parseFileUriList("file:///a/sales%20q1.csv")).toEqual(["/a/sales q1.csv"]);
  });

  it("ignores non-file schemes", () => {
    expect(parseFileUriList("https://example.com/x.csv")).toEqual([]);
  });

  it("strips the empty authority off a windows drive path", () => {
    expect(parseFileUriList("file:///C:/data/x.csv")).toEqual(["C:/data/x.csv"]);
  });

  it("keeps a UNC host as a double-slash prefix", () => {
    expect(parseFileUriList("file://host/share/x.csv")).toEqual(["//host/share/x.csv"]);
  });

  it("returns an empty list for empty input", () => {
    expect(parseFileUriList("")).toEqual([]);
  });
});

describe("resolveDropStrategy", () => {
  it("returns none without files", () => {
    expect(resolveDropStrategy({ isDesktop: true, uriList: "", fileNames: [] })).toEqual({
      kind: "none",
    });
  });

  it("uploads in web mode even when a uri-list is present", () => {
    expect(
      resolveDropStrategy({
        isDesktop: false,
        uriList: "file:///a/x.csv",
        fileNames: ["x.csv"],
      }),
    ).toEqual({ kind: "upload" });
  });

  it("links on desktop when every basename pairs up", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "file:///a/x.csv\nfile:///a/y.csv",
        fileNames: ["x.csv", "y.csv"],
      }),
    ).toEqual({ kind: "link", paths: ["/a/x.csv", "/a/y.csv"] });
  });

  it("uploads on desktop when the webview exposes no uri-list", () => {
    expect(resolveDropStrategy({ isDesktop: true, uriList: "", fileNames: ["x.csv"] })).toEqual({
      kind: "upload",
    });
  });

  it("uploads when the path count does not match the file count", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "file:///a/x.csv",
        fileNames: ["x.csv", "y.csv"],
      }),
    ).toEqual({ kind: "upload" });
  });

  it("uploads rather than guessing when basenames disagree", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "file:///a/x.csv\nfile:///a/z.csv",
        fileNames: ["x.csv", "y.csv"],
      }),
    ).toEqual({ kind: "upload" });
  });

  it("links from nativePaths when the webview exposes no uri-list", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "",
        fileNames: ["x.csv", "y.csv"],
        nativePaths: ["/a/x.csv", "/a/y.csv"],
      }),
    ).toEqual({ kind: "link", paths: ["/a/x.csv", "/a/y.csv"] });
  });

  it("prefers the uri-list when both sources pair up", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "file:///uri/x.csv",
        fileNames: ["x.csv"],
        nativePaths: ["/native/x.csv"],
      }),
    ).toEqual({ kind: "link", paths: ["/uri/x.csv"] });
  });

  it("reorders nativePaths to the dropped file order", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "",
        fileNames: ["y.csv", "x.csv"],
        nativePaths: ["/a/x.csv", "/a/y.csv"],
      }),
    ).toEqual({ kind: "link", paths: ["/a/y.csv", "/a/x.csv"] });
  });

  it("uploads when nativePaths repeat a basename", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "",
        fileNames: ["x.csv", "x.csv"],
        nativePaths: ["/a/x.csv", "/b/x.csv"],
      }),
    ).toEqual({ kind: "upload" });
  });

  it("uploads when the nativePaths count does not match the file count", () => {
    expect(
      resolveDropStrategy({
        isDesktop: true,
        uriList: "",
        fileNames: ["x.csv", "y.csv"],
        nativePaths: ["/a/x.csv"],
      }),
    ).toEqual({ kind: "upload" });
  });
});

describe("pairPaths", () => {
  it("keeps an already-aligned list as-is", () => {
    expect(pairPaths(["/a/x.csv", "/a/y.csv"], ["x.csv", "y.csv"])).toEqual([
      "/a/x.csv",
      "/a/y.csv",
    ]);
  });

  it("returns null when a name has no matching path", () => {
    expect(pairPaths(["/a/x.csv", "/a/z.csv"], ["x.csv", "y.csv"])).toBeNull();
  });

  it("returns null on empty input", () => {
    expect(pairPaths([], [])).toBeNull();
  });

  it("refuses duplicate dropped names even when path basenames are unique", () => {
    expect(pairPaths(["/x/a.csv", "/x/b.csv"], ["a.csv", "a.csv"])).toBeNull();
  });

  it("reorders by basename when both sides are unique", () => {
    expect(pairPaths(["/a/y.csv", "/a/x.csv"], ["x.csv", "y.csv"])).toEqual([
      "/a/x.csv",
      "/a/y.csv",
    ]);
  });
});

describe("summarizeDragTypes", () => {
  it("labels recognized mime types and dedupes", () => {
    const summary = summarizeDragTypes([
      "text/csv",
      "text/csv",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ]);
    expect(summary.fileCount).toBe(3);
    expect(summary.typeLabels).toEqual(["CSV", "Excel"]);
    expect(summary.allUnknown).toBe(false);
  });

  it("reports allUnknown when nothing is recognized", () => {
    const summary = summarizeDragTypes(["", "application/octet-stream"]);
    expect(summary.fileCount).toBe(2);
    expect(summary.typeLabels).toEqual([]);
    expect(summary.allUnknown).toBe(true);
  });
});

describe("shouldBlockStrayDrop", () => {
  it("blocks only drags carrying OS files", () => {
    expect(shouldBlockStrayDrop(["Files"])).toBe(true);
    expect(shouldBlockStrayDrop(["text/plain"])).toBe(false);
    expect(shouldBlockStrayDrop([])).toBe(false);
  });
});
