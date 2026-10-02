import { describe, expect, it } from "vitest";

import { cleanFolders, folderEntry, folderPath, isWritable } from "./kernelFolders";

describe("folderPath", () => {
  it("reads a plain string", () => {
    expect(folderPath("/data")).toBe("/data");
  });

  it("reads an object's path", () => {
    expect(folderPath({ path: "/data", writable: true })).toBe("/data");
  });
});

describe("isWritable", () => {
  it("a plain string is read-only", () => {
    expect(isWritable("/data")).toBe(false);
  });

  it("an object follows its flag", () => {
    expect(isWritable({ path: "/data", writable: true })).toBe(true);
    expect(isWritable({ path: "/data", writable: false })).toBe(false);
  });
});

describe("folderEntry", () => {
  it("keeps a read-only folder a plain string", () => {
    expect(folderEntry("/data", false)).toBe("/data");
  });

  it("makes a writable folder an object", () => {
    expect(folderEntry("/data", true)).toEqual({ path: "/data", writable: true });
  });
});

describe("cleanFolders", () => {
  it("trims paths and drops empty rows of either shape", () => {
    expect(
      cleanFolders([
        "  /a  ",
        "",
        "   ",
        { path: " ", writable: true },
        { path: " /b ", writable: true },
      ]),
    ).toEqual(["/a", { path: "/b", writable: true }]);
  });

  it("turns a non-writable object back into a plain string", () => {
    expect(cleanFolders([{ path: "C:\\data", writable: false }])).toEqual(["C:\\data"]);
  });

  it("keeps order across mixed shapes", () => {
    expect(cleanFolders(["/a", { path: "/b", writable: true }, "/c"])).toEqual([
      "/a",
      { path: "/b", writable: true },
      "/c",
    ]);
  });

  it("returns an empty list for no folders", () => {
    expect(cleanFolders([])).toEqual([]);
  });
});
