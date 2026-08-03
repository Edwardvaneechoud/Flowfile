import { describe, it, expect } from "vitest";

import {
  CANONICAL_SCHEME,
  CLOUD_URI_SCHEMES,
  basename,
  buildUri,
  dirname,
  isCloudUri,
  isRootPath,
  joinPath,
  normalizePath,
  parentPath,
  parseUri,
  schemeOf,
  storageTypeForUri,
} from "./storagePath";

describe("isCloudUri / schemeOf", () => {
  it.each([...CLOUD_URI_SCHEMES])("recognises %s", (scheme) => {
    expect(isCloudUri(`${scheme}container/key`)).toBe(true);
    expect(schemeOf(`${scheme}container/key`)).toBe(scheme);
  });

  it.each(["/tmp/data", "C:\\data", "~/files", "data/file.csv", "", "s3:/bucket"])(
    "rejects the local path %s",
    (value) => {
      expect(isCloudUri(value)).toBe(false);
      expect(schemeOf(value)).toBe("");
    },
  );

  it("is case insensitive on the scheme", () => {
    expect(storageTypeForUri("S3://bucket")).toBe("s3");
  });
});

describe("storageTypeForUri", () => {
  it.each([
    ["s3://b", "s3"],
    ["s3a://b", "s3"],
    ["az://c", "adls"],
    ["abfs://c", "adls"],
    ["abfss://c", "adls"],
    ["adl://c", "adls"],
    ["gs://b", "gcs"],
    ["gcs://b", "gcs"],
  ])("maps %s to %s", (uri, expected) => {
    expect(storageTypeForUri(uri)).toBe(expected);
  });

  it("returns null for local paths", () => {
    expect(storageTypeForUri("/local/path")).toBeNull();
    expect(storageTypeForUri("C:\\x")).toBeNull();
  });

  it("every canonical scheme is a known scheme", () => {
    Object.values(CANONICAL_SCHEME).forEach((scheme) => {
      expect(CLOUD_URI_SCHEMES).toContain(scheme as (typeof CLOUD_URI_SCHEMES)[number]);
    });
  });
});

describe("parseUri / buildUri", () => {
  it("splits a full uri", () => {
    expect(parseUri("s3://bucket/a/b.csv")).toEqual({
      scheme: "s3://",
      container: "bucket",
      key: "a/b.csv",
    });
  });

  it("treats a trailing slash as insignificant", () => {
    expect(parseUri("s3://bucket/a/")).toEqual(parseUri("s3://bucket/a"));
  });

  it("treats the scheme alone as the container list", () => {
    expect(parseUri("s3://")).toEqual({ scheme: "s3://", container: "", key: "" });
  });

  it("round-trips through buildUri", () => {
    ["s3://", "s3://bucket", "s3://bucket/a/b.csv", "az://container/p"].forEach((uri) => {
      const { scheme, container, key } = parseUri(uri);
      expect(buildUri(scheme, container, key)).toBe(uri);
    });
  });

  it("throws on a local path", () => {
    expect(() => parseUri("/tmp/x")).toThrow(/Not a cloud storage URI/);
  });
});

describe("normalizePath", () => {
  it("preserves the scheme separator", () => {
    expect(normalizePath("s3://bucket/x")).toBe("s3://bucket/x");
  });

  it("never collapses :// to :/ (the path-browserify trap)", () => {
    expect(normalizePath("s3://bucket/x")).not.toBe("s3:/bucket/x");
  });

  it("collapses duplicate slashes inside the key only", () => {
    expect(normalizePath("s3://bucket//a///b")).toBe("s3://bucket/a/b");
  });

  it("resolves . and .. inside the key", () => {
    expect(normalizePath("s3://bucket/a/./b/../c")).toBe("s3://bucket/a/c");
  });

  it("strips a trailing slash except at a root", () => {
    expect(normalizePath("s3://bucket/a/")).toBe("s3://bucket/a");
    expect(normalizePath("s3://")).toBe("s3://");
    expect(normalizePath("/")).toBe("/");
  });

  it("leaves local paths to path-browserify", () => {
    expect(normalizePath("/a//b")).toBe("/a/b");
  });
});

describe("parentPath", () => {
  it("walks up key segments", () => {
    expect(parentPath("s3://bucket/a/b/file.csv")).toBe("s3://bucket/a/b");
    expect(parentPath("s3://bucket/a/b")).toBe("s3://bucket/a");
  });

  it("returns the bucket for a single key segment", () => {
    expect(parentPath("s3://bucket/a")).toBe("s3://bucket");
  });

  it("returns the container-list root for a bucket, not a local-looking path", () => {
    expect(parentPath("s3://bucket")).toBe("s3://");
    expect(parentPath("s3://bucket")).not.toBe("s3:/");
  });

  it("handles a trailing slash without adding a level", () => {
    expect(parentPath("s3://bucket/a/b/")).toBe("s3://bucket/a");
    expect(parentPath("s3://bucket/")).toBe("s3://");
  });

  it("is idempotent at the root", () => {
    expect(parentPath("s3://")).toBe("s3://");
    expect(parentPath("/")).toBe("/");
    expect(parentPath("~")).toBe("~");
    expect(parentPath("C:\\")).toBe("C:\\");
  });

  it.each(["gs://", "az://", "abfss://", "gcs://", "s3a://", "adl://", "abfs://"])(
    "preserves the %s scheme",
    (scheme) => {
      expect(parentPath(`${scheme}container/a/b`)).toBe(`${scheme}container/a`);
    },
  );

  it("walks local paths", () => {
    expect(parentPath("/a/b")).toBe("/a");
    expect(parentPath("/a")).toBe("/");
  });

  it("reaches a fixed point from any input", () => {
    ["s3://bucket/a/b/c", "/a/b/c", "C:\\a\\b"].forEach((start) => {
      let current = start;
      for (let step = 0; step < 64; step += 1) {
        const next = parentPath(current);
        if (next === current) break;
        current = next;
      }
      expect(parentPath(current)).toBe(current);
    });
  });
});

describe("joinPath", () => {
  it("joins a bucket onto the container-list root", () => {
    expect(joinPath("s3://", "bucket")).toBe("s3://bucket");
  });

  it("joins a key onto a bucket", () => {
    expect(joinPath("s3://bucket", "key")).toBe("s3://bucket/key");
    expect(joinPath("s3://bucket/", "key")).toBe("s3://bucket/key");
    expect(joinPath("s3://bucket/a", "b")).toBe("s3://bucket/a/b");
  });

  it("trims slashes from the child", () => {
    expect(joinPath("s3://bucket", "/folder/")).toBe("s3://bucket/folder");
  });

  it("treats an empty child as a no-op", () => {
    expect(joinPath("s3://bucket/a", "")).toBe("s3://bucket/a");
  });

  it("never produces a doubled or collapsed scheme separator", () => {
    const joined = joinPath(joinPath("s3://", "bucket"), "key");
    expect(joined).toBe("s3://bucket/key");
    expect(joined).not.toContain("s3:///");
    expect(joined).not.toContain("s3:/bucket");
  });

  it("preserves windows separators", () => {
    expect(joinPath("C:\\a", "b")).toBe("C:\\a\\b");
  });

  it("joins posix paths", () => {
    expect(joinPath("/a", "b")).toBe("/a/b");
    expect(joinPath("", "x")).toBe("x");
  });
});

describe("basename / dirname", () => {
  it("names an object", () => {
    expect(basename("s3://bucket/a/file.csv")).toBe("file.csv");
    expect(dirname("s3://bucket/a/file.csv")).toBe("s3://bucket/a");
  });

  it("names a prefix without its trailing slash", () => {
    expect(basename("s3://bucket/a/b/")).toBe("b");
  });

  it("names a container", () => {
    expect(basename("s3://bucket")).toBe("bucket");
    expect(dirname("s3://bucket")).toBe("s3://");
  });

  it("falls back to the scheme at the root", () => {
    expect(basename("s3://")).toBe("s3://");
  });

  it("round-trips with joinPath", () => {
    ["s3://bucket/a/b.csv", "s3://bucket/a", "/a/b.csv"].forEach((value) => {
      expect(joinPath(dirname(value), basename(value))).toBe(normalizePath(value));
    });
  });
});

describe("isRootPath", () => {
  it.each(["", "/", "~", "C:\\", "C:/", "s3://", "gs://", "abfss://"])("is true for %s", (value) => {
    expect(isRootPath(value)).toBe(true);
  });

  it.each(["s3://bucket", "/a", "C:\\a", "~/x"])("is false for %s", (value) => {
    expect(isRootPath(value)).toBe(false);
  });
});
