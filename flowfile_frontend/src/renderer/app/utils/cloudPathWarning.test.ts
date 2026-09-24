import { describe, it, expect } from "vitest";

import { cloudPathWarning } from "./cloudPathWarning";

describe("cloudPathWarning", () => {
  it.each([undefined, null, "", "   "])("warns about a missing writer path (%s)", (path) => {
    expect(cloudPathWarning(path, "writer")).toMatch(/No target path set/);
    expect(cloudPathWarning(path, "writer")).toContain("s3://bucket/folder/table");
  });

  it("uses reader wording for a missing reader path", () => {
    expect(cloudPathWarning("", "reader")).toMatch(/No source path set/);
    expect(cloudPathWarning("", "reader")).toContain("s3://bucket/folder/file.parquet");
  });

  it.each([
    "s3://bucket/folder/table",
    "az://container/table",
    "abfss://container@account.dfs.core.windows.net/table",
    "gs://bucket/file.parquet",
  ])("accepts the cloud URI %s", (path) => {
    expect(cloudPathWarning(path, "writer")).toBeNull();
    expect(cloudPathWarning(path, "reader")).toBeNull();
  });

  it.each(["output/table", "/tmp/table", "C:\\data\\table", "s3:/bucket", "S3://bucket/key"])(
    "warns that %s is not a cloud URI",
    (path) => {
      expect(cloudPathWarning(path, "writer")).toBe(
        `'${path}' is not a cloud storage URI. Use a path starting with s3://, az://, abfss:// or gs://.`,
      );
    },
  );

  it("leaves a path that starts with a flow parameter to run time", () => {
    expect(cloudPathWarning("${target_uri}", "writer")).toBeNull();
    expect(cloudPathWarning("${bucket}/folder/table", "reader")).toBeNull();
  });

  it("still warns when a parameter sits after a schemeless prefix", () => {
    expect(cloudPathWarning("folder/${name}", "writer")).toMatch(/is not a cloud storage URI/);
  });
});
