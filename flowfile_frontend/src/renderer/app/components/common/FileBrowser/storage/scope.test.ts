import { describe, it, expect } from "vitest";

import { storageScopeKey } from "./scope";

describe("storageScopeKey", () => {
  it("is stable for the local filesystem", () => {
    expect(storageScopeKey({ kind: "local" })).toBe("local");
  });

  it("distinguishes connections", () => {
    const a = storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: "prod" });
    const b = storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: "staging" });
    expect(a).not.toBe(b);
  });

  it("distinguishes storage types on the same connection name", () => {
    const s3 = storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: "data" });
    const gcs = storageScopeKey({ kind: "cloud", storageType: "gcs", connectionName: "data" });
    expect(s3).not.toBe(gcs);
  });

  it("separates ambient credentials from a named connection", () => {
    const ambient = storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: null });
    const named = storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: "prod" });
    expect(ambient).not.toBe(named);
    expect(ambient).toBe(storageScopeKey({ kind: "cloud", storageType: "s3" }));
  });

  it("never collides with the local scope", () => {
    expect(storageScopeKey({ kind: "cloud", storageType: "s3", connectionName: "local" })).not.toBe(
      "local",
    );
  });

  it("is deterministic", () => {
    const location = { kind: "cloud", storageType: "adls", connectionName: "lake" } as const;
    expect(storageScopeKey(location)).toBe(storageScopeKey(location));
  });
});
