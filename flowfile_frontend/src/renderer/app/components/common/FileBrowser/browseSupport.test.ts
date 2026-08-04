import { describe, it, expect } from "vitest";

import { browseUnsupportedReason, canBrowseConnection } from "./browseSupport";

describe("browseUnsupportedReason", () => {
  it.each([
    ["s3", "access_key"],
    ["s3", "iam_role"],
    ["s3", "aws-cli"],
    ["s3", "env_vars"],
    ["gcs", "service_account"],
    ["gcs", "env_vars"],
    ["adls", "access_key"],
    ["adls", "sas_token"],
  ])("allows %s with %s", (storageType, authMethod) => {
    expect(browseUnsupportedReason(storageType, authMethod)).toBeNull();
    expect(canBrowseConnection(storageType, authMethod)).toBe(true);
  });

  it.each(["service_principal", "managed_identity"])(
    "refuses adls with %s and says what to do instead",
    (authMethod) => {
      const reason = browseUnsupportedReason("adls", authMethod);
      expect(reason).toContain("manually");
      expect(canBrowseConnection("adls", authMethod)).toBe(false);
    },
  );

  it("does not refuse s3 or gcs for the identity auth methods", () => {
    expect(canBrowseConnection("s3", "service_principal")).toBe(true);
    expect(canBrowseConnection("gcs", "managed_identity")).toBe(true);
  });

  it("refuses an unknown storage type", () => {
    expect(browseUnsupportedReason("hdfs", "access_key")).toContain("hdfs");
  });

  it("says nothing when no storage type is known yet", () => {
    expect(browseUnsupportedReason(null, null)).toBeNull();
    expect(browseUnsupportedReason(undefined, "access_key")).toBeNull();
  });

  it("allows adls when the auth method is not yet known", () => {
    expect(canBrowseConnection("adls", null)).toBe(true);
  });
});
