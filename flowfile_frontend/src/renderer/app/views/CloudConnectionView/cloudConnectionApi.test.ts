import { describe, expect, it } from "vitest";

import { convertConnectionInterfacePytoTs, toPythonFormat } from "./api";
import { authMethodsByStorageType } from "./CloudConnectionTypes";
import type { FullCloudStorageConnection } from "./CloudConnectionTypes";

describe("authMethodsByStorageType", () => {
  it("never offers the node-level 'auto' mode, which the backend rejects on a connection", () => {
    for (const methods of Object.values(authMethodsByStorageType)) {
      expect(methods.map((m) => m.value)).not.toContain("auto");
    }
  });

  it("keeps AWS CLI available for S3", () => {
    expect(authMethodsByStorageType.s3.map((m) => m.value)).toEqual([
      "access_key",
      "iam_role",
      "aws-cli",
    ]);
  });
});

describe("toPythonFormat", () => {
  const base: FullCloudStorageConnection = {
    connectionName: "minio connection",
    storageType: "s3",
    authMethod: "aws-cli",
    awsRegion: "us-east-1",
    endpointUrl: "http://localhost:9000",
    verifySsl: false,
  };

  it("sends the AWS profile", () => {
    expect(toPythonFormat({ ...base, awsProfile: "analytics" }).aws_profile).toBe("analytics");
  });

  it("sends the session token", () => {
    const payload = toPythonFormat({ ...base, authMethod: "access_key", awsSessionToken: "tok" });
    expect(payload.aws_session_token).toBe("tok");
  });
});

describe("convertConnectionInterfacePytoTs", () => {
  it("reads the AWS profile back", () => {
    const converted = convertConnectionInterfacePytoTs({
      connection_name: "minio connection",
      storage_type: "s3",
      auth_method: "aws-cli",
      aws_profile: "analytics",
      verify_ssl: true,
    });
    expect(converted.awsProfile).toBe("analytics");
  });
});
