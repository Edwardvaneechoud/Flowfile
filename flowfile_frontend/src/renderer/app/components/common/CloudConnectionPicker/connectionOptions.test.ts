import { describe, expect, it } from "vitest";

import type { FullCloudStorageConnectionInterface } from "../../../views/CloudConnectionView/CloudConnectionTypes";
import {
  NO_CONNECTION_VALUE,
  connectionSelectValue,
  resolveConnection,
  unavailableConnectionName,
} from "./connectionOptions";

const conn = (connectionName: string): FullCloudStorageConnectionInterface =>
  ({
    connectionName,
    storageType: "s3",
    authMethod: "access_key",
  }) as FullCloudStorageConnectionInterface;

const minio = conn("minio_connection");
const production = conn("production-data");
const connections = [minio, production];

describe("connectionSelectValue", () => {
  it("uses the connection name", () => {
    expect(connectionSelectValue(production)).toBe("production-data");
  });

  it("maps null and undefined to the no-connection sentinel", () => {
    expect(connectionSelectValue(null)).toBe(NO_CONNECTION_VALUE);
    expect(connectionSelectValue(undefined)).toBe(NO_CONNECTION_VALUE);
  });

  // Regression guard: binding the object itself made every option "[object Object]",
  // so the browser always selected the first one.
  it("gives distinct connections distinct values", () => {
    expect(connectionSelectValue(minio)).not.toBe(connectionSelectValue(production));
  });
});

describe("resolveConnection", () => {
  it("round-trips a connection through its select value", () => {
    expect(resolveConnection(connections, connectionSelectValue(production))).toBe(production);
  });

  it("resolves the sentinel to null", () => {
    expect(resolveConnection(connections, NO_CONNECTION_VALUE)).toBeNull();
  });

  it("resolves a name that is no longer in the list to null", () => {
    expect(resolveConnection(connections, "deleted-connection")).toBeNull();
  });

  it("picks the first match when a shared connection shadows an owned name", () => {
    const shared = conn("production-data");
    expect(resolveConnection([production, shared], "production-data")).toBe(production);
  });
});

describe("unavailableConnectionName", () => {
  it("is null when the saved connection still resolves", () => {
    expect(unavailableConnectionName(connections, "production-data")).toBeNull();
  });

  it("is null when nothing was saved", () => {
    expect(unavailableConnectionName(connections, null)).toBeNull();
    expect(unavailableConnectionName(connections, undefined)).toBeNull();
  });

  it("reports a saved connection that was deleted or unshared", () => {
    expect(unavailableConnectionName(connections, "deleted-connection")).toBe(
      "deleted-connection",
    );
  });

  // A failed fetch leaves the list empty; the saved name must still surface
  // rather than reading as "no connection".
  it("reports the saved connection when the list failed to load", () => {
    expect(unavailableConnectionName([], "production-data")).toBe("production-data");
  });
});

describe("connectionSelectValue with an unavailable connection", () => {
  it("selects the unavailable placeholder when nothing resolved", () => {
    expect(connectionSelectValue(null, "deleted-connection")).toBe("deleted-connection");
  });

  it("prefers a resolved connection over the placeholder", () => {
    expect(connectionSelectValue(production, "deleted-connection")).toBe("production-data");
  });
});
