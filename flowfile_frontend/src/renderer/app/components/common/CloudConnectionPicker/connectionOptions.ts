import type { FullCloudStorageConnectionInterface } from "../../../views/CloudConnectionView/CloudConnectionTypes";
import type { CloudStorageKind } from "../../../utils/storagePath";

// Native <select>/<option> values are strings, so options key off connectionName:
// binding the connection object makes every option stringify to "[object Object]".
export const NO_CONNECTION_VALUE = "";

export function connectionSelectValue(
  connection: FullCloudStorageConnectionInterface | null | undefined,
  unavailableConnection?: string | null,
): string {
  return connection?.connectionName || unavailableConnection || NO_CONNECTION_VALUE;
}

export function resolveConnection(
  connections: FullCloudStorageConnectionInterface[],
  value: string,
): FullCloudStorageConnectionInterface | null {
  if (value === NO_CONNECTION_VALUE) return null;
  // First match wins, mirroring core's own-shadows-shared connection resolution.
  return connections.find((conn) => conn.connectionName === value) ?? null;
}

// A saved connection_name the current list can't resolve — deleted, share revoked,
// or the fetch failed. Surfaced so the picker never silently reads as "no connection".
export function unavailableConnectionName(
  connections: FullCloudStorageConnectionInterface[],
  savedName: string | null | undefined,
): string | null {
  if (!savedName) return null;
  return resolveConnection(connections, savedName) ? null : savedName;
}

export interface NoConnectionChoice {
  label: string;
  warning: string;
  disabled: boolean;
}

/**
 * The "No connection" choice of a cloud storage node: it runs on the credentials of the
 * machine running Flowfile for the provider the node's path names (core picks it by URI
 * scheme; AWS when there is none) and ignores every saved connection's endpoint.
 * Multi-user (docker) servers refuse their own credentials, so the choice is disabled there.
 */
export function ambientCredentialsChoice(
  multiUser: boolean,
  storageType: CloudStorageKind | null = null,
): NoConnectionChoice {
  if (multiUser) {
    return {
      label: "No connection (not available on this server)",
      warning: "This server does not use its own cloud credentials. Select a cloud connection.",
      disabled: true,
    };
  }
  if (storageType === "adls") {
    return {
      label: "No connection (this machine's Azure credentials)",
      warning:
        "Uses the Azure credentials of the machine running Flowfile (AZURE_* environment " +
        "variables), not a saved connection. Azurite and other emulators need a connection.",
      disabled: false,
    };
  }
  if (storageType === "gcs") {
    return {
      label: "No connection (this machine's Google Cloud credentials)",
      warning:
        "Uses the Google Cloud credentials of the machine running Flowfile " +
        "(GOOGLE_APPLICATION_CREDENTIALS or gcloud application-default login), not a saved " +
        "connection. GCS emulators need a connection.",
      disabled: false,
    };
  }
  return {
    label: "No connection (this machine's AWS credentials)",
    warning:
      "Uses the AWS credentials of the machine running Flowfile (the ~/.aws profile or AWS_* " +
      "environment variables), not a saved connection's endpoint. MinIO and other " +
      "S3-compatible storage need a connection (or AWS_ENDPOINT_URL in that environment).",
    disabled: false,
  };
}
