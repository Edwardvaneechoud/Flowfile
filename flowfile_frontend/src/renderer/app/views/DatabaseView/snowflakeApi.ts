import axios from "axios";

import { detailOf } from "../../utils/apiDetail";

export interface SnowflakeSession {
  accountHost: string | null;
  username: string | null;
  role: string | null;
  warehouse: string | null;
  database: string | null;
  warehouses: string[];
  databases: string[];
  roles: string[];
}

export interface SnowflakeDiscoverRequest {
  account: string;
  authMethod: "access_token" | "key_pair";
  username: string;
  // Blank while editing a saved connection: the server then uses the stored secret.
  secret: string;
  privateKeyPassphrase?: string | null;
  role?: string | null;
  connectionName?: string | null;
}

export type SnowflakeSignInStatus = "pending" | "ok" | "error";

export interface SnowflakeOAuthStatus {
  status: SnowflakeSignInStatus;
  message: string | null;
  accountHost: string | null;
  username: string | null;
  role: string | null;
  warehouse: string | null;
  database: string | null;
  warehouses: string[];
  databases: string[];
}

/** The sign-in is unknown to core: expired, superseded or never started by this user. */
export class SnowflakeSignInExpiredError extends Error {}

interface PySession {
  account_host?: string | null;
  username?: string | null;
  role?: string | null;
  warehouse?: string | null;
  database?: string | null;
  warehouses?: string[] | null;
  databases?: string[] | null;
  roles?: string[] | null;
}

interface PyOAuthStatus extends PySession {
  status: SnowflakeSignInStatus;
  message?: string | null;
}

const toSession = (data: PySession): SnowflakeSession => ({
  accountHost: data.account_host ?? null,
  username: data.username ?? null,
  role: data.role ?? null,
  warehouse: data.warehouse ?? null,
  database: data.database ?? null,
  warehouses: data.warehouses ?? [],
  databases: data.databases ?? [],
  roles: data.roles ?? [],
});

export const discoverSnowflake = async (
  request: SnowflakeDiscoverRequest,
): Promise<SnowflakeSession> => {
  try {
    const response = await axios.post<PySession>("/snowflake/discover", {
      account: request.account,
      auth_method: request.authMethod,
      username: request.username,
      secret: request.secret,
      private_key_passphrase: request.privateKeyPassphrase ?? null,
      role: request.role ?? null,
      connection_name: request.connectionName ?? null,
    });
    return toSession(response.data);
  } catch (error) {
    throw new Error(detailOf(error, "Could not connect to Snowflake"));
  }
};

export const startSnowflakeOAuth = async (
  account: string,
  role: string | null,
): Promise<{ authUrl: string; flowId: string }> => {
  try {
    const response = await axios.post<{ auth_url: string; flow_id: string }>(
      "/snowflake/oauth/start",
      { account, role },
    );
    return { authUrl: response.data.auth_url, flowId: response.data.flow_id };
  } catch (error) {
    if ((error as { response?: { status?: number } })?.response?.status === 404) {
      throw new Error("Signing in with Snowflake is not available on this server.");
    }
    throw new Error(detailOf(error, "Could not start the Snowflake sign-in"));
  }
};

export const getSnowflakeOAuthStatus = async (flowId: string): Promise<SnowflakeOAuthStatus> => {
  try {
    const response = await axios.get<PyOAuthStatus>(
      `/snowflake/oauth/status/${encodeURIComponent(flowId)}`,
    );
    const session = toSession(response.data);
    return {
      status: response.data.status,
      message: response.data.message ?? null,
      accountHost: session.accountHost,
      username: session.username,
      role: session.role,
      warehouse: session.warehouse,
      database: session.database,
      warehouses: session.warehouses,
      databases: session.databases,
    };
  } catch (error) {
    if ((error as { response?: { status?: number } })?.response?.status === 404) {
      throw new SnowflakeSignInExpiredError("The sign-in expired. Sign in again.");
    }
    throw new Error(detailOf(error, "Could not check the Snowflake sign-in"));
  }
};
