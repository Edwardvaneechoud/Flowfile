import axios from "axios";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
  GoogleAnalyticsConnectionTestResult,
  GoogleAnalyticsOAuthStartResponse,
  PythonGoogleAnalyticsConnectionInterface,
  PythonGoogleAnalyticsConnectionMetadata,
} from "./GoogleAnalyticsConnectionTypes";

const API_BASE_URL = "/ga_connections";

const toPythonMetadata = (
  c: GoogleAnalyticsConnectionMetadata,
): PythonGoogleAnalyticsConnectionMetadata => ({
  connection_name: c.connectionName,
  description: c.description ?? null,
  default_property_id: c.defaultPropertyId ?? null,
});

const fromPythonInterface = (
  p: PythonGoogleAnalyticsConnectionInterface,
): GoogleAnalyticsConnectionInterface => ({
  connectionName: p.connection_name,
  description: p.description,
  defaultPropertyId: p.default_property_id,
  oauthUserEmail: p.oauth_user_email,
});

type AxiosErrorShape = { response?: { data?: { detail?: string } } };

const extractDetail = (error: unknown, fallback: string): string =>
  (error as AxiosErrorShape).response?.data?.detail || fallback;

export const fetchGoogleAnalyticsConnections = async (): Promise<
  GoogleAnalyticsConnectionInterface[]
> => {
  const response = await axios.get<PythonGoogleAnalyticsConnectionInterface[]>(
    `${API_BASE_URL}/ga_connections`,
  );
  return response.data.map(fromPythonInterface);
};

export const updateGoogleAnalyticsConnectionMetadata = async (
  data: GoogleAnalyticsConnectionMetadata,
): Promise<void> => {
  try {
    await axios.put(`${API_BASE_URL}/ga_connection`, toPythonMetadata(data));
  } catch (error) {
    throw new Error(extractDetail(error, "Failed to update Google Analytics connection"));
  }
};

export const deleteGoogleAnalyticsConnection = async (connectionName: string): Promise<void> => {
  await axios.delete(
    `${API_BASE_URL}/ga_connection?connection_name=${encodeURIComponent(connectionName)}`,
  );
};

export const startGoogleAnalyticsOAuth = async (
  data: GoogleAnalyticsConnectionMetadata,
): Promise<GoogleAnalyticsOAuthStartResponse> => {
  const params: Record<string, string> = { connection_name: data.connectionName };
  if (data.description) params.description = data.description;
  if (data.defaultPropertyId) params.default_property_id = data.defaultPropertyId;
  const response = await axios.get<{ auth_url: string }>(`${API_BASE_URL}/oauth/start`, {
    params,
  });
  return { authUrl: response.data.auth_url };
};

export const testGoogleAnalyticsConnection = async (
  connectionName: string,
): Promise<GoogleAnalyticsConnectionTestResult> => {
  try {
    const response = await axios.post<GoogleAnalyticsConnectionTestResult>(`${API_BASE_URL}/test`, {
      connection_name: connectionName,
    });
    return response.data;
  } catch (error) {
    return { success: false, message: extractDetail(error, "Connection test failed") };
  }
};
