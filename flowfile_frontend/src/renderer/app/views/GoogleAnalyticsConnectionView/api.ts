import axios from "axios";
import type {
  GoogleAnalyticsConnection,
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionTestResult,
  PythonGoogleAnalyticsConnection,
  PythonGoogleAnalyticsConnectionInterface,
} from "./GoogleAnalyticsConnectionTypes";

const API_BASE_URL = "/ga_connections";

const toPython = (c: GoogleAnalyticsConnection): PythonGoogleAnalyticsConnection => ({
  connection_name: c.connectionName,
  description: c.description ?? null,
  default_property_id: c.defaultPropertyId ?? null,
  service_account_json: c.serviceAccountJson ?? null,
});

const fromPythonInterface = (
  p: PythonGoogleAnalyticsConnectionInterface,
): GoogleAnalyticsConnectionInterface => ({
  connectionName: p.connection_name,
  description: p.description,
  defaultPropertyId: p.default_property_id,
});

export const fetchGoogleAnalyticsConnections = async (): Promise<
  GoogleAnalyticsConnectionInterface[]
> => {
  const response = await axios.get<PythonGoogleAnalyticsConnectionInterface[]>(
    `${API_BASE_URL}/ga_connections`,
  );
  return response.data.map(fromPythonInterface);
};

export const createGoogleAnalyticsConnection = async (
  data: GoogleAnalyticsConnection,
): Promise<void> => {
  try {
    await axios.post(`${API_BASE_URL}/ga_connection`, toPython(data));
  } catch (error) {
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to create Google Analytics connection";
    throw new Error(errorMsg);
  }
};

export const updateGoogleAnalyticsConnection = async (
  data: GoogleAnalyticsConnection,
): Promise<void> => {
  try {
    await axios.put(`${API_BASE_URL}/ga_connection`, toPython(data));
  } catch (error) {
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to update Google Analytics connection";
    throw new Error(errorMsg);
  }
};

export const deleteGoogleAnalyticsConnection = async (connectionName: string): Promise<void> => {
  await axios.delete(
    `${API_BASE_URL}/ga_connection?connection_name=${encodeURIComponent(connectionName)}`,
  );
};

export const testGoogleAnalyticsConnection = async (
  serviceAccountJson: string,
): Promise<GoogleAnalyticsConnectionTestResult> => {
  try {
    const response = await axios.post<GoogleAnalyticsConnectionTestResult>(
      `${API_BASE_URL}/test`,
      { service_account_json: serviceAccountJson },
    );
    return response.data;
  } catch (error) {
    const errorMsg = (error as any).response?.data?.detail || "Connection test failed";
    return { success: false, message: errorMsg };
  }
};
