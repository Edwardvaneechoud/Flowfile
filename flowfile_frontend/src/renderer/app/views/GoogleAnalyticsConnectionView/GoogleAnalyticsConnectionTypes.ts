// Google Analytics 4 connection types. Mirrors
// flowfile_core/flowfile_core/schemas/google_analytics_schemas.py.

export interface PythonGoogleAnalyticsConnection {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
  service_account_json?: string | null;
}

export interface PythonGoogleAnalyticsConnectionInterface {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
}

export interface GoogleAnalyticsConnection {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
  // Raw service-account JSON string. Kept only in memory during the form
  // lifecycle; the backend stores it encrypted.
  serviceAccountJson?: string | null;
}

export interface GoogleAnalyticsConnectionInterface {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
}

export interface GoogleAnalyticsConnectionTestResult {
  success: boolean;
  message: string;
}
