// Google Analytics 4 connection types. Mirrors
// flowfile_core/flowfile_core/schemas/google_analytics_schemas.py.

export interface PythonGoogleAnalyticsConnectionMetadata {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
}

export interface PythonGoogleAnalyticsConnectionInterface {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
  oauth_user_email?: string | null;
}

export interface GoogleAnalyticsConnectionMetadata {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
}

export interface GoogleAnalyticsConnectionInterface {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
  oauthUserEmail?: string | null;
}

export interface GoogleAnalyticsConnectionTestResult {
  success: boolean;
  message: string;
}

export interface GoogleAnalyticsOAuthStartResponse {
  authUrl: string;
}
