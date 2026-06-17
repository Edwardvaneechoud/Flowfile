// Google Analytics 4 connection types. Mirrors
// flowfile_core/flowfile_core/schemas/google_analytics_schemas.py.

export type GoogleAnalyticsAuthMethod = "oauth" | "service_account";

export interface PythonGoogleAnalyticsConnectionMetadata {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
}

export interface PythonGoogleAnalyticsServiceAccountInput {
  connection_name: string;
  service_account_key: string;
  description?: string | null;
  default_property_id?: string | null;
}

export interface PythonGoogleAnalyticsConnectionInterface {
  connection_name: string;
  description?: string | null;
  default_property_id?: string | null;
  auth_method?: GoogleAnalyticsAuthMethod;
  oauth_user_email?: string | null;
}

export interface GoogleAnalyticsConnectionMetadata {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
}

export interface GoogleAnalyticsServiceAccountInput {
  connectionName: string;
  serviceAccountKey: string;
  description?: string | null;
  defaultPropertyId?: string | null;
}

export interface GoogleAnalyticsConnectionInterface {
  connectionName: string;
  description?: string | null;
  defaultPropertyId?: string | null;
  authMethod?: GoogleAnalyticsAuthMethod;
  oauthUserEmail?: string | null;
}

export interface GoogleAnalyticsConnectionTestResult {
  success: boolean;
  message: string;
}

export interface GoogleAnalyticsOAuthStartResponse {
  authUrl: string;
}
