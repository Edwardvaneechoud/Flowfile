import axios from "axios";

const API_BASE_URL = "/ga_connections/oauth/client_config";

export interface GoogleOAuthConfigView {
  clientId: string;
  redirectUri: string;
  isConfigured: boolean;
}

export interface GoogleOAuthConfigInput {
  clientId: string;
  clientSecret: string;
  redirectUri: string;
}

interface PythonGoogleOAuthConfigView {
  client_id: string;
  redirect_uri: string;
  is_configured: boolean;
}

export const fetchGoogleOAuthConfig = async (): Promise<GoogleOAuthConfigView> => {
  const response = await axios.get<PythonGoogleOAuthConfigView>(API_BASE_URL);
  return {
    clientId: response.data.client_id,
    redirectUri: response.data.redirect_uri,
    isConfigured: response.data.is_configured,
  };
};

export const saveGoogleOAuthConfig = async (data: GoogleOAuthConfigInput): Promise<void> => {
  await axios.put(API_BASE_URL, {
    client_id: data.clientId,
    client_secret: data.clientSecret,
    redirect_uri: data.redirectUri,
  });
};

export const clearGoogleOAuthConfig = async (): Promise<void> => {
  await axios.delete(API_BASE_URL);
};
