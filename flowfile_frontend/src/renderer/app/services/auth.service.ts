// src/app/services/auth.service.ts
import axios from 'axios';
import { ref } from 'vue';

interface AuthResponse {
  access_token: string;
  token_type: string;
  expires_at?: number;
  user?: {
    id: string;
    username: string;
    email?: string;
  };
}

class AuthService {
  private token = ref<string | null>(null);
  private tokenExpiration = ref<number | null>(null);
  private isElectronMode = ref(false);
  private refreshPromise: Promise<string | null> | null = null;
  
  constructor() {
    // Simplified detection that works in both Docker and Electron
    this.isElectronMode.value = true; // Always treat as Electron for now
    
    // Clear any potentially invalid tokens on startup
    this.clearStoredTokens();
    
    // Then try to load a valid token if one exists
    const savedToken = localStorage.getItem('auth_token');
    const savedExpiration = localStorage.getItem('auth_token_expiration');
    
    if (savedToken && savedExpiration) {
      const expirationTime = parseInt(savedExpiration, 10);
      
      // Only set the token if it's not expired
      if (expirationTime > Date.now()) {
        this.token.value = savedToken;
        this.tokenExpiration.value = expirationTime;
        console.log(`Constructor: Loaded valid token from storage, expires at ${new Date(expirationTime)}`);
      } else {
        console.log(`Constructor: Found expired token in storage, clearing it`);
        this.clearStoredTokens();
      }
    }
  }
  
  /**
   * Checks local storage for invalid tokens and clears them
   */
  private clearStoredTokens(): void {
    const savedExpiration = localStorage.getItem('auth_token_expiration');
    
    // Clear token if it's expired or if expiration is missing
    if (!savedExpiration || parseInt(savedExpiration, 10) <= Date.now()) {
      console.log('Clearing invalid or expired tokens from storage');
      localStorage.removeItem('auth_token');
      localStorage.removeItem('auth_token_expiration');
    }
  }

  async initialize(): Promise<boolean> {
    // First, clear any invalid tokens
    this.clearStoredTokens();
    
    console.log(`Initializing auth service, has valid token: ${this.hasValidToken()}`);
    
    if (this.hasValidToken()) {
      console.log('Using existing valid token');
      return true;
    }
    
    console.log('No valid token found, requesting new token');
    // Always try to get a token regardless of environment
    return await this.getElectronToken();
  }
  
  private async getElectronToken(): Promise<boolean> {
    try {
      console.log("Requesting new auth token");
      const response = await axios.post<AuthResponse>('/auth/token', {}, {
        headers: { 'X-Skip-Auth-Header': 'true' }
      });
      
      if (response.data && response.data.access_token) {
        const expirationTime = response.data.expires_at || this.calculateExpiration();
        this.setToken(response.data.access_token, expirationTime);
        
        console.log(`Token obtained successfully, expires at ${new Date(expirationTime)}`);
        return true;
      }
      
      console.error("No access token in response");
      return false;
    } catch (error) {
      console.error('Failed to get token:', error);
      return false;
    }
  }
  
  async getToken(): Promise<string | null> {
    // If there's already a refresh in progress, wait for it
    if (this.refreshPromise) {
      return this.refreshPromise;
    }
    
    // If the token is valid, return it immediately
    if (this.hasValidToken()) {
      return this.token.value;
    }
    
    // Otherwise, refresh the token
    console.log('Token invalid or expired, refreshing...');
    this.refreshPromise = this.refreshToken();
    const newToken = await this.refreshPromise;
    this.refreshPromise = null;
    return newToken;
  }
  
  private async refreshToken(): Promise<string | null> {
    console.log('Attempting to refresh token');
    const success = await this.getElectronToken();
    if (!success) {
      console.log('Failed to refresh token, will retry on next request');
      // Clear any existing invalid token
      this.logout();
    }
    return success ? this.token.value : null;
  }
  
  private setToken(token: string, expiration: number): void {
    this.token.value = token;
    this.tokenExpiration.value = expiration;
    
    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_token_expiration', expiration.toString());
    
    console.log(`Token set, expires at ${new Date(expiration)}`);
  }
  
  private calculateExpiration(hoursTilExpire = 1): number {
    // Convert hours to milliseconds (fixed the bug where it was only 59ms)
    return Date.now() + (hoursTilExpire * 60 * 60 * 1000);
  }
  
  hasValidToken(): boolean {
    const isValid = !!(
      this.token.value && 
      this.tokenExpiration.value && 
      this.tokenExpiration.value > Date.now()
    );
    
    console.log(`Token validation check:
      - Has token: ${!!this.token.value}
      - Has expiration: ${!!this.tokenExpiration.value}
      - Current time: ${new Date(Date.now())}
      - Expiration time: ${this.tokenExpiration.value ? new Date(this.tokenExpiration.value) : 'none'}
      - Is valid: ${isValid}
    `);
    
    return isValid;
  }
  
  isAuthenticated(): boolean {
    return this.hasValidToken();
  }
  
  logout(): void {
    this.token.value = null;
    this.tokenExpiration.value = null;
    
    // Ensure tokens are removed from localStorage
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_token_expiration');
    
    console.log('User logged out, token cleared from memory and storage');
    
    // Force a check for any other potential tokens
    this.clearStoredTokens();
  }
}

// Create an axios interceptor to automatically handle 401 errors
axios.interceptors.response.use(
  (response) => {
    return response;
  },
  async (error) => {
    const originalRequest = error.config;
    
    // If the error is 401 and we haven't already tried to refresh the token
    if (error.response?.status === 401 && !originalRequest._retry) {
      console.log('Received 401 error, clearing any invalid tokens and retrying');
      originalRequest._retry = true;
      
      // Clear any existing tokens first
      localStorage.removeItem('auth_token');
      localStorage.removeItem('auth_token_expiration');
      
      // Force a completely fresh authentication
      const authInstance = new AuthService();
      
      // Force token refresh with a clean state
      await authInstance.initialize();
      const newToken = await authInstance.getToken();
      
      if (newToken) {
        console.log('Successfully obtained new token after 401 error');
        // Update the authorization header with the new token
        originalRequest.headers['Authorization'] = `Bearer ${newToken}`;
        // Retry the original request
        return axios(originalRequest);
      } else {
        console.error('Failed to get new token after 401 error');
      }
    }
    
    return Promise.reject(error);
  }
);

export const authService = new AuthService();
export default authService;