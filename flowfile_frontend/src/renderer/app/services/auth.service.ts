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

interface UserInfo {
  username: string;
  email?: string;
  full_name?: string;
}

class AuthService {
  private token = ref<string | null>(null);
  private tokenExpiration = ref<number | null>(null);
  private isElectronMode = ref(false);
  private refreshPromise: Promise<string | null> | null = null;
  private currentUsername = ref<string | null>(null);

  constructor() {
    // Detect environment - check for Electron or Docker mode
    this.isElectronMode.value = this.detectElectronMode();

    // Clear any potentially invalid tokens on startup
    this.clearStoredTokens();

    // Then try to load a valid token if one exists
    const savedToken = localStorage.getItem('auth_token');
    const savedExpiration = localStorage.getItem('auth_token_expiration');
    const savedUsername = localStorage.getItem('auth_username');

    if (savedToken && savedExpiration) {
      const expirationTime = parseInt(savedExpiration, 10);

      // Only set the token if it's not expired
      if (expirationTime > Date.now()) {
        this.token.value = savedToken;
        this.tokenExpiration.value = expirationTime;
        this.currentUsername.value = savedUsername;
        console.log(`Constructor: Loaded valid token from storage, expires at ${new Date(expirationTime)}`);
      } else {
        console.log(`Constructor: Found expired token in storage, clearing it`);
        this.clearStoredTokens();
      }
    }
  }

  private detectElectronMode(): boolean {
    // Check if running in Electron by looking for the electronAPI exposed by preload
    // In Docker/web mode, this won't be available
    return !!(window as unknown as { electronAPI?: unknown }).electronAPI;
  }

  isInElectronMode(): boolean {
    return this.isElectronMode.value;
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
      localStorage.removeItem('auth_username');
    }
  }

  async initialize(): Promise<boolean> {
    // First, clear any invalid tokens
    this.clearStoredTokens();

    console.log(`Initializing auth service, has valid token: ${this.hasValidToken()}, electron mode: ${this.isElectronMode.value}`);

    if (this.hasValidToken()) {
      console.log('Using existing valid token');
      return true;
    }

    // In Electron mode, auto-authenticate without credentials
    if (this.isElectronMode.value) {
      console.log('Electron mode: auto-authenticating');
      return await this.getElectronToken();
    }

    // In Docker/web mode, require manual login
    console.log('Docker mode: manual login required');
    return false;
  }

  /**
   * Login with username and password (for Docker/web mode)
   */
  async login(username: string, password: string): Promise<boolean> {
    try {
      console.log('Attempting login with credentials');

      const formData = new FormData();
      formData.append('username', username);
      formData.append('password', password);

      const response = await axios.post<AuthResponse>('/auth/token', formData, {
        headers: {
          'X-Skip-Auth-Header': 'true',
          'Content-Type': 'multipart/form-data',
        },
      });

      if (response.data && response.data.access_token) {
        const expirationTime = response.data.expires_at || this.calculateExpiration();
        this.setToken(response.data.access_token, expirationTime, username);
        console.log(`Login successful for user: ${username}`);
        return true;
      }

      console.error('No access token in login response');
      return false;
    } catch (error) {
      console.error('Login failed:', error);
      return false;
    }
  }

  /**
   * Get current user information from the backend
   */
  async getCurrentUser(): Promise<UserInfo | null> {
    if (!this.hasValidToken()) {
      return null;
    }

    // If we have a stored username, return it
    if (this.currentUsername.value) {
      return { username: this.currentUsername.value };
    }

    try {
      const response = await axios.get<UserInfo>('/auth/users/me');
      if (response.data) {
        this.currentUsername.value = response.data.username;
        localStorage.setItem('auth_username', response.data.username);
        return response.data;
      }
      return null;
    } catch (error) {
      console.error('Failed to get current user:', error);
      return null;
    }
  }

  /**
   * Check if login is required (Docker/web mode without valid token)
   */
  requiresLogin(): boolean {
    return !this.isElectronMode.value && !this.hasValidToken();
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

    // In Docker mode, don't try to auto-refresh - require manual login
    if (!this.isElectronMode.value) {
      console.log('Docker mode: no valid token, login required');
      return null;
    }

    // In Electron mode, refresh the token automatically
    console.log('Electron mode: token invalid or expired, refreshing...');
    this.refreshPromise = this.refreshToken();
    const newToken = await this.refreshPromise;
    this.refreshPromise = null;
    return newToken;
  }

  private async refreshToken(): Promise<string | null> {
    // Only auto-refresh in Electron mode
    if (!this.isElectronMode.value) {
      console.log('Docker mode: cannot auto-refresh, login required');
      return null;
    }

    console.log('Attempting to refresh token');
    const success = await this.getElectronToken();
    if (!success) {
      console.log('Failed to refresh token, will retry on next request');
      // Clear any existing invalid token
      this.logout();
    }
    return success ? this.token.value : null;
  }
  
  private setToken(token: string, expiration: number, username?: string): void {
    this.token.value = token;
    this.tokenExpiration.value = expiration;

    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_token_expiration', expiration.toString());

    if (username) {
      this.currentUsername.value = username;
      localStorage.setItem('auth_username', username);
    }

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
    
    return isValid;
  }
  
  isAuthenticated(): boolean {
    return this.hasValidToken();
  }
  
  logout(): void {
    this.token.value = null;
    this.tokenExpiration.value = null;
    this.currentUsername.value = null;

    // Ensure tokens are removed from localStorage
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_token_expiration');
    localStorage.removeItem('auth_username');

    console.log('User logged out, token cleared from memory and storage');
  }
}

// Create an axios interceptor to automatically handle 401 errors
axios.interceptors.response.use(
  (response) => {
    return response;
  },
  async (error) => {
    const originalRequest = error.config;
    const requestUrl = originalRequest?.url || '';

    // Never retry for auth/token requests - these are login attempts
    const isAuthRequest = requestUrl.includes('/auth/token') || requestUrl.includes('/auth/');

    // If the error is 401 and we haven't already tried to refresh the token
    if (error.response?.status === 401 && !originalRequest._retry && !isAuthRequest) {
      console.log('Received 401 error, checking if we can refresh token');
      originalRequest._retry = true;

      // Check if we're in Electron mode (can auto-refresh) or Docker mode (need login)
      const isElectron = !!(window as unknown as { electronAPI?: unknown }).electronAPI;

      if (isElectron) {
        // In Electron mode, try to auto-refresh the token
        console.log('Electron mode: attempting auto-refresh');

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
      } else {
        // In Docker mode, clear tokens and redirect to login
        console.log('Docker mode: 401 received, redirecting to login');
        localStorage.removeItem('auth_token');
        localStorage.removeItem('auth_token_expiration');
        localStorage.removeItem('auth_username');

        // Redirect to login page if not already there
        if (window.location.pathname !== '/login') {
          window.location.href = '/login';
        }
      }
    }

    return Promise.reject(error);
  }
);

export const authService = new AuthService();
export default authService;