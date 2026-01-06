// src/app/services/auth.service.ts
import axios from "axios";
import { ref } from "vue";

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
  is_admin?: boolean;
  id?: number;
}

class AuthService {
  private token = ref<string | null>(null);
  private tokenExpiration = ref<number | null>(null);
  private isElectronMode = ref(false);
  private refreshPromise: Promise<string | null> | null = null;
  private currentUsername = ref<string | null>(null);

  constructor() {
    this.isElectronMode.value = this.detectElectronMode();
    this.clearStoredTokens();
    this.loadStoredToken();
  }

  private loadStoredToken(): void {
    const savedToken = localStorage.getItem("auth_token");
    const savedExpiration = localStorage.getItem("auth_token_expiration");
    const savedUsername = localStorage.getItem("auth_username");

    if (savedToken && savedExpiration) {
      const expirationTime = parseInt(savedExpiration, 10);
      if (expirationTime > Date.now()) {
        this.token.value = savedToken;
        this.tokenExpiration.value = expirationTime;
        this.currentUsername.value = savedUsername;
      } else {
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

  private clearStoredTokens(): void {
    const savedExpiration = localStorage.getItem("auth_token_expiration");
    if (!savedExpiration || parseInt(savedExpiration, 10) <= Date.now()) {
      localStorage.removeItem("auth_token");
      localStorage.removeItem("auth_token_expiration");
      localStorage.removeItem("auth_username");
    }
  }

  async initialize(): Promise<boolean> {
    this.clearStoredTokens();

    if (this.hasValidToken()) {
      return true;
    }

    // In Electron mode, auto-authenticate without credentials
    if (this.isElectronMode.value) {
      return await this.getElectronToken();
    }

    // In Docker/web mode, require manual login
    return false;
  }

  async login(username: string, password: string): Promise<boolean> {
    try {
      const formData = new FormData();
      formData.append("username", username);
      formData.append("password", password);

      const response = await axios.post<AuthResponse>("/auth/token", formData, {
        headers: {
          "X-Skip-Auth-Header": "true",
          "Content-Type": "multipart/form-data",
        },
      });

      if (response.data?.access_token) {
        const expirationTime = response.data.expires_at || this.calculateExpiration();
        this.setToken(response.data.access_token, expirationTime, username);
        return true;
      }
      return false;
    } catch (error) {
      console.error("Login failed:", error);
      return false;
    }
  }

  async getCurrentUser(): Promise<UserInfo | null> {
    if (!this.hasValidToken()) {
      return null;
    }

    try {
      const response = await axios.get<UserInfo>("/auth/users/me");
      if (response.data) {
        this.currentUsername.value = response.data.username;
        localStorage.setItem("auth_username", response.data.username);
        return response.data;
      }
      return null;
    } catch (error) {
      console.error("Failed to get current user:", error);
      return this.currentUsername.value ? { username: this.currentUsername.value } : null;
    }
  }

  requiresLogin(): boolean {
    return !this.isElectronMode.value && !this.hasValidToken();
  }

  private async getElectronToken(): Promise<boolean> {
    try {
      const response = await axios.post<AuthResponse>(
        "/auth/token",
        {},
        {
          headers: { "X-Skip-Auth-Header": "true" },
        },
      );

      if (response.data?.access_token) {
        const expirationTime = response.data.expires_at || this.calculateExpiration();
        this.setToken(response.data.access_token, expirationTime);
        return true;
      }
      return false;
    } catch (error) {
      console.error("Failed to get Electron token:", error);
      return false;
    }
  }

  async getToken(): Promise<string | null> {
    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    if (this.hasValidToken()) {
      return this.token.value;
    }

    // In Docker mode, don't auto-refresh - require manual login
    if (!this.isElectronMode.value) {
      return null;
    }

    // In Electron mode, refresh the token automatically
    this.refreshPromise = this.refreshToken();
    const newToken = await this.refreshPromise;
    this.refreshPromise = null;
    return newToken;
  }

  private async refreshToken(): Promise<string | null> {
    if (!this.isElectronMode.value) {
      return null;
    }

    const success = await this.getElectronToken();
    if (!success) {
      this.logout();
    }
    return success ? this.token.value : null;
  }

  private setToken(token: string, expiration: number, username?: string): void {
    this.token.value = token;
    this.tokenExpiration.value = expiration;
    localStorage.setItem("auth_token", token);
    localStorage.setItem("auth_token_expiration", expiration.toString());

    if (username) {
      this.currentUsername.value = username;
      localStorage.setItem("auth_username", username);
    }
  }

  private calculateExpiration(hoursTilExpire = 1): number {
    return Date.now() + hoursTilExpire * 60 * 60 * 1000;
  }

  hasValidToken(): boolean {
    return !!(
      this.token.value &&
      this.tokenExpiration.value &&
      this.tokenExpiration.value > Date.now()
    );
  }

  isAuthenticated(): boolean {
    return this.hasValidToken();
  }

  logout(): void {
    this.token.value = null;
    this.tokenExpiration.value = null;
    this.currentUsername.value = null;
    localStorage.removeItem("auth_token");
    localStorage.removeItem("auth_token_expiration");
    localStorage.removeItem("auth_username");
  }
}

// Axios interceptor to handle 401 errors
axios.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;
    const requestUrl = originalRequest?.url || "";
    const isAuthRequest = requestUrl.includes("/auth/token") || requestUrl.includes("/auth/");

    if (error.response?.status === 401 && !originalRequest._retry && !isAuthRequest) {
      originalRequest._retry = true;
      const isElectron = !!(window as unknown as { electronAPI?: unknown }).electronAPI;

      if (isElectron) {
        // In Electron mode, auto-refresh the token
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_token_expiration");

        const authInstance = new AuthService();
        await authInstance.initialize();
        const newToken = await authInstance.getToken();

        if (newToken) {
          originalRequest.headers["Authorization"] = `Bearer ${newToken}`;
          return axios(originalRequest);
        }
      } else {
        // In Docker mode, redirect to login
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_token_expiration");
        localStorage.removeItem("auth_username");

        if (!window.location.hash.includes("login")) {
          window.location.href = "/#/login";
        }
      }
    }

    return Promise.reject(error);
  },
);

export const authService = new AuthService();
export default authService;
