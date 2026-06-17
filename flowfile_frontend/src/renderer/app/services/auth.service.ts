// src/app/services/auth.service.ts
import axios from "axios";
import { ref } from "vue";
import { isDesktop } from "../../lib/desktop";

interface AuthResponse {
  access_token: string;
  refresh_token?: string;
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
  private isDesktopMode = ref(false);
  private modeInitialized = false;
  private refreshPromise: Promise<string | null> | null = null;
  private currentUsername = ref<string | null>(null);

  constructor() {
    // Initial detection based on the Tauri runtime presence.
    this.isDesktopMode.value = this.detectDesktopMode();
    this.clearStoredTokens();
    this.loadStoredToken();
  }

  /**
   * Update desktop mode based on backend status.
   * Backend reports `mode` from FLOWFILE_MODE env. We treat both legacy
   * "electron" and current "tauri"/"desktop" as desktop mode, so
   * `flowfile run ui` (web shell, backend reports "electron") still
   * skips the setup wizard and auto-authenticates.
   */
  setModeFromBackend(mode: string): void {
    if (!this.modeInitialized) {
      if (!this.detectDesktopMode()) {
        this.isDesktopMode.value =
          mode === "electron" || mode === "tauri" || mode === "desktop";
      }
      this.modeInitialized = true;
    }
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

  private detectDesktopMode(): boolean {
    // True iff running inside the Tauri shell (or any future desktop shell
    // that exposes the same `__TAURI_INTERNALS__` contract via `isDesktop`).
    return isDesktop;
  }

  isInDesktopMode(): boolean {
    return this.isDesktopMode.value;
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

    // In desktop mode, auto-authenticate without credentials.
    if (this.isDesktopMode.value) {
      return await this.getDesktopToken();
    }

    // In Docker/web mode, require manual login.
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
        if (response.data.refresh_token) {
          localStorage.setItem("auth_refresh_token", response.data.refresh_token);
        }
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
    return !this.isDesktopMode.value && !this.hasValidToken();
  }

  private async getDesktopToken(): Promise<boolean> {
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
      console.error("Failed to get desktop token:", error);
      return false;
    }
  }

  async getToken(): Promise<string | null> {
    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    if (this.hasValidToken()) {
      // Proactively refresh if token expires within 5 minutes (Docker mode only).
      if (!this.isDesktopMode.value && this.isTokenExpiringSoon() && !this.refreshPromise) {
        this.refreshPromise = this.refreshAccessToken();
        this.refreshPromise.finally(() => {
          this.refreshPromise = null;
        });
      }
      return this.token.value;
    }

    // In Docker mode, try refresh token before requiring manual login.
    if (!this.isDesktopMode.value) {
      this.refreshPromise = this.refreshAccessToken();
      const newToken = await this.refreshPromise;
      this.refreshPromise = null;
      return newToken;
    }

    // In desktop mode, refresh the token automatically.
    this.refreshPromise = this.refreshToken();
    const newToken = await this.refreshPromise;
    this.refreshPromise = null;
    return newToken;
  }

  private isTokenExpiringSoon(): boolean {
    if (!this.tokenExpiration.value) return false;
    const FIVE_MINUTES = 5 * 60 * 1000;
    return this.tokenExpiration.value - Date.now() < FIVE_MINUTES;
  }

  async refreshAccessToken(): Promise<string | null> {
    const refreshToken = localStorage.getItem("auth_refresh_token");
    if (!refreshToken) return null;

    try {
      const formData = new FormData();
      formData.append("refresh_token", refreshToken);

      const response = await axios.post<AuthResponse>("/auth/refresh", formData, {
        headers: {
          "X-Skip-Auth-Header": "true",
          "Content-Type": "multipart/form-data",
        },
      });

      if (response.data?.access_token) {
        const expirationTime = response.data.expires_at || this.calculateExpiration();
        this.setToken(response.data.access_token, expirationTime);
        if (response.data.refresh_token) {
          localStorage.setItem("auth_refresh_token", response.data.refresh_token);
        }
        return response.data.access_token;
      }
      return null;
    } catch {
      // Refresh token expired or invalid — must re-login.
      this.logout();
      return null;
    }
  }

  private async refreshToken(): Promise<string | null> {
    if (!this.isDesktopMode.value) {
      return null;
    }

    const success = await this.getDesktopToken();
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
    localStorage.removeItem("auth_refresh_token");
    localStorage.removeItem("auth_username");
  }
}

// Create the singleton instance.
export const authService = new AuthService();

// Axios interceptor to handle 401 errors.
// Note: This uses the authService singleton which has the correct mode from backend.
axios.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;
    const requestUrl = originalRequest?.url || "";
    const isAuthRequest =
      requestUrl.includes("/auth/token") || requestUrl.includes("/auth/refresh");

    if (error.response?.status === 401 && !originalRequest._retry && !isAuthRequest) {
      originalRequest._retry = true;

      if (authService.isInDesktopMode()) {
        // In desktop mode (or "flowfile run ui" mode), auto-refresh the token.
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_token_expiration");

        await authService.initialize();
        const newToken = await authService.getToken();

        if (newToken) {
          originalRequest.headers["Authorization"] = `Bearer ${newToken}`;
          return axios(originalRequest);
        }
      } else {
        // In Docker mode, try refresh token before redirecting to login.
        const newToken = await authService.refreshAccessToken();
        if (newToken) {
          originalRequest.headers["Authorization"] = `Bearer ${newToken}`;
          return axios(originalRequest);
        }

        // Refresh failed — redirect to login.
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_token_expiration");
        localStorage.removeItem("auth_refresh_token");
        localStorage.removeItem("auth_username");

        if (!window.location.hash.includes("login")) {
          window.location.href = "/#/login";
        }
      }
    }

    return Promise.reject(error);
  },
);

export default authService;
