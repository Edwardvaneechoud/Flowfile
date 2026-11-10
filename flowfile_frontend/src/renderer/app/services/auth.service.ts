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

/** How people sign in on this server: with a password, or through the hub it joined. */
export type SignInMethod = { method: "password" } | { method: "hub"; url: string; hub_url: string };

const HUB_SIGN_IN_FAILED =
  "Signing in through the hub did not work. Open this server from the hub again.";

// This tab's one-time state for the sign-in through the hub it started.
const HUB_SIGN_IN_STATE = "hub_signin_state";

// Set on a session that came through the hub, the only kind whose sign-out the server is told of.
const HUB_SESSION = "auth_hub_session";

/** 32 random bytes as base64url; `getRandomValues` works on a plain-http server too. */
function newSignInState(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

/** The server's own sentence for a refused request, or `fallback` when it sent none. */
function refusalSentence(error: unknown, fallback: string): string {
  const detail = axios.isAxiosError(error) ? error.response?.data?.detail : undefined;
  return typeof detail === "string" && detail ? detail : fallback;
}

class AuthService {
  private token = ref<string | null>(null);
  private tokenExpiration = ref<number | null>(null);
  private isDesktopMode = ref(false);
  private modeInitialized = false;
  private refreshPromise: Promise<string | null> | null = null;
  private currentUsername = ref<string | null>(null);
  // Held in memory only, from the fragment until `initialize` posts it.
  private hubSignInToken: string | null = null;
  private hubSignInError: string | null = null;

  constructor() {
    // Initial detection based on the Tauri runtime presence.
    this.isDesktopMode.value = this.detectDesktopMode();
    this.clearStoredTokens();
    this.loadStoredToken();
    this.takeTokenFromFragment();
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
        this.isDesktopMode.value = mode === "electron" || mode === "tauri" || mode === "desktop";
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

    const signInToken = this.hubSignInToken;
    this.hubSignInToken = null;
    if (signInToken !== null && !this.isDesktopMode.value) {
      return await this.signInThroughHub(signInToken);
    }

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

  /**
   * Keep a hub's one-time sign-in token, `#signin=<token>&state=<state>`, in memory
   * only and remove it from the URL; a fragment never reaches a server or its access
   * logs. `initialize` trades it for a session.
   *
   * The token is kept only when its state is the one this tab made when it started
   * the sign-in (`hubSignInUrl`), and that state is spent by any `#signin=` that
   * arrives. A token without it, or with another, is dropped and changes nothing:
   * a link carrying someone's sign-in token signs nobody in and nobody out.
   *
   * This runs when the service is constructed, at module load. The router imports
   * this module, so the fragment is read and removed before its hash history is
   * created; that history would otherwise keep it as the initial route.
   */
  private takeTokenFromFragment(): void {
    const fragment = new URLSearchParams(window.location.hash.slice(1));
    const signInToken = fragment.get("signin");
    if (signInToken === null) return;

    const { pathname, search } = window.location;
    window.history.replaceState(window.history.state, "", pathname + search);
    const expected = sessionStorage.getItem(HUB_SIGN_IN_STATE);
    sessionStorage.removeItem(HUB_SIGN_IN_STATE);
    if (signInToken && expected && fragment.get("state") === expected) {
      this.hubSignInToken = signInToken;
    }
  }

  /**
   * The hub's Open page for this server, `openUrl`, with a new one-time `state` that
   * this tab keeps in sessionStorage; the hub carries it back beside the sign-in token.
   */
  hubSignInUrl(openUrl: string): string {
    const state = newSignInState();
    sessionStorage.setItem(HUB_SIGN_IN_STATE, state);
    const url = new URL(openUrl, window.location.href);
    url.searchParams.set("state", state);
    return url.toString();
  }

  /** Start a sign-in through the hub: send this tab to the hub's Open page, replacing the entry so Back skips it. */
  startHubSignIn(openUrl: string): void {
    window.location.replace(this.hubSignInUrl(openUrl));
  }

  /**
   * Trade the hub's sign-in token for this server's session, stored as a password
   * sign-in stores it and marked as the hub's, expiring at the answer's `expires_at`.
   * The token is posted once, on a client without the interceptors that renew a
   * session or retry a refused request, since the hub takes it only once. Any
   * earlier session in this browser ends; a refusal keeps the server's sentence
   * for the sign-in page.
   */
  private async signInThroughHub(signInToken: string): Promise<boolean> {
    try {
      const response = await axios
        .create()
        .post<AuthResponse>("/auth/hub/signin", { token: signInToken });
      const accessToken = response.data?.access_token;
      const expiresAt = response.data?.expires_at;
      if (!accessToken || !expiresAt) throw new Error("The server answered no session.");
      this.logout();
      this.setToken(accessToken, expiresAt);
      if (response.data.refresh_token) {
        localStorage.setItem("auth_refresh_token", response.data.refresh_token);
      }
      localStorage.setItem(HUB_SESSION, "1");
      this.hubSignInError = null;
      return true;
    } catch (error) {
      this.logout();
      this.hubSignInError = refusalSentence(error, HUB_SIGN_IN_FAILED);
      return false;
    }
  }

  /** The server's sentence for a hub sign-in it refused at this load, given once. */
  takeHubSignInError(): string | null {
    const sentence = this.hubSignInError;
    this.hubSignInError = null;
    return sentence;
  }

  /** How this server signs people in; a password form when it cannot be asked. */
  async getSignInMethod(): Promise<SignInMethod> {
    try {
      const response = await axios.get<SignInMethod>("/auth/sign_in_method", {
        headers: { "X-Skip-Auth-Header": "true" },
      });
      const answer = response.data;
      if (answer?.method === "hub" && typeof answer.url === "string" && answer.url) return answer;
    } catch (error) {
      console.error("Could not read how this server signs people in:", error);
    }
    return { method: "password" };
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

  /**
   * Sign out: a session that came through the hub tells the server first, so the
   * sign-in ends at the hub too; then this browser's session is cleared whatever
   * the server answered. Any other session is cleared here only. The requests go
   * on a client without the interceptors, which would renew the session or send
   * the browser to the sign-in page on a refusal.
   */
  async signOut(): Promise<void> {
    if (localStorage.getItem(HUB_SESSION) === null) {
      this.logout();
      return;
    }
    const client = axios.create();
    try {
      let token = this.hasValidToken() ? this.token.value : null;
      const refreshToken = localStorage.getItem("auth_refresh_token");
      if (!token && refreshToken && !this.isDesktopMode.value) {
        const formData = new FormData();
        formData.append("refresh_token", refreshToken);
        const response = await client.post<AuthResponse>("/auth/refresh", formData);
        token = response.data?.access_token ?? null;
      }
      if (token) {
        await client.post("/auth/logout", null, { headers: { Authorization: `Bearer ${token}` } });
      }
    } catch (error) {
      console.error("The server was not told of the sign-out:", error);
    } finally {
      this.logout();
    }
  }

  logout(): void {
    this.token.value = null;
    this.tokenExpiration.value = null;
    this.currentUsername.value = null;
    localStorage.removeItem("auth_token");
    localStorage.removeItem("auth_token_expiration");
    localStorage.removeItem("auth_refresh_token");
    localStorage.removeItem("auth_username");
    localStorage.removeItem(HUB_SESSION);
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
        localStorage.removeItem(HUB_SESSION);

        if (!window.location.hash.includes("login")) {
          window.location.href = "/#/login";
        }
      }
    }

    return Promise.reject(error);
  },
);

export default authService;
