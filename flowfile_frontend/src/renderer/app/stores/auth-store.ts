import { defineStore } from "pinia";
import authService from "../services/auth.service";

export interface User {
  username: string;
  email?: string;
  full_name?: string;
  is_admin?: boolean;
  id?: number;
  must_change_password?: boolean;
}

interface AuthState {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
}

export const useAuthStore = defineStore("auth", {
  state: (): AuthState => ({
    user: null,
    isAuthenticated: false,
    isLoading: false,
    error: null,
  }),

  getters: {
    currentUser: (state): User | null => state.user,
    isLoggedIn: (state): boolean => state.isAuthenticated,
    authError: (state): string | null => state.error,
    isAdmin: (state): boolean => state.user?.is_admin ?? false,
    mustChangePassword: (state): boolean => state.user?.must_change_password ?? false,
  },

  actions: {
    async login(username: string, password: string): Promise<boolean> {
      this.isLoading = true;
      this.error = null;

      try {
        const success = await authService.login(username, password);

        if (success) {
          this.isAuthenticated = true;
          // Fetch full user info including is_admin
          const userInfo = await authService.getCurrentUser();
          if (userInfo) {
            this.user = userInfo;
          } else {
            this.user = { username };
          }
          return true;
        } else {
          this.error = "Invalid username or password";
          return false;
        }
      } catch (error) {
        this.error = error instanceof Error ? error.message : "Login failed";
        return false;
      } finally {
        this.isLoading = false;
      }
    },

    async initialize(): Promise<boolean> {
      this.isLoading = true;

      try {
        const authenticated = await authService.initialize();
        this.isAuthenticated = authenticated;

        if (authenticated) {
          // Try to get user info from the backend
          const userInfo = await authService.getCurrentUser();
          if (userInfo) {
            this.user = userInfo;
          }
        }

        return authenticated;
      } catch (error) {
        console.error("Failed to initialize auth:", error);
        this.isAuthenticated = false;
        return false;
      } finally {
        this.isLoading = false;
      }
    },

    logout() {
      authService.logout();
      this.user = null;
      this.isAuthenticated = false;
      this.error = null;
    },

    clearError() {
      this.error = null;
    },

    async refreshUserInfo() {
      const userInfo = await authService.getCurrentUser();
      if (userInfo) {
        this.user = userInfo;
      }
    },

    clearMustChangePassword() {
      if (this.user) {
        this.user.must_change_password = false;
      }
    },
  },
});
