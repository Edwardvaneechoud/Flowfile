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
    this.isElectronMode.value = !!(
        (window as any)?.process?.type === 'renderer' || 
        (window?.navigator && /electron/i.test(window.navigator.userAgent))
      );
    const savedToken = localStorage.getItem('auth_token');
    const savedExpiration = localStorage.getItem('auth_token_expiration');
    
    if (savedToken) {
      this.token.value = savedToken;
      this.tokenExpiration.value = savedExpiration ? parseInt(savedExpiration, 10) : null;
    }
  }

  async initialize(): Promise<boolean> {
    console.log(this.hasValidToken, 'has Token')
    if (this.hasValidToken()) {
      return true;
    }
    
    if (this.isElectronMode.value) {
      return await this.getElectronToken();
    }
    
    return false;
  }
  
  private async getElectronToken(): Promise<boolean> {
    try {
        console.log("getting token")

      const response = await axios.post<AuthResponse>('/auth/token', {}, {
        headers: { 'X-Skip-Auth-Header': 'true' }
      });
      if (response.data && response.data.access_token) {
        this.setToken(
          response.data.access_token,
          response.data.expires_at || this.calculateExpiration()
        );
        console.log("got token")
        console.log(response)
        return true;
        
      }
      return false;
    } catch (error) {
      console.error('Failed to get token:', error);
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
    
    this.refreshPromise = this.refreshToken();
    const newToken = await this.refreshPromise;
    this.refreshPromise = null;
    return newToken;
  }
  
  private async refreshToken(): Promise<string | null> {
    if (this.isElectronMode.value) {
      const success = await this.getElectronToken();
      return success ? this.token.value : null;
    }
    
    return null;
  }
  
  private setToken(token: string, expiration: number): void {
    this.token.value = token;
    this.tokenExpiration.value = expiration;
    
    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_token_expiration', expiration.toString());
  }
  
  private calculateExpiration(hoursTilExpire = 1): number {
    return Date.now() + hoursTilExpire * 59;
  }
  
  hasValidToken(): boolean {
    console.log('token',this.token.value)
    console.log('tokenExpiration',this.tokenExpiration.value)
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
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_token_expiration');
  }
}

export const authService = new AuthService();
export default authService;