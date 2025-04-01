// src/app/services/axios-setup.ts
import axios, { AxiosRequestConfig, AxiosResponse, AxiosError } from 'axios';
import authService from './auth.service';
import { flowfileCorebaseURL } from '../../config/constants';

axios.defaults.baseURL = flowfileCorebaseURL;
axios.defaults.withCredentials = true;

// Add auth token to all requests
axios.interceptors.request.use(
  async (config: AxiosRequestConfig): Promise<AxiosRequestConfig> => {
    if (config.headers && config.headers['X-Skip-Auth-Header']) {
      delete config.headers['X-Skip-Auth-Header'];
      return config;
    }

    try {
      const token = await authService.getToken();
      
      if (token) {
        config.headers = config.headers || {};
        config.headers.Authorization = `Bearer ${token}`;
      }
      
      return config;
    } catch (error) {
      console.error('Error in request interceptor:', error);
      return config;
    }
  },
  (error: AxiosError) => {
    return Promise.reject(error);
  }
);

// Handle auth errors and refresh token if needed
axios.interceptors.response.use(
  (response: AxiosResponse) => {
    return response;
  },
  async (error: AxiosError) => {
    const originalRequest = error.config as AxiosRequestConfig & { _retry?: boolean };
    
    // If 401 Unauthorized and not retried yet
    if (
      error.response?.status === 401 &&
      !originalRequest._retry
    ) {
      originalRequest._retry = true;
      
      try {
        await authService.getToken();
        return axios(originalRequest);
      } catch (refreshError) {
        console.error('Token refresh failed:', refreshError);
        authService.logout();
        return Promise.reject(error);
      }
    }
    
    return Promise.reject(error);
  }
);

export default axios;