// src/app/services/axios-setup.ts
import axios, { InternalAxiosRequestConfig, AxiosResponse, AxiosError } from "axios";
import authService from "./auth.service";
import { createMutationQueue, installMutationChannel } from "./mutationChannel";
import { useFlowStore } from "../stores/flow-store";
import { flowfileCorebaseURL } from "../../config/constants";

axios.defaults.baseURL = flowfileCorebaseURL;
axios.defaults.withCredentials = true;

axios.interceptors.request.use(
  async (config: InternalAxiosRequestConfig): Promise<InternalAxiosRequestConfig> => {
    if (config.headers && config.headers["X-Skip-Auth-Header"]) {
      delete config.headers["X-Skip-Auth-Header"];
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
      console.error("Error in request interceptor:", error);
      return config;
    }
  },
  (error: AxiosError) => {
    return Promise.reject(error);
  },
);

axios.interceptors.response.use(
  (response: AxiosResponse) => {
    return response;
  },
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean };

    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        await authService.getToken();
        return axios(originalRequest);
      } catch (refreshError) {
        console.error("Token refresh failed:", refreshError);
        authService.logout();
        return Promise.reject(error);
      }
    }

    return Promise.reject(error);
  },
);

// Registered after the auth interceptors on purpose; see installMutationChannel.
const mutationQueue = createMutationQueue();
// The store is resolved per response: this module and the stores import each other.
installMutationChannel(
  axios,
  mutationQueue,
  (history) => useFlowStore().updateHistoryState(history),
  () => useFlowStore().requestReload(),
);

/** Resolves once every graph mutation enqueued so far has completed. */
export const whenMutationsIdle = (): Promise<void> => mutationQueue.whenIdle();

/** Bumped each time a graph mutation is enqueued; lets a reload detect it was overtaken. */
export const mutationGeneration = (): number => mutationQueue.generation();

/**
 * Take a slot now for a request that is sent later (pass it as `mutationSlot`), so the
 * gesture keeps its place in the order. A slot that ends up unused must be released.
 */
export const reserveMutationSlot = (): number => mutationQueue.enqueue();

export const releaseMutationSlot = (slot: number): void => {
  mutationQueue.release(slot);
};

export default axios;
