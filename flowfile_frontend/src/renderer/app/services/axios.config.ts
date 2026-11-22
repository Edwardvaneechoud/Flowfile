// src/app/services/axios-setup.ts
import axios, { InternalAxiosRequestConfig, AxiosError } from "axios";
import authService from "./auth.service";
import { createMutationQueue, installMutationChannel } from "./mutationChannel";
import { CLIENT_HEADER, clientId } from "./clientId";
import { useFlowStore } from "../stores/flow-store";
import { flowfileCorebaseURL } from "../../config/constants";

axios.defaults.baseURL = flowfileCorebaseURL;
axios.defaults.withCredentials = true;

axios.interceptors.request.use(
  async (config: InternalAxiosRequestConfig): Promise<InternalAxiosRequestConfig> => {
    // Core echoes it on the change feed, so this window can skip the events it caused.
    if (config.headers) config.headers[CLIENT_HEADER] = clientId;
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

// A 401 is answered by the auth service's own response interceptor, registered when it is imported above.

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
