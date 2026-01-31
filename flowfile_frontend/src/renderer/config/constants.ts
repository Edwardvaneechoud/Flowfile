import axios from "axios";

// Default core API URL (used in non-Electron environments and as initial fallback)
const DEFAULT_CORE_PORT = 63578;
export let flowfileCorebaseURL = `http://localhost:${DEFAULT_CORE_PORT}/`;

/**
 * Initializes the service configuration by querying the Electron main process
 * for the actual service ports. In non-Electron environments (e.g., browser/Docker),
 * the default URL is kept.
 */
export async function initializeServiceConfig(): Promise<void> {
  if (typeof window !== "undefined" && window.electronAPI?.getServicePorts) {
    try {
      const ports = await window.electronAPI.getServicePorts();
      flowfileCorebaseURL = `http://localhost:${ports.corePort}/`;
      axios.defaults.baseURL = flowfileCorebaseURL;
      console.log(`Service config initialized - Core API: ${flowfileCorebaseURL}`);
    } catch (error) {
      console.warn("Failed to get service ports from Electron, using defaults:", error);
    }
  }
}
