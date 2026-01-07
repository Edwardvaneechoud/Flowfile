/**
 * Type definitions for the Electron preload API
 * Should match main/preload.ts for typescript support in renderer
 */

export interface DockerStatus {
  isAvailable: boolean;
  error: string | null;
}

export interface ServicesStatus {
  status: "not_started" | "starting" | "ready" | "error";
  error: string | null;
}

export default interface ElectronAPI {
  /** Send a message to the main process */
  sendMessage: (message: string) => void;

  /** Get the current Docker availability status */
  getDockerStatus: () => Promise<DockerStatus>;

  /** Get the current services status */
  getServicesStatus: () => Promise<ServicesStatus>;

  /** Register a callback for startup success event. Returns cleanup function. */
  onStartupSuccess: (callback: () => void) => () => void;

  /** Register a callback for Docker status updates. Returns cleanup function. */
  onDockerStatusUpdate: (callback: (status: DockerStatus) => void) => () => void;

  /** Register a callback for services status updates. Returns cleanup function. */
  onServicesStatusUpdate: (callback: (status: ServicesStatus) => void) => () => void;

  /** Request the app to quit gracefully */
  quitApp: () => void;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}
