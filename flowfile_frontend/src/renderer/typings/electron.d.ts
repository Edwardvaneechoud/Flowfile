export interface DockerStatus {
  isAvailable: boolean;
  error: string | null;
}

export interface ServicesStatus {
  status: "not_started" | "starting" | "ready" | "error";
  error: string | null;
}

export default interface ElectronAPI {
  sendMessage: (message: string) => void;
  getDockerStatus: () => Promise<DockerStatus>;
  getServicesStatus: () => Promise<ServicesStatus>;
  getAppVersion: () => Promise<string>;
  onStartupSuccess: (callback: () => void) => () => void;
  onDockerStatusUpdate: (callback: (status: DockerStatus) => void) => () => void;
  onServicesStatusUpdate: (callback: (status: ServicesStatus) => void) => () => void;
  quitApp: () => void;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}
