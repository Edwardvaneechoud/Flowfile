/**
 * Should match main/preload.ts for typescript support in renderer
 */
export default interface ElectronAPI {
  getServicesStatus: () => Promise<{ status: string; error: string | null }>;
  getDockerStatus: () => Promise<{ isAvailable: boolean; error: string | null }>;
  onStartupSuccess: (callback: () => void) => () => void;
  quitApp: () => void;
  sendMessage: (message: string) => void;
  onDockerStatusUpdate: (callback: (status: any) => void) => () => void;
  onServicesStatusUpdate: (callback: (status: any) => void) => () => void;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}
