interface ElectronAPI {
    sendMessage: (message: string) => void;
    getDockerStatus: () => Promise<any>;
    getServicesStatus: () => Promise<any>;
    onStartupSuccess: (callback: () => void) => () => void;
    onDockerStatusUpdate: (callback: (status: any) => void) => () => void;
    onServicesStatusUpdate: (callback: (status: any) => void) => () => void;
  }
  
  declare global {
    interface Window {
      electronAPI: ElectronAPI;
    }
  }
  
  export {};
  