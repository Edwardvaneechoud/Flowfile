import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("electronAPI", {
  sendMessage: (message: string) => ipcRenderer.send("message", message),
  getServicesStatus: () => ipcRenderer.invoke("get-services-status"),
  getAppVersion: () => ipcRenderer.invoke("get-app-version"),
  onStartupSuccess: (callback: () => void) => {
    const listener = () => callback();
    ipcRenderer.on("startup-success", listener);
    return () => {
      ipcRenderer.removeListener("startup-success", listener);
    };
  },
  onServicesStatusUpdate: (callback: (status: any) => void) => {
    const listener = (_: any, status: any) => callback(status);
    ipcRenderer.on("update-services-status", listener);
    return () => {
      ipcRenderer.removeListener("update-services-status", listener);
    };
  },
  quitApp: () => ipcRenderer.send("quit-app"),
});
