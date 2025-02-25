

import { contextBridge, ipcRenderer } from "electron";

console.log("Preload script loaded");


contextBridge.exposeInMainWorld("electronAPI", {
  sendMessage: (message: string) => ipcRenderer.send("message", message),
  getDockerStatus: () => ipcRenderer.invoke("get-docker-status"),
  getServicesStatus: () => ipcRenderer.invoke("get-services-status"),
  onStartupSuccess: (callback: () => void) => {
    const listener = (_event: any) => callback();
    ipcRenderer.on("startup-success", listener);
    return () => {
      ipcRenderer.removeListener("startup-success", listener);
    };
  },
  onDockerStatusUpdate: (callback: (status: any) => void) => {
    const listener = (_event: any, status: any) => callback(status);
    ipcRenderer.on("update-docker-status", listener);
    return () => {
      ipcRenderer.removeListener("update-docker-status", listener);
    };
  },
  onServicesStatusUpdate: (callback: (status: any) => void) => {
    const listener = (_event: any, status: any) => callback(status);
    ipcRenderer.on("update-services-status", listener);
    return () => {
      ipcRenderer.removeListener("update-services-status", listener);
    };
  },
  quitApp: () => ipcRenderer.send("quit-app")
});
