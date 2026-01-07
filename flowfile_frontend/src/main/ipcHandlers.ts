// ipcHandlers.ts
import { ipcMain, BrowserWindow } from "electron";
import { loadWindow } from "./windowLoader";

// State management for IPC responses
interface AppState {
  dockerStatus: { isAvailable: boolean; error: string | null };
  servicesStatus: { status: string; error: string | null };
}

const appState: AppState = {
  dockerStatus: { isAvailable: false, error: null },
  servicesStatus: { status: "not_started", error: null },
};

export function getAppState(): AppState {
  return appState;
}

export function updateDockerStatus(status: { isAvailable: boolean; error: string | null }): void {
  appState.dockerStatus = status;
}

export function updateServicesStatus(status: { status: string; error: string | null }): void {
  appState.servicesStatus = status;
}

export function setupIpcHandlers(): void {
  // Query handlers - respond to renderer requests
  ipcMain.handle("get-docker-status", () => appState.dockerStatus);
  ipcMain.handle("get-services-status", () => appState.servicesStatus);
}

export function setupWindowIpcHandlers(mainWindow: BrowserWindow): void {
  // Window-specific handlers
  ipcMain.on("app-refresh", async () => {
    try {
      await mainWindow.webContents.session.clearCache();
      loadWindow(mainWindow);
    } catch (error) {
      console.error("Failed to clear cache:", error);
    }
  });
}

export function setupAppIpcHandlers(quitCallback: () => void): void {
  // App-level handlers
  ipcMain.on("quit-app", () => {
    console.log("Received quit-app command, quitting...");
    quitCallback();
  });
}
