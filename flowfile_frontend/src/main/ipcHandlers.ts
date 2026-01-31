import { ipcMain, BrowserWindow, app } from "electron";
import { loadWindow } from "./windowLoader";
import { CORE_PORT, WORKER_PORT } from "./constants";

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
  ipcMain.handle("get-docker-status", () => appState.dockerStatus);
  ipcMain.handle("get-services-status", () => appState.servicesStatus);
  ipcMain.handle("get-app-version", () => app.getVersion());
  ipcMain.handle("get-service-ports", () => ({
    corePort: CORE_PORT,
    workerPort: WORKER_PORT,
  }));
}

export function setupWindowIpcHandlers(mainWindow: BrowserWindow): void {
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
  ipcMain.on("quit-app", () => {
    console.log("Received quit-app command, quitting...");
    quitCallback();
  });
}
