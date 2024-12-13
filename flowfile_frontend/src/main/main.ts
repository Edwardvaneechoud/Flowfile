// main.ts
import { app, ipcMain, globalShortcut, BrowserWindow, dialog } from "electron";
import { exec } from "child_process";
import { setupLogging } from "./logger";
import {
  startServices,
  cleanupProcesses,
  setupProcessMonitoring,
} from "./services";
import {
  createWindow,
  getMainWindow,
  createLoadingWindow,
} from "./windowManager";
import { modifySessionHeaders } from "./session";
import { setupAppEventListeners } from "./appEvents";
import { loadWindow } from "./windowLoader";
import { platform } from "os";

async function checkDocker(): Promise<{
  isAvailable: boolean;
  error: string | null;
}> {
  const isWindows = platform() === "win32";

  return new Promise((resolve) => {
    const checkCommand = isWindows
      ? "docker info"
      : `${"/Applications/Docker.app/Contents/Resources/bin/docker"} info`;

    // Windows doesn't need process check
    if (isWindows) {
      exec(checkCommand, (error) => {
        resolve({
          isAvailable: !error,
          error: error ? "Docker service is currently unreachable" : null,
        });
      });
      return;
    }

    // Mac-specific checks
    exec('ps aux | grep -v grep | grep "Docker.app"', (error, stdout) => {
      if (!stdout) {
        resolve({
          isAvailable: false,
          error:
            "Docker is not available. Some features may have limited functionality.",
        });
        return;
      }

      exec(checkCommand, (error) => {
        resolve({
          isAvailable: !error,
          error: error ? "Docker service is currently unreachable" : null,
        });
      });
    });
  });
}

app.whenReady().then(async () => {
  const logFile = setupLogging();
  console.log("Logging to:", logFile);
  console.log("Running the app in:", process.env.NODE_ENV);

  setupAppEventListeners();

  try {
    // Create loading window first
    const loadingWin = createLoadingWindow();

    // Check Docker status
    const dockerStatus = await checkDocker();
    console.log("Docker status:", dockerStatus);

    // Update loading window with Docker status
    loadingWin?.webContents.send("update-docker-status", {
      isAvailable: dockerStatus.isAvailable,
      error: dockerStatus.error,
    });

    // Start services and update status
    try {
      loadingWin?.webContents.send("update-services-status", {
        status: "starting",
      });

      await startServices();

      loadingWin?.webContents.send("update-services-status", {
        status: "ready",
      });

      console.log("All services started successfully");
    } catch (error) {
      loadingWin?.webContents.send("update-services-status", {
        status: "error",
        error: "Failed to start services",
      });
      throw error;
    }

    // Create main window only after services are ready
    createWindow();
    modifySessionHeaders();
    setupProcessMonitoring();

    // Register shortcuts
    globalShortcut.register("CommandOrControl+R", async () => {
      const mainWindow = getMainWindow();
      if (mainWindow) {
        try {
          await mainWindow.webContents.session.clearCache();
          loadWindow(mainWindow);
        } catch (error) {
          console.error("Failed to clear cache:", error);
        }
      }
    });
  } catch (error) {
    console.error("Fatal error starting services:", error);
    await cleanupProcesses();
    app.quit();
  }

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});
