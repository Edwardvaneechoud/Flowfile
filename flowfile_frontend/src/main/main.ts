// main.ts
import { app, ipcMain, globalShortcut, BrowserWindow, dialog } from "electron";
import { exec } from "child_process";
import { setupLogging } from "./logger";
import {
  startServices,
  cleanupProcesses,
  setupProcessMonitoring,
} from "./services";
import { createWindow, getMainWindow } from "./windowManager";
import { modifySessionHeaders } from "./session";
import { setupAppEventListeners } from "./appEvents";
import { openAuthWindow } from "./windowManager";
import { loadWindow } from "./windowLoader";

async function checkDocker(): Promise<{
  isAvailable: boolean;
  error: string | null;
}> {
  return new Promise((resolve) => {
    // Use absolute path to Docker
    const dockerPath = "/Applications/Docker.app/Contents/Resources/bin/docker";

    // First check if Docker is running
    exec('ps aux | grep -v grep | grep "Docker.app"', async (error, stdout) => {
      if (!stdout) {
        resolve({
          isAvailable: false,
          error: "Docker Desktop is not running. Please start Docker Desktop.",
        });
        return;
      }

      // Then check if Docker daemon is responsive
      exec(`${dockerPath} info`, (error, stdout, stderr) => {
        if (error) {
          console.error("Docker check error:", error);
          console.error("Docker check stderr:", stderr);
          resolve({
            isAvailable: false,
            error: stderr || error.message,
          });
        } else {
          console.log("Docker check stdout:", stdout);
          resolve({
            isAvailable: true,
            error: null,
          });
        }
      });
    });
  });
}

async function showDockerError(error: string) {
  const mainWindow = getMainWindow();
  if (mainWindow) {
    await dialog.showMessageBox(mainWindow, {
      type: "error",
      title: "Docker Error",
      message: "Docker is not available",
      detail: `Please ensure Docker Desktop is running and accessible.\n\nError details: ${error}`,
      buttons: ["OK"],
    });
  }
}

app.whenReady().then(async () => {
  const logFile = setupLogging();
  console.log("Logging to:", logFile);
  console.log("Running the app in:", process.env.NODE_ENV);

  setupAppEventListeners();

  try {
    const dockerStatus = await checkDocker();
    console.log("Docker status:", dockerStatus);

    // Create window regardless of Docker status
    createWindow();

    if (!dockerStatus.isAvailable) {
      await showDockerError(dockerStatus.error || "Unknown error");
    }

    await startServices();
    console.log("All services started successfully");
    createWindow();
    modifySessionHeaders();
    setupProcessMonitoring();

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

// IPC handlers
ipcMain.on("open-auth-window", () => {
  openAuthWindow();
});

// Error handling for uncaught exceptions
process.on("uncaughtException", async (error) => {
  console.error("Uncaught exception:", error);
  await cleanupProcesses();
  app.quit();
});

process.on("unhandledRejection", async (error) => {
  console.error("Unhandled rejection:", error);
  await cleanupProcesses();
  app.quit();
});
