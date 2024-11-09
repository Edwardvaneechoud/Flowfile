// main.ts
import { app, ipcMain, globalShortcut, BrowserWindow } from "electron";
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

app.whenReady().then(async () => {
  const logFile = setupLogging();
  console.log("Logging to:", logFile);
  console.log("App is ready");
  console.log("Running the app in:", process.env.NODE_ENV);

  setupAppEventListeners();

  try {
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
