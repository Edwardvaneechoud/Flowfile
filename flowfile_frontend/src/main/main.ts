import { app, BrowserWindow, Menu, MenuItemConstructorOptions, shell } from "electron";
import { setupLogging } from "./logger";
import { startServices, cleanupProcesses, setupProcessMonitoring } from "./services";
import { createWindow, getMainWindow, createLoadingWindow } from "./windowManager";
import { modifySessionHeaders } from "./session";
import { setupAppEventListeners } from "./appEvents";
import { loadWindow } from "./windowLoader";
import {
  setupIpcHandlers,
  setupWindowIpcHandlers,
  setupAppIpcHandlers,
  updateServicesStatus,
} from "./ipcHandlers";

function setupCustomMenu(mainWindow: BrowserWindow): void {
  const refreshHandler = async (): Promise<void> => {
    try {
      await mainWindow.webContents.session.clearCache();
      loadWindow(mainWindow);
    } catch (error) {
      console.error("Failed to clear cache:", error);
    }
  };

  const template: MenuItemConstructorOptions[] = [
    {
      label: "File",
      submenu: [{ role: "close" }],
    },
    {
      label: "Edit",
      submenu: [
        { role: "undo" },
        { role: "redo" },
        { type: "separator" },
        { role: "cut" },
        { role: "copy" },
        { role: "paste" },
        { role: "selectAll" },
      ],
    },
    {
      label: "View",
      submenu: [
        {
          label: "Refresh",
          accelerator: "CommandOrControl+R",
          click: refreshHandler,
        },
        { type: "separator" },
        { role: "toggleDevTools" },
        { type: "separator" },
        { role: "resetZoom" },
        { role: "zoomIn" },
        { role: "zoomOut" },
        { type: "separator" },
        { role: "togglefullscreen" },
      ],
    },
    {
      label: "Window",
      submenu: [{ role: "minimize" }, { role: "zoom" }],
    },
    {
      role: "help",
      submenu: [
        {
          label: "Documentation",
          click: async () => {
            await shell.openExternal("https://github.com/Edwardvaneechoud/Flowfile#readme");
          },
        },
        {
          label: "Report an Issue",
          click: async () => {
            await shell.openExternal("https://github.com/Edwardvaneechoud/Flowfile/issues");
          },
        },
        { type: "separator" },
        {
          label: "View on GitHub",
          click: async () => {
            await shell.openExternal("https://github.com/Edwardvaneechoud/Flowfile");
          },
        },
      ],
    },
  ];

  if (process.platform === "darwin") {
    template.unshift({
      label: app.name,
      submenu: [
        { role: "about" },
        { type: "separator" },
        { role: "services" },
        { type: "separator" },
        { role: "hide" },
        { role: "hideOthers" },
        { role: "unhide" },
        { type: "separator" },
        { role: "quit" },
      ],
    });
  }

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

app.whenReady().then(async () => {
  const logFile = setupLogging();
  console.log("Logging to:", logFile);
  console.log("Running the app in:", process.env.NODE_ENV);

  setupIpcHandlers();
  setupAppIpcHandlers(() => app.quit());
  setupAppEventListeners();

  try {
    const loadingWin = createLoadingWindow();

    try {
      const startingStatus = { status: "starting", error: null };
      updateServicesStatus(startingStatus);

      loadingWin?.webContents.send("update-services-status", startingStatus);

      await startServices();

      const readyStatus = { status: "ready", error: null };
      updateServicesStatus(readyStatus);

      loadingWin?.webContents.send("update-services-status", readyStatus);

      console.log("All services started successfully");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Failed to start services";
      const errorStatus = {
        status: "error",
        error: errorMessage,
      };

      updateServicesStatus(errorStatus);
      loadingWin?.webContents.send("update-services-status", errorStatus);

      throw error;
    }

    createWindow();
    modifySessionHeaders();
    setupProcessMonitoring();

    const mainWindow = getMainWindow();
    if (mainWindow) {
      setupWindowIpcHandlers(mainWindow);
      mainWindow.webContents.once("did-finish-load", () => {
        console.log("Electron app startup successful, sending signal...");
        mainWindow.webContents.send("startup-success");
      });
      setupCustomMenu(mainWindow);
    }
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
