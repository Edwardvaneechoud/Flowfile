// main.ts
import { app, ipcMain, BrowserWindow, Menu, MenuItemConstructorOptions, shell } from "electron";
import { exec } from "child_process";
import { setupLogging } from "./logger";
import { startServices, cleanupProcesses, setupProcessMonitoring } from "./services";
import { createWindow, getMainWindow, createLoadingWindow } from "./windowManager";
import { modifySessionHeaders } from "./session";
import { setupAppEventListeners } from "./appEvents";
import { loadWindow } from "./windowLoader";
import { platform } from "os";

// Global variables to store status for IPC access
let globalDockerStatus = { isAvailable: false, error: null as string | null };
let globalServicesStatus = { status: "not_started", error: null as string | null };

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
          error: "Docker is not available. Some features may have limited functionality.",
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

function setupCustomMenu(mainWindow: BrowserWindow): void {
  // Create refresh handler function
  const refreshHandler = async (): Promise<void> => {
    try {
      await mainWindow.webContents.session.clearCache();
      loadWindow(mainWindow);
    } catch (error) {
      console.error("Failed to clear cache:", error);
    }
  };

  // Create the menu template with standard items
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
          label: "Learn More",
          click: async () => {
            await shell.openExternal("https://electronjs.org");
          },
        },
      ],
    },
  ];

  // Add macOS-specific menu items
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

  // Setup IPC handlers for testing
  ipcMain.handle("get-docker-status", () => globalDockerStatus);
  ipcMain.handle("get-services-status", () => globalServicesStatus);

  setupAppEventListeners();

  try {
    const loadingWin = createLoadingWindow();

    const dockerStatusResult = await checkDocker();
    console.log("Docker status:", dockerStatusResult);

    globalDockerStatus = dockerStatusResult;

    // Update loading window with Docker status
    loadingWin?.webContents.send("update-docker-status", dockerStatusResult);

    // Start services and update status
    try {
      const startingStatus = { status: "starting", error: null };
      globalServicesStatus = startingStatus;

      loadingWin?.webContents.send("update-services-status", startingStatus);

      await startServices();

      const readyStatus = { status: "ready", error: null };
      globalServicesStatus = readyStatus;

      loadingWin?.webContents.send("update-services-status", readyStatus);

      console.log("All services started successfully");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Failed to start services";
      const errorStatus = {
        status: "error",
        error: errorMessage,
      };

      globalServicesStatus = errorStatus;
      loadingWin?.webContents.send("update-services-status", errorStatus);

      throw error;
    }

    // Create main window only after services are ready
    createWindow();
    modifySessionHeaders();
    setupProcessMonitoring();

    const mainWindow = getMainWindow();
    if (mainWindow) {
      mainWindow.webContents.once("did-finish-load", () => {
        console.log("Electron app startup successful, sending signal...");
        mainWindow.webContents.send("startup-success");
      });

      // Setup menu with custom refresh handler
      setupCustomMenu(mainWindow);

      // Also handle the refresh event via IPC for custom implementations
      ipcMain.on("app-refresh", async () => {
        try {
          await mainWindow.webContents.session.clearCache();
          loadWindow(mainWindow);
        } catch (error) {
          console.error("Failed to clear cache:", error);
        }
      });
    }
  } catch (error) {
    console.error("Fatal error starting services:", error);
    await cleanupProcesses();
    app.quit();
  }

  ipcMain.on("quit-app", () => {
    console.log("Received quit-app command, quitting...");
    app.quit();
  });

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});
