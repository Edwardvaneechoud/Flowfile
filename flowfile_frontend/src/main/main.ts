// main.ts
import { app, BrowserWindow, Menu, MenuItemConstructorOptions, shell } from "electron";
import { exec } from "child_process";
import { setupLogging } from "./logger";
import { startServices, cleanupProcesses, setupProcessMonitoring } from "./services";
import { createWindow, getMainWindow, createLoadingWindow } from "./windowManager";
import { modifySessionHeaders } from "./session";
import { setupAppEventListeners } from "./appEvents";
import { loadWindow } from "./windowLoader";
import { platform } from "os";
import {
  setupIpcHandlers,
  setupWindowIpcHandlers,
  setupAppIpcHandlers,
  updateDockerStatus,
  updateServicesStatus,
  getAppState,
} from "./ipcHandlers";

async function checkDocker(): Promise<{
  isAvailable: boolean;
  error: string | null;
}> {
  const currentPlatform = platform();
  const isWindows = currentPlatform === "win32";
  const isMac = currentPlatform === "darwin";

  return new Promise((resolve) => {
    // On Windows and Linux, docker command should be in PATH
    // On Mac, try Docker.app first, then fall back to PATH
    const getDockerCommand = (): string => {
      if (!isMac) {
        return "docker info";
      }
      // macOS: Try Docker Desktop path first
      return "docker info";
    };

    const checkCommand = getDockerCommand();

    // Windows and Linux: Just check if docker is available
    if (isWindows || !isMac) {
      exec(checkCommand, (error) => {
        resolve({
          isAvailable: !error,
          error: error ? "Docker is not available. Some features may have limited functionality." : null,
        });
      });
      return;
    }

    // Mac-specific checks: First verify Docker Desktop is running
    exec('pgrep -x "Docker"', (error, stdout) => {
      if (!stdout) {
        // Docker Desktop not running, but docker CLI might still work (e.g., colima, rancher)
        exec(checkCommand, (error) => {
          resolve({
            isAvailable: !error,
            error: error ? "Docker is not available. Some features may have limited functionality." : null,
          });
        });
        return;
      }

      // Docker Desktop is running, check if it's responsive
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

  // Setup all IPC handlers first (before any windows are created)
  setupIpcHandlers();
  setupAppIpcHandlers(() => app.quit());
  setupAppEventListeners();

  try {
    const loadingWin = createLoadingWindow();

    const dockerStatusResult = await checkDocker();
    console.log("Docker status:", dockerStatusResult);

    updateDockerStatus(dockerStatusResult);

    // Update loading window with Docker status
    loadingWin?.webContents.send("update-docker-status", dockerStatusResult);

    // Start services and update status
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

    // Create main window only after services are ready
    createWindow();
    modifySessionHeaders();
    setupProcessMonitoring();

    const mainWindow = getMainWindow();
    if (mainWindow) {
      // Setup window-specific IPC handlers
      setupWindowIpcHandlers(mainWindow);

      mainWindow.webContents.once("did-finish-load", () => {
        console.log("Electron app startup successful, sending signal...");
        mainWindow.webContents.send("startup-success");
      });

      // Setup menu with custom refresh handler
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
