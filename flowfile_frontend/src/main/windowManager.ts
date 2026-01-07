import { BrowserWindow, screen } from "electron";
import { join } from "path";
import { loadWindow } from "./windowLoader";

let mainWindow: BrowserWindow | null = null;
let authWindow: BrowserWindow | null = null;
let loadingWindow: BrowserWindow | null = null;

export function createLoadingWindow() {
  const primaryDisplay = screen.getPrimaryDisplay();
  const { width: screenWidth, height: screenHeight } = primaryDisplay.workAreaSize;
  const windowWidth = Math.min(700, screenWidth * 0.5);
  const windowHeight = Math.min(400, screenHeight * 0.4);

  const preloadPath = join(__dirname, "preload.js");
  const loadingHtmlPath = join(__dirname, "loading.html");

  loadingWindow = new BrowserWindow({
    width: windowWidth,
    height: windowHeight,
    x: (screenWidth - windowWidth) / 2,
    y: (screenHeight - windowHeight) / 2,
    webPreferences: {
      preload: preloadPath,
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
    },
    frame: false,
    transparent: true,
    show: false,
    backgroundColor: "#00ffffff",
    resizable: false,
    skipTaskbar: true,
  });

  loadingWindow.loadFile(loadingHtmlPath);

  loadingWindow.once("ready-to-show", () => {
    loadingWindow?.show();
  });

  loadingWindow.on("closed", () => {
    loadingWindow = null;
  });

  return loadingWindow;
}

export function createWindow() {
  const primaryDisplay = screen.getPrimaryDisplay();
  const { width, height } = primaryDisplay.workAreaSize;

  console.log("Creating main window");

  mainWindow = new BrowserWindow({
    width,
    height,
    webPreferences: {
      preload: join(__dirname, "preload.js"),
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
    },
    show: false,
    backgroundColor: "#ffffff",
  });

  mainWindow.once("ready-to-show", () => {
    console.log("Window ready to show");
    mainWindow?.show();
    if (loadingWindow) {
      loadingWindow.close();
    }
  });

  mainWindow.on("closed", () => {
    console.log("Main window closed");
    mainWindow = null;
  });

  loadWindow(mainWindow);
}

export function openAuthWindow(authUrl: string): Promise<string | null> {
  console.log("Opening authentication window");

  return new Promise((resolve) => {
    authWindow = new BrowserWindow({
      width: 600,
      height: 700,
      webPreferences: {
        nodeIntegration: false,
        contextIsolation: true,
        sandbox: true,
      },
      parent: mainWindow || undefined,
      modal: true,
      show: false,
    });

    authWindow.webContents.on("will-redirect", (_event, url) => {
      if (url.includes("code=")) {
        const urlParams = new URL(url).searchParams;
        const code = urlParams.get("code");
        resolve(code);
        authWindow?.close();
      }
    });

    authWindow.webContents.on("will-navigate", (_event, url) => {
      if (url.includes("code=")) {
        const urlParams = new URL(url).searchParams;
        const code = urlParams.get("code");
        resolve(code);
        authWindow?.close();
      }
    });

    authWindow.loadURL(authUrl);

    authWindow.once("ready-to-show", () => {
      console.log("Auth window ready to show");
      authWindow?.show();
    });

    authWindow.on("closed", () => {
      console.log("Auth window closed");
      authWindow = null;
      resolve(null); // User closed window without completing auth
    });
  });
}

export function getMainWindow() {
  return mainWindow;
}

export function getLoadingWindow() {
  return loadingWindow;
}
