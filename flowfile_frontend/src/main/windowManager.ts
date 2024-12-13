// windowManager.ts
import { BrowserWindow, screen } from "electron";
import { join } from "path";
import { loadWindow } from "./windowLoader";
import FileSystem from "fs";

let mainWindow: BrowserWindow | null = null;
let authWindow: BrowserWindow | null = null;
let loadingWindow: BrowserWindow | null = null;

export function createLoadingWindow() {
  const primaryDisplay = screen.getPrimaryDisplay();
  const { width: screenWidth, height: screenHeight } = primaryDisplay.workAreaSize;

  // Calculate window size based on screen size
  const windowWidth = Math.min(700, screenWidth * 0.5); // Max 700px or 50% of screen width
  const windowHeight = Math.min(400, screenHeight * 0.4); // Max 400px or 40% of screen height

  const preloadPath = join(__dirname, "preload.js");
  const loadingHtmlPath = join(__dirname, "loading.html");

  loadingWindow = new BrowserWindow({
    width: windowWidth,
    height: windowHeight,
    x: (screenWidth - windowWidth) / 2,
    y: (screenHeight - windowHeight) / 2,
    webPreferences: {
      preload: preloadPath,
      nodeIntegration: true,
      contextIsolation: false,
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

  // Window events
  mainWindow.once("ready-to-show", () => {
    console.log("Window ready to show");
    mainWindow?.show();

    // Close loading window after main window is shown
    if (loadingWindow) {
      loadingWindow.close();
    }
  });

  mainWindow.on("closed", () => {
    console.log("Main window closed");
    mainWindow = null;
  });

  // Load the content
  loadWindow(mainWindow);
}

export function openAuthWindow() {
  console.log("Opening authentication window");

  authWindow = new BrowserWindow({
    width: 600,
    height: 600,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: false,
    },
    parent: mainWindow || undefined,
    modal: true,
    show: false,
  });

  authWindow.loadURL("https://accounts.google.com/o/oauth2/auth");

  authWindow.once("ready-to-show", () => {
    console.log("Auth window ready to show");
    authWindow?.show();
  });

  authWindow.on("closed", () => {
    console.log("Auth window closed");
    authWindow = null;
  });
}

export function getMainWindow() {
  return mainWindow;
}

export function getLoadingWindow() {
  return loadingWindow;
}
