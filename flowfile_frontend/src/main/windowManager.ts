// windowManager.ts
import { BrowserWindow, screen } from "electron";
import { join } from "path";
import { loadWindow } from "./windowLoader";

let mainWindow: BrowserWindow | null = null;
let authWindow: BrowserWindow | null = null;

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
    show: false, // Don't show the window until it's ready
    backgroundColor: "#ffffff",
  });

  // Window events
  mainWindow.once("ready-to-show", () => {
    console.log("Window ready to show");
    mainWindow?.show();
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
    parent: mainWindow || undefined, // Make it a child of the main window
    modal: true, // Modal window
    show: false, // Don't show until it's ready
  });

  // Load the authentication URL
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
