// windowLoader.ts
import { app, BrowserWindow } from "electron";
import { join } from "path";

export function loadWindow(window: BrowserWindow) {
  if (process.env.NODE_ENV === "development") {
    const rendererPort = process.argv[2];
    window.loadURL(`http://localhost:${rendererPort}`).catch((err) => {
      console.error("Failed to load URL in development mode:", err);
    });
  } else {
    window
      .loadFile(join(app.getAppPath(), "renderer", "index.html"))
      .catch((err) => {
        console.error("Failed to load file in production mode:", err);
      });
  }
}
