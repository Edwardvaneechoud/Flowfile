// appEvents.ts
import { app } from "electron";
import { cleanupProcesses, shutdownState } from "./services";
import { SHUTDOWN_TIMEOUT, FORCE_KILL_TIMEOUT } from "./constants";

export function setupAppEventListeners() {
  // Handle child process exits (includes GPU process crashes in newer Electron versions)
  app.on("child-process-gone", (_event, details) => {
    console.error("Child process gone:", {
      type: details.type,
      reason: details.reason,
      exitCode: details.exitCode,
      serviceName: details.serviceName,
    });
  });

  // Handle application quitting
  app.on("before-quit", (event) => {
    if (shutdownState.isShuttingDown) {
      return;
    }

    shutdownState.isShuttingDown = true;
    event.preventDefault();
    if (process.env.NODE_ENV === "development") {
      app.exit(0); // Exit the app without triggering 'before-quit' again
      return;
    }
    (async () => {
      try {
        await Promise.race([
          cleanupProcesses(),
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error("Shutdown timeout")),
              SHUTDOWN_TIMEOUT + FORCE_KILL_TIMEOUT,
            ),
          ),
        ]);
      } catch (error) {
        console.error("Shutdown timed out or failed:", error);
      } finally {
        app.exit(0); // Exit the app without triggering 'before-quit' again
      }
    })();
  });

  // Prevent app from quitting when all windows are closed on macOS
  app.on("window-all-closed", () => {
    if (process.platform !== "darwin") {
      app.quit();
    }
  });
}
