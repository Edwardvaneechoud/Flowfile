// appEvents.ts
import { app } from "electron";
import { cleanupProcesses, shutdownState } from "./services";
import { SHUTDOWN_TIMEOUT, FORCE_KILL_TIMEOUT } from "./constants";

export function setupAppEventListeners() {
  // Handle GPU process crashes
  app.on("gpu-process-crashed", (_event, killed) => {
    console.error("GPU Process crashed. Killed:", killed);
  });

  // Handle child process exits
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
      console.log("Shutdown already in progress...");
      return;
    }

    shutdownState.isShuttingDown = true;
    event.preventDefault();
    console.log("Initiating application shutdown...");
    if (process.env.NODE_ENV === "development") {
      console.log("Exiting app...");
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
        console.log("Exiting app...");
        app.exit(0); // Exit the app without triggering 'before-quit' again
      }
    })();
  });

  // Prevent app from quitting when all windows are closed on macOS
  app.on("window-all-closed", () => {
    if (process.platform !== "darwin") {
      console.log("All windows closed, quitting app...");
      app.quit();
    }
  });
}
