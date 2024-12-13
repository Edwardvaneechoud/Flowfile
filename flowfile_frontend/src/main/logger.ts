// logger.ts
import { app } from "electron";
import { join } from "path";
import { format } from "util";
import { appendFileSync, mkdirSync, existsSync } from "fs";

export function setupLogging() {
  const logPath = app.getPath("userData");
  const logFile = join(logPath, "app.log");
  console.log("Logging to:", logPath);

  // Ensure log directory exists
  if (!existsSync(logPath)) {
    mkdirSync(logPath, { recursive: true });
  }

  // Log startup
  appendFileSync(logFile, `\n\n=== App Started at ${new Date().toISOString()} ===\n`);

  // Override console.log and console.error
  const originalLog = console.log;
  const originalError = console.error;

  console.log = (...args) => {
    originalLog.apply(console, args);
    appendFileSync(logFile, `${new Date().toISOString()} [LOG] ${format(...args)}\n`);
  };

  console.error = (...args) => {
    originalError.apply(console, args);
    appendFileSync(logFile, `${new Date().toISOString()} [ERROR] ${format(...args)}\n`);
  };

  return logFile;
}
