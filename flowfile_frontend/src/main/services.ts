import { app } from "electron";
import { join } from "path";
import { ChildProcess, spawn } from "child_process";
import axios from "axios";
import { platform } from "os";
import {
  SHUTDOWN_TIMEOUT,
  FORCE_KILL_TIMEOUT,
  WORKER_PORT,
  CORE_PORT,
} from "./constants";
import { existsSync, mkdirSync } from "fs";

export const shutdownState = { isShuttingDown: false };
let cleanupInProgress = false;
export let workerProcess: ChildProcess | null = null;
export let coreProcess: ChildProcess | null = null;

const log = (message: string, error?: any) => {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  error ? console.error(logMessage, error) : console.log(logMessage);
};

export async function shutdownService(port: number): Promise<void> {
  try {
    log(`Attempting to shutdown service on port ${port}`);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SHUTDOWN_TIMEOUT);

    await axios.post(`http://127.0.0.1:${port}/shutdown`, null, {
      signal: controller.signal,
      timeout: SHUTDOWN_TIMEOUT,
    });

    clearTimeout(timeout);
    log(`Successfully sent shutdown signal to port ${port}`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  } catch (error) {
    if (axios.isAxiosError(error) && error.code === "ECONNREFUSED") {
      log(`Service on port ${port} is already stopped`);
    } else {
      log(`Service on port ${port} shutdown timed out or failed`, error);
    }
  }
}

export async function cleanupProcesses(): Promise<void> {
  if (cleanupInProgress) {
    log("Cleanup already in progress...");
    return;
  }

  cleanupInProgress = true;
  log("Starting cleanup process...");

  try {
    await Promise.race([
      ensureServicesStopped(),
      new Promise((resolve) => setTimeout(resolve, SHUTDOWN_TIMEOUT)),
    ]);

    const cleanup = async (process: ChildProcess | null, name: string) => {
      if (!process) return;

      return new Promise<void>((resolve) => {
        const forceKill = setTimeout(() => {
          try {
            log(`Force killing ${name} process`);
            process.kill("SIGKILL");
          } catch (error) {
            log(`Error force killing ${name}:`, error);
          }
          resolve();
        }, FORCE_KILL_TIMEOUT);

        process.once("exit", () => {
          clearTimeout(forceKill);
          log(`${name} process exited successfully`);
          resolve();
        });

        try {
          process.kill("SIGTERM");
        } catch (error) {
          clearTimeout(forceKill);
          log(`Error sending SIGTERM to ${name}:`, error);
          try {
            process.kill("SIGKILL");
          } catch (secondError) {
            log(`Failed to force kill ${name}:`, secondError);
          }
          resolve();
        }
      });
    };

    await Promise.all([
      cleanup(workerProcess, "flowfile_worker"),
      cleanup(coreProcess, "flowfile_core"),
    ]);
  } finally {
    workerProcess = null;
    coreProcess = null;
    cleanupInProgress = false;
    log("Cleanup process completed");
  }
}

export async function ensureServicesStopped(): Promise<void> {
  try {
    await Promise.all([
      shutdownService(WORKER_PORT),
      shutdownService(CORE_PORT),
    ]);
  } catch (error) {
    log("Error during service shutdown:", error);
  }
}

export function getResourcePath(resourceName: string): string {
  const basePath =
    process.env.NODE_ENV === "development"
      ? app.getAppPath()
      : process.resourcesPath;

  const isWindows = platform() === "win32";
  const executableName = isWindows ? `${resourceName}.exe` : resourceName;

  // First try the new directory structure
  const directoryPath = join(basePath, resourceName, resourceName);
  const executablePath = join(basePath, resourceName, executableName);

  if (existsSync(directoryPath)) {
    log(`Using directory-based executable at: ${directoryPath}`);
    return directoryPath;
  }

  if (existsSync(executablePath)) {
    log(`Using directory executable at: ${executablePath}`);
    return executablePath;
  }

  // Fallback to old structure
  const legacyPath = join(basePath, executableName);
  log(`Falling back to legacy path: ${legacyPath}`);

  if (!existsSync(legacyPath)) {
    log(`WARNING: No executable found at any location for ${resourceName}`);
  }

  return legacyPath;
}

function getProcessEnv(): NodeJS.ProcessEnv {
  const isWindows = platform() === "win32";
  const homeDir = app.getPath("home");
  const tempDir = app.getPath("temp");
  const flowfileDir = join(homeDir, ".flowfile");
  const cacheDirRoot = join(flowfileDir, ".tmp");

  const dirsToCreate = [flowfileDir, cacheDirRoot];
  for (const dir of dirsToCreate) {
    try {
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
        log(`Created directory: ${dir}`);
      }
    } catch (error) {
      log(`Failed to create directory ${dir}`, error);
    }
  }

  return {
    ...process.env,
    HOME: homeDir,
    TMPDIR: tempDir,
    FLOWFILE_CACHE_ROOT: cacheDirRoot,
    PYTHONOPTIMIZE: "1",
    PYTHONDONTWRITEBYTECODE: "1",
  };
}

export function startProcess(
  name: string,
  path: string,
  port: number,
  onData?: (data: string) => void,
): Promise<ChildProcess> {
  return new Promise((resolve, reject) => {
    const isWindows = platform() === "win32";
    log(`Starting ${name} from ${path}`);

    // Get the working directory (directory containing the executable)
    const workingDirectory = path.endsWith(name)
      ? join(path, "..") // New structure
      : join(path, "../.."); // Legacy structure

    const childProcess = spawn(path, [], {
      env: getProcessEnv(),
      shell: isWindows,
      detached: false,
      stdio: ["ignore", "pipe", "pipe"],
      cwd: workingDirectory, // Set working directory
    });

    if (!childProcess.pid) {
      const error = new Error(`Failed to start ${name}`);
      log(`Process start failed for ${name}`, error);
      reject(error);
      return;
    }

    childProcess.stdout?.on("data", (data) => {
      const output = data.toString().trim();
      log(`[${name}] ${output}`);
      onData?.(output);
    });

    childProcess.stderr?.on("data", (data) => {
      log(`[${name} ERROR] ${data.toString().trim()}`);
    });

    childProcess.on("error", (error) => {
      log(`${name} process error`, error);
      reject(error);
    });

    const checkService = async () => {
      try {
        await axios.get(`http://127.0.0.1:${port}/docs`);
        log(`${name} is responsive on port ${port}`);
        resolve(childProcess);
      } catch (error) {
        setTimeout(checkService, 1000);
      }
    };
    setTimeout(checkService, 1000);
  });
}

export async function startServices(retry = true): Promise<void> {
  try {
    const corePromise = startProcess(
      "flowfile_core",
      getResourcePath("flowfile_core"),
      CORE_PORT,
      (data) => {
        if (data.includes("Core server started")) {
          console.log("Core process is ready");
        }
      },
    );

    const workerPromise = startProcess(
      "flowfile_worker",
      getResourcePath("flowfile_worker"),
      WORKER_PORT,
      (data) => {
        if (data.includes("Server started")) {
          console.log("Worker process is ready");
        }
      },
    );

    [coreProcess, workerProcess] = await Promise.all([
      corePromise,
      workerPromise,
    ]);
  } catch (error) {
    console.error("Error starting services:", error);
    if (retry) {
      console.log("Retrying service startup...");
      await cleanupProcesses();
      return startServices(false);
    }
    throw error;
  }
}

export function setupProcessMonitoring() {
  const monitorProcess = (process: ChildProcess | null, name: string) => {
    if (!process) return;

    process.on("exit", (code) => {
      console.log(`${name} exited with code ${code}`);
    });

    process.on("error", (error) => {
      console.error(`${name} encountered an error:`, error);
    });
  };

  monitorProcess(workerProcess, "flowfile_worker");
  monitorProcess(coreProcess, "flowfile_core");
}
