// services.ts
import { app } from "electron";
import { join } from "path";
import { ChildProcess, exec } from "child_process";
import axios from "axios";
import {
  SHUTDOWN_TIMEOUT,
  FORCE_KILL_TIMEOUT,
  WORKER_PORT,
  CORE_PORT,
} from "./constants";

// Global variables for managing processes
export const shutdownState = {
  isShuttingDown: false,
};

let cleanupInProgress = false;
export let workerProcess: ChildProcess | null = null;
export let coreProcess: ChildProcess | null = null;

export async function shutdownService(port: number): Promise<void> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SHUTDOWN_TIMEOUT);

    await axios.post(`http://127.0.0.1:${port}/shutdown`, null, {
      signal: controller.signal,
      timeout: SHUTDOWN_TIMEOUT,
    });

    clearTimeout(timeout);
    console.log(`Successfully sent shutdown signal to port ${port}`);
    await new Promise((resolve) => setTimeout(resolve, 500)); // Brief wait for shutdown to begin
  } catch (error) {
    if (axios.isAxiosError(error) && error.code === "ECONNREFUSED") {
      console.log(`Service on port ${port} is already stopped`);
    } else {
      console.log(`Service on port ${port} shutdown timed out or failed`);
    }
  }
}

export async function ensureServicesStopped(): Promise<void> {
  try {
    await Promise.all([
      shutdownService(WORKER_PORT),
      shutdownService(CORE_PORT),
    ]);
  } catch (error) {
    console.error("Error during service shutdown:", error);
  }
}

export function getResourcePath(resourceName: string): string {
  const basePath =
    process.env.NODE_ENV === "development"
      ? app.getAppPath()
      : process.resourcesPath;

  console.log("Base path:", basePath);
  return join(basePath, resourceName);
}

export function startProcess(
  name: string,
  path: string,
  port: number,
  onData?: (data: string) => void,
): Promise<ChildProcess> {
  return new Promise((resolve, reject) => {
    console.log(`Starting ${name} from ${path}`);

    const childProcess = exec(path);

    if (!childProcess.pid) {
      reject(new Error(`Failed to start ${name}`));
      return;
    }

    childProcess.stdout?.on("data", (data) => {
      console.log(`[${name} stdout]: ${data}`);
      onData?.(data.toString());
    });

    childProcess.stderr?.on("data", (data) => {
      console.error(`[${name} stderr]: ${data}`);
    });

    childProcess.on("error", (error) => {
      console.error(`${name} error:`, error);
      reject(error);
    });

    // Check if service is responsive
    const checkService = async () => {
      try {
        await axios.get(`http://127.0.0.1:${port}/docs`);
        console.log(`${name} is responsive on port ${port}`);
        resolve(childProcess);
      } catch (error) {
        console.log(`${name} not yet responsive, retrying...`);
        setTimeout(checkService, 1000);
      }
    };
    setTimeout(checkService, 1000);
  });
}

export async function startServices(retry = true): Promise<void> {
  try {
    if (retry) {
      // await ensureServicesStopped();
    }

    // Start both processes in parallel
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

    // Wait for both processes to be ready
    [coreProcess, workerProcess] = await Promise.all([
      corePromise,
      workerPromise,
    ]);
  } catch (error) {
    console.error("Error starting services:", error);
    if (retry) {
      console.log("Retrying service startup...");
      await cleanupProcesses();
      return startServices(false); // Retry once without the initial shutdown
    }
    throw error;
  }
}

export async function cleanupProcesses(): Promise<void> {
  if (cleanupInProgress) {
    console.log("Cleanup already in progress...");
    return;
  }

  cleanupInProgress = true;
  console.log("Starting cleanup process...");

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
            console.warn(`Force killing ${name} process`);
            process.kill("SIGKILL");
          } catch (error) {
            console.error(`Error force killing ${name}:`, error);
          }
          resolve();
        }, FORCE_KILL_TIMEOUT);

        process.once("exit", () => {
          clearTimeout(forceKill);
          console.log(`${name} process exited successfully`);
          resolve();
        });

        try {
          process.kill("SIGTERM");
        } catch (error) {
          clearTimeout(forceKill);
          console.error(`Error sending SIGTERM to ${name}:`, error);
          try {
            process.kill("SIGKILL");
          } catch (secondError) {
            console.error(`Failed to force kill ${name}:`, secondError);
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
    console.log("Cleanup process completed");
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
