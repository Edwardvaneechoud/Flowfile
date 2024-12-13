import { app } from "electron";
import { join, dirname } from "path";
import { ChildProcess, exec, spawn } from "child_process";
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
    await new Promise((resolve) => setTimeout(resolve, 500));
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


function findProjectRoot(startPath: string): string {
  let currentPath = startPath;
  while (currentPath !== dirname(currentPath)) { // Stop at root directory
    if (existsSync(join(currentPath, 'package.json'))) {
      return currentPath;
    }
    currentPath = dirname(currentPath);
  }
  return '';
}


export function getResourceServicePath(resourceName: string): string {
  if (process.env.NODE_ENV === "development") {
    const projectRoot = findProjectRoot(app.getAppPath());
    console.log(projectRoot)
    if (!projectRoot) {
      console.warn('Could not find project root directory');
      return '';
    }

    const devPath = join(projectRoot, "..", "services_dist", resourceName);
    if (existsSync(devPath)) {
      console.log(`Using development executable at: ${devPath}`);
      return devPath;
    }
    console.warn(`Development executable not found at: ${devPath}`);
    return "";
  }

  // Production path handling remains the same...
  const basePath = join(process.resourcesPath, "flowfile-services");
  const isWindows = platform() === "win32";
  const executableName = isWindows ? `${resourceName}.exe` : resourceName;
  const directoryPath = join(basePath, resourceName);
  const executablePath = join(directoryPath, executableName);

  if (existsSync(directoryPath) && existsSync(executablePath)) {
    console.log(`Using production executable at: ${directoryPath}`);
    return directoryPath;
  }

  console.warn(`Production executable not found at: ${executablePath}`);
  return "";
}

function formatDockerPath(path: string): string {
  if (platform() === "win32") {
    return path.replace(/\\/g, "/").replace(/^(\w):\//, "/$1/");
  }
  return path;
}

function getProcessEnv(): NodeJS.ProcessEnv {
  const isWindows = platform() === "win32";
  const homeDir = app.getPath("home");
  const tempDir = app.getPath("temp");
  const flowfileDir = join(homeDir, ".flowfile");
  const airbyteDir = join(homeDir, ".airbyte");
  const cacheDirRoot = join(flowfileDir, ".tmp");

  const dirsToCreate = [
    flowfileDir,
    airbyteDir,
    cacheDirRoot,
    join(airbyteDir, "connectors"),
    join(tempDir, "airbyte", "logs"),
  ];

  for (const dir of dirsToCreate) {
    try {
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
    } catch (error) {
      console.error(`Failed to create directory ${dir}:`, error);
    }
  }

  const dockerVolumes = isWindows
    ? `-v "${formatDockerPath(cacheDirRoot)}:/tmp" -v "${formatDockerPath(airbyteDir)}:/airbyte"`
    : `--volume "${cacheDirRoot}:/tmp" --volume "${airbyteDir}:/airbyte"`;

  const baseEnv = {
    ...process.env,
    HOME: homeDir,
    DOCKER_CONFIG: join(homeDir, ".docker"),
    TMPDIR: tempDir,
    AIRBYTE_CACHE_ROOT: cacheDirRoot,
    AIRBYTE_TEMP_DIR: tempDir,
    AIRBYTE_LOGS_DIR: join(tempDir, "airbyte", "logs"),
    AIRBYTE_LOCAL_ROOT: airbyteDir,
    AIRBYTE_EXTRA_DOCKER_OPTS: dockerVolumes,
  };

  if (isWindows) {
    return {
      ...baseEnv,
      DOCKER_HOST: "npipe:////.//pipe//docker_engine",
    };
  }

  return {
    ...baseEnv,
    PATH: `/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${process.env.PATH || ""}`,
    DOCKER_HOST: "unix:///var/run/docker.sock",
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
    console.log(`Starting ${name} from ${path}`);

    // Get the working directory (directory containing the executable)
    const workingDirectory = path.endsWith(name)
      ? join(path, "..") // New structure
      : join(path, "../.."); // Legacy structure

    const childProcess = spawn(path, [], {
      env: getProcessEnv(), // Keep using the original getProcessEnv with all Docker settings
      shell: isWindows ? true : "/bin/bash",
      detached: false,
      stdio: ["ignore", "pipe", "pipe"],
      cwd: workingDirectory, // Add this line for directory handling
    });

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

    const checkService = async () => {
      try {
        await axios.get(`http://127.0.0.1:${port}/docs`);
        console.log(`${name} is responsive on port ${port}`);
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
      getResourceServicePath("flowfile_core"),
      CORE_PORT,
      (data) => {
        if (data.includes("Core server started")) {
          console.log("Core process is ready");
        }
      },
    );

    const workerPromise = startProcess(
      "flowfile_worker",
      getResourceServicePath("flowfile_worker"),
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
