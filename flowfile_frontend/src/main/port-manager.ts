// port-manager.ts
import { app } from "electron";
import { join } from "path";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { platform } from "os";
import { nanoid } from "nanoid";
import { getPortPromise } from "portfinder";

// Default ports (fallbacks)
const DEFAULT_CORE_PORT = 63578;
const DEFAULT_WORKER_PORT = 63579;

// Port range to search for available ports
const PORT_RANGE_START = 63000;
const PORT_RANGE_END = 64000;

// File to store port information
const getPortConfigPath = () => join(app.getPath("userData"), "port-config.json");

interface PortConfig {
  corePort: number;
  workerPort: number;
  timestamp: number;
  sessionId: string;
}

/**
 * Get available port in a specific range
 */
async function getAvailablePort(startPort: number): Promise<number> {
  try {
    return await getPortPromise({
      port: startPort,
      stopPort: PORT_RANGE_END
    });
  } catch (error) {
    console.error("Error finding available port:", error);
    // Return a random port in our range as fallback
    return Math.floor(Math.random() * (PORT_RANGE_END - PORT_RANGE_START)) + PORT_RANGE_START;
  }
}

/**
 * Load port configuration from disk
 */
export function loadPortConfig(): PortConfig | null {
  const configPath = getPortConfigPath();
  
  if (existsSync(configPath)) {
    try {
      const data = readFileSync(configPath, 'utf8');
      return JSON.parse(data) as PortConfig;
    } catch (error) {
      console.error("Error reading port config file:", error);
    }
  }
  
  return null;
}

/**
 * Save port configuration to disk
 */
export function savePortConfig(config: PortConfig): void {
  try {
    const configPath = getPortConfigPath();
    writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
    console.log("Port configuration saved to:", configPath);
  } catch (error) {
    console.error("Error saving port config:", error);
  }
}

/**
 * Setup port configuration for this session
 */
export async function setupPortConfig(): Promise<PortConfig> {
  // Try to load existing config first
  const existingConfig = loadPortConfig();
  
  // Generate new session ID for this run
  const sessionId = nanoid();
  
  if (existingConfig) {
    console.log("Found existing port configuration:", existingConfig);
    
    // Create updated config with new session ID but same ports
    const updatedConfig: PortConfig = {
      ...existingConfig,
      sessionId,
      timestamp: Date.now()
    };
    
    savePortConfig(updatedConfig);
    return updatedConfig;
  }
  
  // Find available ports
  console.log("Finding available ports...");
  const corePort = await getAvailablePort(PORT_RANGE_START);
  const workerPort = await getAvailablePort(corePort + 1); // Ensure different port
  
  const config: PortConfig = {
    corePort,
    workerPort,
    timestamp: Date.now(),
    sessionId
  };
  
  savePortConfig(config);
  console.log("Created new port configuration:", config);
  
  return config;
}

/**
 * Get command line arguments for launching services with specific ports
 */
export function getServiceArgs(config: PortConfig): {
  coreArgs: string[];
  workerArgs: string[];
} {
  return {
    coreArgs: [
      "--port", config.corePort.toString(),
      "--worker-port", config.workerPort.toString()
    ],
    workerArgs: [
      "--port", config.workerPort.toString(),
      "--core-port", config.corePort.toString()
    ]
  };
}

/**
 * Get service URLs based on configuration
 */
export function getServiceUrls(config: PortConfig): {
  coreUrl: string;
  workerUrl: string;
} {
  // Use 127.0.0.1 for local connections from electron
  return {
    coreUrl: `http://127.0.0.1:${config.corePort}`,
    workerUrl: `http://127.0.0.1:${config.workerPort}`
  };
}