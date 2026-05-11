// Detect if running in Electron by checking for the preload-exposed electronAPI
const isElectron = !!(window as unknown as { electronAPI?: unknown }).electronAPI;

// In Electron mode, connect directly to the local backend.
// In web/Docker mode, send to <origin>/api/ so requests stay same-origin and
// nginx (Docker) or the Vite dev proxy (dev) can forward /api/* to flowfile-core.
// The base must be absolute because callers that use `new URL(path, base)`
// (aiStreamClient / aiDiffClient) reject relative bases — `new URL(path, "/api/")`
// throws "Invalid base URL".
export const flowfileCorebaseURL = isElectron
  ? "http://localhost:63578/"
  : `${window.location.origin}/api/`;
