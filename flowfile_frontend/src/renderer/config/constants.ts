// Detect if running in Electron by checking for the preload-exposed electronAPI
const isElectron = !!(window as unknown as { electronAPI?: unknown }).electronAPI;

// In Electron mode, connect directly to the local backend.
// In web/Docker mode, use a relative /api/ path so requests go through the nginx reverse proxy
// (the browser sends requests to the same origin, and nginx forwards /api/* to flowfile-core).
export const flowfileCorebaseURL = isElectron ? "http://localhost:63578/" : "/api/";
