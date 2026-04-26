// Deep-clone via JSON round-trip. Strips Vue reactive proxies before payloads
// hit GraphicWalker's web worker — native structuredClone() preserves proxy
// traps and postMessage rejects them.
export const toPlainJson = <T>(value: T): T => JSON.parse(JSON.stringify(value));
