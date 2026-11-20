import { toRaw } from "vue";

// Deep-clone via JSON round-trip. Strips Vue reactive proxies before payloads
// hit GraphicWalker's web worker — native structuredClone() preserves proxy
// traps and postMessage rejects them.
export const toPlainJson = <T>(value: T): T => JSON.parse(JSON.stringify(value));

const plainCopy = (value: unknown): unknown => {
  const raw = value !== null && typeof value === "object" ? toRaw(value) : value;
  if (Array.isArray(raw)) return raw.map((v) => (v === undefined ? null : plainCopy(v)));
  if (raw === null || typeof raw !== "object") return raw;
  const out: Record<string, unknown> = {};
  for (const [key, v] of Object.entries(raw)) if (v !== undefined) out[key] = plainCopy(v);
  return out;
};

/**
 * The same plain, proxy-free copy of JSON data as `toPlainJson`, keeping each string by
 * reference instead of copying it. Strings are immutable, so the copy is as independent, and
 * many copies of a value holding a large string (an image's data URL) share its bytes.
 */
export const toPlainShared = <T>(value: T): T => plainCopy(value) as T;
