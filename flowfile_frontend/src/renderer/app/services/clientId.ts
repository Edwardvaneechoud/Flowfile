/**
 * This window's identity on core's change feed. Every request carries it as X-Flowfile-Client and
 * every event core streams echoes it, so a window can tell its own changes from another window's.
 * One id per window or tab (sessionStorage), kept across reloads.
 */

export const CLIENT_HEADER = "X-Flowfile-Client";

const STORAGE_KEY = "flowfile.client_id.v1";

const mint = (): string =>
  typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;

const resolve = (): string => {
  try {
    const saved = sessionStorage.getItem(STORAGE_KEY);
    if (saved) return saved;
    const id = mint();
    sessionStorage.setItem(STORAGE_KEY, id);
    return id;
  } catch {
    return mint();
  }
};

export const clientId: string = resolve();
