/**
 * This window's identity on core's change feed. Every request carries it as X-Flowfile-Client and
 * every event core streams echoes it, so a window can tell its own changes from another window's.
 * One id per window or tab (sessionStorage), kept across reloads. A pop-out keeps its own key: a
 * browser copies the opener's sessionStorage into a window it opens, and two windows sharing one id
 * would each take the other's changes for their own.
 */

import { isPopoutHash } from "../../lib/popoutWindow";

export const CLIENT_HEADER = "X-Flowfile-Client";

export const STORAGE_KEY = "flowfile.client_id.v1";
export const POPOUT_STORAGE_KEY = "flowfile.client_id.popout.v1";

const storageKey = (): string =>
  typeof location !== "undefined" && isPopoutHash(location.hash) ? POPOUT_STORAGE_KEY : STORAGE_KEY;

const mint = (): string =>
  typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;

const resolve = (): string => {
  try {
    const key = storageKey();
    const saved = sessionStorage.getItem(key);
    if (saved) return saved;
    const id = mint();
    sessionStorage.setItem(key, id);
    return id;
  } catch {
    return mint();
  }
};

export const clientId: string = resolve();
