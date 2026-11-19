// The app's own auth service and axios interceptors, loaded fresh over a stand-in for the server.
import axios, { AxiosError, type AxiosAdapter, type InternalAxiosRequestConfig } from "axios";
import { vi } from "vitest";
import { fakeStorage } from "./mount";

export type Answer = { status: number; data?: unknown } | Error;

export interface Recorded {
  config: InternalAxiosRequestConfig;
  storedToken: string | null;
}

/** Answers every request axios sends, as core would, and records it with what the browser held then. */
export function fakeServer(answer: (config: InternalAxiosRequestConfig) => Answer) {
  const requests: Recorded[] = [];
  const adapter: AxiosAdapter = async (config) => {
    requests.push({ config, storedToken: localStorage.getItem("auth_token") });
    const reply = answer(config);
    if (reply instanceof Error) throw new AxiosError(reply.message, "ERR_NETWORK", config);
    const response = {
      data: reply.data ?? "",
      status: reply.status,
      statusText: "",
      headers: {},
      config,
    };
    if (reply.status >= 400) {
      throw new AxiosError("refused", "ERR_BAD_REQUEST", config, null, response);
    }
    return response;
  };
  const renewals = () => requests.filter(({ config }) => config.url === "/auth/refresh");
  return { adapter, requests, renewals };
}

/** The renewal the server answers: a new pair, refused with 401, or `answer` as given. */
export const renewed = (accessToken = "renewed-token"): Answer => ({
  status: 200,
  data: {
    access_token: accessToken,
    refresh_token: "renewed-refresh",
    token_type: "bearer",
    expires_at: Date.now() + 900_000,
  },
});

export const refused: Answer = { status: 401, data: { detail: "Invalid refresh token" } };

/** How long a file's first import of the app's modules may take, which no test's own limit should pay. */
export const WARM_UP_MS = 60_000;

/**
 * Imports the auth service and the app's axios set-up once, in a `beforeAll`, so the first test of a file does not
 * spend its time limit on the cold transform of their module graph; `loadSession` then evaluates them afresh.
 */
export async function warmSession(...more: (() => Promise<unknown>)[]) {
  vi.stubGlobal("localStorage", fakeStorage());
  vi.stubGlobal("sessionStorage", fakeStorage());
  try {
    await import("../services/auth.service");
    await import("../services/axios.config");
    for (const load of more) await load();
  } finally {
    axios.interceptors.request.clear();
    axios.interceptors.response.clear();
    vi.unstubAllGlobals();
  }
}

/** Loads the auth service and the app's axios set-up afresh, in main.ts's order, answered by `adapter`. */
export async function loadSession(adapter: AxiosAdapter) {
  axios.interceptors.request.clear();
  axios.interceptors.response.clear();
  axios.defaults.adapter = adapter;
  vi.resetModules();
  const { authService } = await import("../services/auth.service");
  await import("../services/axios.config");
  return authService;
}

/** A session as the browser keeps it, its access token ending `expiresIn` ms from now. */
export const storeSession = (expiresIn: number, { hub = false, refresh = true } = {}) => {
  localStorage.setItem("auth_token", "ana-token");
  localStorage.setItem("auth_token_expiration", String(Date.now() + expiresIn));
  localStorage.setItem("auth_username", "ana");
  if (refresh) localStorage.setItem("auth_refresh_token", "ana-refresh");
  if (hub) localStorage.setItem("auth_hub_session", "1");
};

export interface Departure {
  href: string;
  pathname: string;
  hash: string;
  stored: (string | null)[];
}

const SESSION_KEYS = [
  "auth_token",
  "auth_token_expiration",
  "auth_refresh_token",
  "auth_username",
  "auth_hub_session",
];

/** Records each reload of the page, with the address and what localStorage held at that moment. */
export function watchReloads() {
  const reloads: Departure[] = [];
  const spy = vi.spyOn(window.location, "reload").mockImplementation(() => {
    reloads.push({
      href: window.location.href,
      pathname: window.location.pathname,
      hash: window.location.hash,
      stored: SESSION_KEYS.map((key) => localStorage.getItem(key)),
    });
  });
  return { reloads, restore: () => spy.mockRestore() };
}

/** The `redirect` of a sign-in page address. */
export const redirectOf = (hash: string) =>
  new URLSearchParams(hash.split("?")[1] ?? "").get("redirect");

/** Settles with `promise`, or fails after `ms` instead of waiting for ever. */
export const within = <T>(promise: Promise<T>, ms = 500): Promise<T> =>
  Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`still waiting after ${ms} ms`)), ms),
    ),
  ]);

/** Lets the promises in flight settle. */
export const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
