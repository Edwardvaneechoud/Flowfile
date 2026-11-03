// @vitest-environment happy-dom
// A session handed over in the URL fragment (`#token=<jwt>` or a hub's `#signin=<token>`) on the web build.
import axios, { AxiosError, type AxiosAdapter, type InternalAxiosRequestConfig } from "axios";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createRouter, createWebHashHistory, type RouterHistory } from "vue-router";

const base64url = (value: string): string => {
  const bytes = new TextEncoder().encode(value);
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
};

const jwt = (claims: Record<string, unknown>): string =>
  [
    base64url(JSON.stringify({ alg: "HS256", typ: "JWT" })),
    base64url(JSON.stringify(claims)),
    "sig",
  ].join(".");

const inAnHour = () => Math.floor(Date.now() / 1000) + 3600;

function fakeLocalStorage() {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
    removeItem: (key: string) => void store.delete(key),
    clear: () => store.clear(),
    values: () => [...store.values()],
  };
}

const load = async () => {
  vi.resetModules();
  return (await import("./auth.service")).authService;
};

const openAt = (hash: string) => window.history.replaceState(null, "", `/editor${hash}`);

const view = { render: () => null };

// The app's router imports the service, so the service is constructed before the hash history.
const startApp = async () => {
  const authService = await load();
  const history = createWebHashHistory();
  const router = createRouter({
    history,
    routes: [
      { path: "/", redirect: "/designer" },
      { path: "/designer", component: view },
      { path: "/:pathMatch(.*)*", redirect: "/designer" },
    ],
  });
  return { authService, history, router };
};

describe("auth service: token in the URL fragment", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
    openAt("");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    openAt("");
  });

  it("stores the session from the token's claims and removes the fragment", async () => {
    const exp = inAnHour();
    const token = jwt({ sub: "zoë", exp, type: "access" });
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(authService.isAuthenticated()).toBe(true);
    expect(await authService.getToken()).toBe(token);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_token_expiration")).toBe(String(exp * 1000));
    expect(localStorage.getItem("auth_username")).toBe("zoë");
    expect(window.location.hash).toBe("");
    expect(window.location.pathname).toBe("/editor");
  });

  it("replaces a stored session and drops its refresh token", async () => {
    localStorage.setItem("auth_token", "earlier");
    localStorage.setItem("auth_token_expiration", String(Date.now() + 3_600_000));
    localStorage.setItem("auth_username", "someone-else");
    localStorage.setItem("auth_refresh_token", "earlier-refresh");
    const token = jwt({ sub: "alice", exp: inAnHour() });
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
  });

  it("changes nothing without a fragment", async () => {
    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
  });

  it("reads only the token key from the fragment", async () => {
    openAt("#/designer");

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(window.location.hash).toBe("#/designer");
  });

  it.each([
    ["not a JWT", "abc"],
    ["a payload that is not JSON", `x.${base64url("not json")}.sig`],
    ["no subject", jwt({ exp: inAnHour() })],
    ["no expiry", jwt({ sub: "alice" })],
    ["an expired token", jwt({ sub: "alice", exp: Math.floor(Date.now() / 1000) - 60 })],
    ["an empty token", ""],
  ])("ignores %s and leaves the sign-in page to show", async (_label, token) => {
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_username")).toBeNull();
    expect(window.location.hash).toBe("");
  });
});

describe("auth service: token in the URL fragment, with the hash router", () => {
  let history: RouterHistory | undefined;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
    openAt("");
  });

  afterEach(() => {
    history?.destroy();
    history = undefined;
    vi.unstubAllGlobals();
    openAt("");
  });

  it("signs in on a fresh load and leaves no token in the route or history", async () => {
    const token = jwt({ sub: "alice", exp: inAnHour() });
    openAt(`#token=${token}`);

    const app = await startApp();
    history = app.history;

    expect(await app.authService.initialize()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(app.history.location).toBe("/");
    expect(window.location.hash).toBe("#/");
    expect(JSON.stringify(window.history.state)).not.toContain(token);

    await app.router.push(app.history.location);
    expect(app.router.currentRoute.value.fullPath).toBe("/designer");
    expect(app.router.currentRoute.value.redirectedFrom?.fullPath).toBe("/");
    expect(window.location.href).not.toContain(token);
  });

  it("ignores a malformed token and leaves no trace of it", async () => {
    openAt("#token=not-a-jwt");

    const app = await startApp();
    history = app.history;

    expect(await app.authService.initialize()).toBe(false);
    expect(app.authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(app.history.location).toBe("/");

    await app.router.push(app.history.location);
    expect(app.router.currentRoute.value.redirectedFrom?.fullPath).toBe("/");
    expect(window.location.href).not.toContain("not-a-jwt");
  });
});

type Answer = { status: number; data?: unknown } | Error;

/** Answers every request axios sends, as core would, and records it with what the browser held then. */
function fakeServer(answer: (config: InternalAxiosRequestConfig) => Answer) {
  const requests: { config: InternalAxiosRequestConfig; storedToken: string | null }[] = [];
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
  return { adapter, requests };
}

const inFifteenMinutes = () => Math.floor(Date.now() / 1000) + 900;

describe("auth service: sign-in through the hub", () => {
  const original = axios.defaults.adapter;
  let history: RouterHistory | undefined;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
    openAt("");
  });

  afterEach(() => {
    axios.defaults.adapter = original;
    history?.destroy();
    history = undefined;
    vi.unstubAllGlobals();
    openAt("");
  });

  it("posts the sign-in token once, stores the session and opens the editor without it", async () => {
    const exp = inFifteenMinutes();
    const access = jwt({ sub: "ana@hub.example.com", exp, type: "access" });
    const server = fakeServer(() => ({
      status: 200,
      data: { access_token: access, refresh_token: "core-refresh", token_type: "bearer" },
    }));
    axios.defaults.adapter = server.adapter;
    openAt("#signin=one-time-token");

    const app = await startApp();
    history = app.history;

    expect(await app.authService.initialize()).toBe(true);
    expect(server.requests).toHaveLength(1);
    const { config } = server.requests[0];
    expect([config.method, config.url]).toEqual(["post", "/auth/hub/signin"]);
    expect(JSON.parse(config.data)).toEqual({ token: "one-time-token" });
    expect(config.headers.Authorization).toBeUndefined();

    expect(app.authService.isAuthenticated()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(access);
    expect(localStorage.getItem("auth_token_expiration")).toBe(String(exp * 1000));
    expect(localStorage.getItem("auth_username")).toBe("ana@hub.example.com");
    expect(localStorage.getItem("auth_refresh_token")).toBe("core-refresh");
    const stored = (localStorage as unknown as { values: () => string[] }).values();
    expect(stored.some((value) => value.includes("one-time-token"))).toBe(false);

    await app.router.push(app.history.location);
    expect(app.router.currentRoute.value.fullPath).toBe("/designer");
    expect(window.location.href).not.toContain("one-time-token");
    expect(JSON.stringify(window.history.state)).not.toContain("one-time-token");
  });

  it("leaves the sign-in page to show the server's sentence for a refused token", async () => {
    localStorage.setItem("auth_token", "earlier");
    localStorage.setItem("auth_token_expiration", String(Date.now() + 3_600_000));
    localStorage.setItem("auth_refresh_token", "earlier-refresh");
    const sentence = "The hub refused this sign-in: this sign-in token was already used.";
    const server = fakeServer(() => ({ status: 401, data: { detail: sentence } }));
    axios.defaults.adapter = server.adapter;
    openAt("#signin=spent-token");

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(server.requests).toHaveLength(1);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
    expect(window.location.hash).toBe("");
    expect(authService.takeHubSignInError()).toBe(sentence);
    expect(authService.takeHubSignInError()).toBeNull();
  });

  it("tells the server of a sign-out before it clears the session", async () => {
    const access = jwt({ sub: "ana", exp: inFifteenMinutes() });
    localStorage.setItem("auth_token", access);
    localStorage.setItem("auth_token_expiration", String(Date.now() + 600_000));
    localStorage.setItem("auth_username", "ana");
    localStorage.setItem("auth_refresh_token", "core-refresh");
    const server = fakeServer(() => ({ status: 204 }));
    axios.defaults.adapter = server.adapter;
    const authService = await load();

    await authService.signOut();

    expect(server.requests).toHaveLength(1);
    const { config, storedToken } = server.requests[0];
    expect([config.method, config.url]).toEqual(["post", "/auth/logout"]);
    expect(config.headers.Authorization).toBe(`Bearer ${access}`);
    expect(storedToken).toBe(access);
    expect(authService.isAuthenticated()).toBe(false);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
    expect(localStorage.getItem("auth_username")).toBeNull();
  });

  it.each([
    ["a refusal", { status: 401, data: { detail: "Not authenticated" } }],
    ["no answer", new Error("Network Error")],
  ])("clears the session on a sign-out the server meets with %s", async (_label, answer) => {
    localStorage.setItem("auth_token", jwt({ sub: "ana", exp: inFifteenMinutes() }));
    localStorage.setItem("auth_token_expiration", String(Date.now() + 600_000));
    localStorage.setItem("auth_refresh_token", "core-refresh");
    axios.defaults.adapter = fakeServer(() => answer).adapter;
    const authService = await load();

    await authService.signOut();

    expect(authService.isAuthenticated()).toBe(false);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
  });
});
