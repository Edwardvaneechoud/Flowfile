// @vitest-environment happy-dom
// A hub's sign-in token in the URL fragment (`#signin=<token>&state=<state>`) on the web build, taken only with the
// state this tab made when it started the sign-in; `#token=<jwt>` signs nobody in.
import axios, { AxiosError, type AxiosAdapter, type InternalAxiosRequestConfig } from "axios";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createRouter, createWebHashHistory, type RouterHistory } from "vue-router";
import { fakeStorage } from "../__fixtures__/mount";

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

describe("auth service: a token in the URL fragment", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeStorage());
    vi.stubGlobal("sessionStorage", fakeStorage());
    openAt("");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    openAt("");
  });

  it("is no sign-in: a link carrying `#token=<jwt>` signs nobody in", async () => {
    const token = jwt({ sub: "mallory", exp: inAnHour(), type: "access" });
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_username")).toBeNull();
  });

  it("leaves a stored session as it is", async () => {
    const earlier = jwt({ sub: "alice", exp: inAnHour() });
    localStorage.setItem("auth_token", earlier);
    localStorage.setItem("auth_token_expiration", String(Date.now() + 3_600_000));
    localStorage.setItem("auth_username", "alice");
    localStorage.setItem("auth_refresh_token", "alice-refresh");
    openAt(`#token=${jwt({ sub: "mallory", exp: inAnHour() })}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(earlier);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(localStorage.getItem("auth_refresh_token")).toBe("alice-refresh");
  });

  it("changes nothing without a fragment", async () => {
    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
  });

  it("leaves a route in the fragment alone", async () => {
    openAt("#/designer");

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(window.location.hash).toBe("#/designer");
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

const OPEN = "https://hub.example.com:8443/open/12";

/** Start a sign-in through the hub as the sign-in page does, and answer the state the hub will carry back. */
const startSignIn = async (): Promise<string> => {
  const url = new URL((await load()).hubSignInUrl(OPEN));
  return url.searchParams.get("state") ?? "";
};

const storeSession = (username: string) => {
  const token = jwt({ sub: username, exp: inAnHour() });
  localStorage.setItem("auth_token", token);
  localStorage.setItem("auth_token_expiration", String(Date.now() + 3_600_000));
  localStorage.setItem("auth_username", username);
  localStorage.setItem("auth_refresh_token", `${username}-refresh`);
  return token;
};

describe("auth service: sign-in through the hub", () => {
  const original = axios.defaults.adapter;
  let history: RouterHistory | undefined;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeStorage());
    vi.stubGlobal("sessionStorage", fakeStorage());
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
    const expiresAt = Date.now() + 900_000;
    const access = "opaque-access-token";
    const server = fakeServer(() => ({
      status: 200,
      data: {
        access_token: access,
        refresh_token: "core-refresh",
        token_type: "bearer",
        expires_at: expiresAt,
      },
    }));
    axios.defaults.adapter = server.adapter;
    openAt(`#signin=one-time-token&state=${await startSignIn()}`);

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
    expect(localStorage.getItem("auth_token_expiration")).toBe(String(expiresAt));
    expect(localStorage.getItem("auth_refresh_token")).toBe("core-refresh");
    expect(localStorage.getItem("auth_hub_session")).toBe("1");
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
    openAt(`#signin=spent-token&state=${await startSignIn()}`);

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

  it("sends the browser to the hub's Open page with a new one-time state each time", async () => {
    const authService = await load();

    const first = new URL(authService.hubSignInUrl(OPEN));
    const second = new URL(authService.hubSignInUrl(OPEN));

    expect(first.origin + first.pathname).toBe(OPEN);
    expect([...first.searchParams.keys()]).toEqual(["state"]);
    expect(first.searchParams.get("state")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(second.searchParams.get("state")).not.toBe(first.searchParams.get("state"));
  });

  it("goes to the hub's Open page by replacing the history entry", async () => {
    const authService = await load();
    const replace = vi.spyOn(window.location, "replace").mockImplementation(() => {});
    const assign = vi.spyOn(window.location, "assign").mockImplementation(() => {});

    authService.startHubSignIn(OPEN);

    expect(replace).toHaveBeenCalledTimes(1);
    expect(
      new URL(replace.mock.calls[0][0] as string).origin +
        new URL(replace.mock.calls[0][0] as string).pathname,
    ).toBe(OPEN);
    expect(assign).not.toHaveBeenCalled();
    replace.mockRestore();
    assign.mockRestore();
  });

  it.each([
    ["without a state", (token: string) => `#signin=${token}`],
    ["with an empty state", (token: string) => `#signin=${token}&state=`],
    ["with another state", (token: string) => `#signin=${token}&state=not-the-one`],
  ])("signs nobody in and nobody out for a sign-in token %s", async (_case, fragment) => {
    const server = fakeServer(() => ({ status: 200, data: {} }));
    axios.defaults.adapter = server.adapter;
    await startSignIn();
    const earlier = storeSession("alice");
    openAt(fragment("mallory-token"));

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(server.requests).toHaveLength(0);
    expect(localStorage.getItem("auth_token")).toBe(earlier);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(localStorage.getItem("auth_refresh_token")).toBe("alice-refresh");
    expect(window.location.hash).toBe("");
    expect(authService.takeHubSignInError()).toBeNull();
  });

  it("signs nobody in for a sign-in token when this tab started no sign-in", async () => {
    const server = fakeServer(() => ({ status: 200, data: {} }));
    axios.defaults.adapter = server.adapter;
    openAt("#signin=mallory-token&state=guessed");

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(server.requests).toHaveLength(0);
    expect(authService.requiresLogin()).toBe(true);
    expect(authService.takeHubSignInError()).toBeNull();
  });

  it("takes a state once", async () => {
    const access = jwt({ sub: "ana@hub.example.com", exp: inFifteenMinutes(), type: "access" });
    const server = fakeServer(() => ({
      status: 200,
      data: {
        access_token: access,
        refresh_token: "core-refresh",
        token_type: "bearer",
        expires_at: Date.now() + 900_000,
      },
    }));
    axios.defaults.adapter = server.adapter;
    const state = await startSignIn();
    openAt(`#signin=ana-token&state=${state}`);
    expect(await (await load()).initialize()).toBe(true);
    expect(server.requests).toHaveLength(1);

    const earlier = storeSession("alice");
    openAt(`#signin=mallory-token&state=${state}`);
    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(server.requests).toHaveLength(1);
    expect(localStorage.getItem("auth_token")).toBe(earlier);
    expect(localStorage.getItem("auth_username")).toBe("alice");
  });

  it("spends the state on a sign-in token that did not carry it", async () => {
    const server = fakeServer(() => ({ status: 200, data: {} }));
    axios.defaults.adapter = server.adapter;
    const state = await startSignIn();
    openAt("#signin=mallory-token&state=not-the-one");
    await (await load()).initialize();

    openAt(`#signin=late-token&state=${state}`);
    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(server.requests).toHaveLength(0);
  });

  it("ends the session when the server answers no expiry for it", async () => {
    const server = fakeServer(() => ({
      status: 200,
      data: {
        access_token: "opaque-access-token",
        refresh_token: "core-refresh",
        token_type: "bearer",
      },
    }));
    axios.defaults.adapter = server.adapter;
    openAt(`#signin=one-time-token&state=${await startSignIn()}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_hub_session")).toBeNull();
  });
});

const storeHubSession = (expiration: number) => {
  const access = jwt({ sub: "ana", exp: inFifteenMinutes() });
  localStorage.setItem("auth_token", access);
  localStorage.setItem("auth_token_expiration", String(expiration));
  localStorage.setItem("auth_username", "ana");
  localStorage.setItem("auth_refresh_token", "core-refresh");
  localStorage.setItem("auth_hub_session", "1");
  return access;
};

describe("auth service: sign-out and expiry", () => {
  const original = axios.defaults.adapter;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeStorage());
    vi.stubGlobal("sessionStorage", fakeStorage());
    openAt("");
  });

  afterEach(() => {
    axios.defaults.adapter = original;
    vi.unstubAllGlobals();
    openAt("");
  });

  it("clears a password session in this browser and asks the server nothing", async () => {
    storeSession("alice");
    const server = fakeServer(() => ({ status: 204 }));
    axios.defaults.adapter = server.adapter;
    const authService = await load();

    await authService.signOut();

    expect(server.requests).toHaveLength(0);
    expect(authService.isAuthenticated()).toBe(false);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
    expect(localStorage.getItem("auth_username")).toBeNull();
  });

  it("asks the server nothing for a password session whose token expired", async () => {
    localStorage.setItem("auth_refresh_token", "alice-refresh");
    const server = fakeServer(() => ({ status: 200, data: {} }));
    axios.defaults.adapter = server.adapter;
    const authService = await load();

    await authService.signOut();

    expect(server.requests).toHaveLength(0);
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
  });

  it("keeps the estimated hour for a refreshed password session", async () => {
    localStorage.setItem("auth_refresh_token", "alice-refresh");
    const renewed = jwt({ sub: "alice", exp: Math.floor(Date.now() / 1000) + 7200 });
    axios.defaults.adapter = fakeServer(() => ({
      status: 200,
      data: { access_token: renewed, refresh_token: "alice-refresh-2", token_type: "bearer" },
    })).adapter;
    const authService = await load();
    const before = Date.now();

    expect(await authService.refreshAccessToken()).toBe(renewed);

    const expiration = Number(localStorage.getItem("auth_token_expiration"));
    expect(expiration).toBeGreaterThanOrEqual(before + 3_600_000);
    expect(expiration).toBeLessThanOrEqual(Date.now() + 3_600_000);
  });

  it("keeps the expiry the server answers for a refreshed hub session", async () => {
    storeHubSession(Date.now() + 60_000);
    const expiresAt = Date.now() + 900_000;
    axios.defaults.adapter = fakeServer(() => ({
      status: 200,
      data: {
        access_token: "renewed",
        refresh_token: "core-refresh-2",
        token_type: "bearer",
        expires_at: expiresAt,
      },
    })).adapter;
    const authService = await load();

    expect(await authService.refreshAccessToken()).toBe("renewed");

    expect(localStorage.getItem("auth_token_expiration")).toBe(String(expiresAt));
    expect(localStorage.getItem("auth_hub_session")).toBe("1");
  });

  it("tells the server of a hub sign-out before it clears the session", async () => {
    const access = storeHubSession(Date.now() + 600_000);
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
    expect(localStorage.getItem("auth_hub_session")).toBeNull();
  });

  it("refreshes an expired hub session to sign it out", async () => {
    storeHubSession(Date.now() - 1000);
    const server = fakeServer((config) =>
      config.url === "/auth/refresh"
        ? { status: 200, data: { access_token: "renewed", token_type: "bearer" } }
        : { status: 204 },
    );
    axios.defaults.adapter = server.adapter;
    const authService = await load();

    await authService.signOut();

    expect(server.requests.map(({ config }) => config.url)).toEqual([
      "/auth/refresh",
      "/auth/logout",
    ]);
    expect(server.requests[1].config.headers.Authorization).toBe("Bearer renewed");
    expect(localStorage.getItem("auth_hub_session")).toBeNull();
  });

  it.each([
    ["a refusal", { status: 401, data: { detail: "Not authenticated" } }],
    ["no answer", new Error("Network Error")],
  ])("clears a hub session on a sign-out the server meets with %s", async (_label, answer) => {
    storeHubSession(Date.now() + 600_000);
    axios.defaults.adapter = fakeServer(() => answer).adapter;
    const authService = await load();

    await authService.signOut();

    expect(authService.isAuthenticated()).toBe(false);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
    expect(localStorage.getItem("auth_hub_session")).toBeNull();
  });
});
