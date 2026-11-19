// @vitest-environment happy-dom
// A session the server refuses ends in one place: the tokens go, the tab changes only the hash of its address to
// the sign-in page and reloads. A renewal the server did not answer keeps the session.
import axios, { AxiosError } from "axios";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStorage } from "../__fixtures__/mount";
import {
  fakeServer,
  flush,
  loadSession,
  redirectOf,
  refused,
  renewed,
  storeSession,
  WARM_UP_MS,
  warmSession,
  watchReloads,
  within,
  type Answer,
} from "../__fixtures__/session";

const original = axios.defaults.adapter;
let reloads: ReturnType<typeof watchReloads>;

const openAt = (address: string) => window.history.replaceState(null, "", address);

/** Core refuses every other request with 401 and answers the renewal with `renewal`. */
const serverAnswering = (renewal: Answer | (() => Answer)) =>
  fakeServer((config) => {
    if (config.url === "/auth/refresh") return typeof renewal === "function" ? renewal() : renewal;
    return { status: 401, data: { detail: "Could not validate credentials" } };
  });

const statusOf = (error: unknown) => (error as AxiosError).response?.status;

beforeAll(() => warmSession(), WARM_UP_MS);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  vi.stubGlobal("sessionStorage", fakeStorage());
  openAt("/#/main/catalog?tab=runs");
  reloads = watchReloads();
});

afterEach(() => {
  reloads.restore();
  axios.defaults.adapter = original;
  vi.unstubAllGlobals();
  openAt("/");
});

describe("a refused renewal ends the session at the sign-in page", () => {
  it.each([
    ["with an hour left on the token", 3_600_000],
    ["with under five minutes left on the token", 120_000],
  ])("settles %s", async (_case, expiresIn) => {
    storeSession(expiresIn);
    const server = serverAnswering(refused);
    const authService = await loadSession(server.adapter);

    const failure = await within(axios.get("/catalog/namespaces")).catch((error) => error);

    expect(statusOf(failure)).toBe(401);
    expect(reloads.reloads).toHaveLength(1);
    expect(reloads.reloads[0].hash.startsWith("#/login?")).toBe(true);
    expect(redirectOf(reloads.reloads[0].hash)).toBe("/main/catalog?tab=runs");
    expect(authService.isAuthenticated()).toBe(false);
    expect(authService.takeSessionEnded()).toBe(true);
  });

  it("settles with the token expired", async () => {
    storeSession(40);
    const server = serverAnswering(refused);
    const authService = await loadSession(server.adapter);
    await new Promise((resolve) => setTimeout(resolve, 60));

    const failure = await within(axios.get("/catalog/namespaces")).catch((error) => error);

    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).not.toMatch(/still waiting/);
    expect(reloads.reloads).toHaveLength(1);
    expect(redirectOf(reloads.reloads[0].hash)).toBe("/main/catalog?tab=runs");
    expect(authService.isAuthenticated()).toBe(false);
  });

  it("settles when the server refuses the renewal with 403, as one that no longer takes passwords does", async () => {
    storeSession(3_600_000);
    const server = serverAnswering({
      status: 403,
      data: { detail: "Passwords are not taken here" },
    });
    const authService = await loadSession(server.adapter);

    const failure = await within(axios.get("/catalog/namespaces")).catch((error) => error);

    expect(statusOf(failure)).toBe(401);
    expect(server.renewals()).toHaveLength(1);
    expect(reloads.reloads).toHaveLength(1);
    expect(redirectOf(reloads.reloads[0].hash)).toBe("/main/catalog?tab=runs");
    expect(reloads.reloads[0].stored).toEqual([null, null, null, null, null]);
    expect(authService.isAuthenticated()).toBe(false);
    expect(authService.takeSessionEnded()).toBe(true);
  });

  it.each([
    ["an hour", 3_600_000],
    ["under five minutes", 120_000],
  ])(
    "sends the renewal once, with no Bearer header, never again (%s left)",
    async (_case, left) => {
      storeSession(left);
      const server = serverAnswering(refused);
      await loadSession(server.adapter);

      await within(axios.get("/catalog/namespaces")).catch(() => undefined);
      await flush();

      expect(server.renewals()).toHaveLength(1);
      expect(server.renewals()[0].config.headers.Authorization).toBeUndefined();
      expect(
        server.requests.filter(({ config }) => config.url === "/catalog/namespaces"),
      ).toHaveLength(1);
    },
  );

  it("lets a later request settle once the renewal was refused", async () => {
    storeSession(120_000);
    const server = serverAnswering(refused);
    const authService = await loadSession(server.adapter);
    await within(axios.get("/catalog/namespaces")).catch(() => undefined);

    expect(await within(authService.getToken())).toBeNull();
    await within(axios.get("/catalog/namespaces")).catch(() => undefined);
    expect(reloads.reloads).toHaveLength(1);
  });

  it("clears the tokens, in memory and in localStorage, before it leaves", async () => {
    storeSession(3_600_000, { hub: true });
    const server = serverAnswering(refused);
    const authService = await loadSession(server.adapter);

    await within(axios.get("/catalog/namespaces")).catch(() => undefined);

    expect(reloads.reloads[0].stored).toEqual([null, null, null, null, null]);
    expect(authService.isAuthenticated()).toBe(false);
    expect(authService.hasValidToken()).toBe(false);
  });

  it("ends at the sign-in page, not at Home, when another tab took the refresh token away", async () => {
    storeSession(3_600_000);
    const server = serverAnswering(renewed());
    const authService = await loadSession(server.adapter);
    for (const key of [
      "auth_token",
      "auth_token_expiration",
      "auth_refresh_token",
      "auth_username",
    ]) {
      localStorage.removeItem(key);
    }

    await within(axios.get("/catalog/namespaces")).catch(() => undefined);

    expect(server.renewals()).toHaveLength(0);
    expect(reloads.reloads).toHaveLength(1);
    expect(reloads.reloads[0].hash.startsWith("#/login")).toBe(true);
    expect(authService.isAuthenticated()).toBe(false);
  });

  it("ends a refused session that has no refresh token to renew it with", async () => {
    storeSession(3_600_000, { hub: true, refresh: false });
    const server = serverAnswering(renewed());
    const authService = await loadSession(server.adapter);

    const failure = await within(axios.get("/catalog/namespaces")).catch((error) => error);

    expect(statusOf(failure)).toBe(401);
    expect(server.renewals()).toHaveLength(0);
    expect(reloads.reloads).toHaveLength(1);
    expect(reloads.reloads[0].hash.startsWith("#/login?")).toBe(true);
    expect(reloads.reloads[0].stored).toEqual([null, null, null, null, null]);
    expect(authService.isAuthenticated()).toBe(false);
  });

  it("leaves nothing when there was no session to end", async () => {
    const server = serverAnswering(refused);
    const authService = await loadSession(server.adapter);

    await within(axios.get("/catalog/namespaces")).catch(() => undefined);

    expect(await within(authService.getToken())).toBeNull();
    expect(server.renewals()).toHaveLength(0);
    expect(reloads.reloads).toHaveLength(0);
  });
});

describe("the sign-in page's address", () => {
  it.each([
    ["under /ui/", "/ui/#/main/catalog?tab=runs", "/ui/"],
    ["under /", "/#/main/catalog?tab=runs", "/"],
  ])("keeps the path %s and changes only the hash, then reloads", async (_case, address, path) => {
    openAt(address);
    storeSession(3_600_000);
    await loadSession(serverAnswering(refused).adapter);

    await within(axios.get("/catalog/namespaces")).catch(() => undefined);

    expect(reloads.reloads).toHaveLength(1);
    const [left] = reloads.reloads;
    expect(left.pathname).toBe(path);
    expect(left.hash).toBe("#/login?redirect=%2Fmain%2Fcatalog%3Ftab%3Druns");
    expect(redirectOf(left.hash)).toBe("/main/catalog?tab=runs");
  });
});

describe("a renewal the server does not answer keeps the session", () => {
  it.each([
    ["answered 502", { status: 502, data: { detail: "The hub did not answer." } } as Answer],
    ["lost to the network", new Error("Network Error") as Answer],
  ])(
    "%s: the request fails with its own error and the next one asks again",
    async (_case, answer) => {
      storeSession(3_600_000, { hub: true });
      const server = serverAnswering(answer);
      const authService = await loadSession(server.adapter);

      const failure = await within(axios.get("/catalog/namespaces")).catch((error) => error);

      expect(statusOf(failure)).toBe(401);
      expect(reloads.reloads).toHaveLength(0);
      expect(authService.isAuthenticated()).toBe(true);
      expect(localStorage.getItem("auth_token")).toBe("ana-token");
      expect(localStorage.getItem("auth_refresh_token")).toBe("ana-refresh");
      expect(localStorage.getItem("auth_hub_session")).toBe("1");
      expect(server.renewals()).toHaveLength(1);
      expect(
        server.requests.filter(({ config }) => config.url === "/catalog/namespaces"),
      ).toHaveLength(1);

      await within(axios.get("/catalog/namespaces")).catch(() => undefined);
      expect(server.renewals()).toHaveLength(2);
      expect(reloads.reloads).toHaveLength(0);
    },
  );

  it("an expired token whose renewal is not answered keeps its refresh token for the next request", async () => {
    storeSession(40);
    const server = serverAnswering({ status: 502 });
    const authService = await loadSession(server.adapter);
    await new Promise((resolve) => setTimeout(resolve, 60));

    expect(await within(authService.getToken())).toBeNull();
    expect(localStorage.getItem("auth_refresh_token")).toBe("ana-refresh");
    expect(reloads.reloads).toHaveLength(0);
  });
});

describe("a renewal the server grants", () => {
  it("sends the refused request once more with the new token", async () => {
    storeSession(3_600_000);
    const sent: unknown[] = [];
    const server = fakeServer((config) => {
      if (config.url === "/auth/refresh") return renewed("fresh-token");
      sent.push(config.headers.Authorization);
      return sent.length === 1 ? { status: 401 } : { status: 200, data: ["main"] };
    });
    await loadSession(server.adapter);

    const response = await within(axios.get("/catalog/namespaces"));

    expect(response.data).toEqual(["main"]);
    expect(sent).toEqual(["Bearer ana-token", "Bearer fresh-token"]);
    expect(reloads.reloads).toHaveLength(0);
  });
});
