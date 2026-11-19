// @vitest-environment happy-dom
// A session that came through the hub is renewed when the person comes back to the tab, at most once a minute,
// so a sign-in that ended at the hub leaves for the sign-in page then. A password session is not.
import axios from "axios";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStorage } from "../__fixtures__/mount";
import {
  fakeServer,
  loadSession,
  refused,
  renewed,
  storeSession,
  WARM_UP_MS,
  warmSession,
  watchReloads,
  type Answer,
} from "../__fixtures__/session";

const original = axios.defaults.adapter;
let reloads: ReturnType<typeof watchReloads>;
let stopFollowing: (() => void) | undefined;

const load = async (renewal: Answer) => {
  const server = fakeServer((config) =>
    config.url === "/auth/refresh" ? renewal : { status: 200, data: {} },
  );
  const authService = await loadSession(server.adapter);
  stopFollowing = authService.followSession();
  return { authService, server };
};

const comeBack = async () => {
  Object.defineProperty(document, "visibilityState", { value: "visible", configurable: true });
  document.dispatchEvent(new Event("visibilitychange"));
  window.dispatchEvent(new Event("focus"));
  await vi.advanceTimersByTimeAsync(0);
};

beforeAll(() => warmSession(), WARM_UP_MS);

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("localStorage", fakeStorage());
  vi.stubGlobal("sessionStorage", fakeStorage());
  window.history.replaceState(null, "", "/ui/#/main/designer");
  reloads = watchReloads();
});

afterEach(() => {
  stopFollowing?.();
  stopFollowing = undefined;
  reloads.restore();
  axios.defaults.adapter = original;
  vi.unstubAllGlobals();
  vi.useRealTimers();
  window.history.replaceState(null, "", "/");
});

describe("renewal on return to the tab", () => {
  it("renews a hub session once on return, and not again within a minute", async () => {
    storeSession(900_000, { hub: true });
    const { server } = await load(renewed());

    await comeBack();
    expect(server.renewals()).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(30_000);
    await comeBack();
    expect(server.renewals()).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(31_000);
    await comeBack();
    expect(server.renewals()).toHaveLength(2);
    expect(reloads.reloads).toHaveLength(0);
  });

  it("does not renew a password session on return", async () => {
    storeSession(900_000);
    const { server } = await load(renewed());

    await comeBack();

    expect(server.renewals()).toHaveLength(0);
  });

  it("does not renew when the tab is hidden", async () => {
    storeSession(900_000, { hub: true });
    const { server } = await load(renewed());

    Object.defineProperty(document, "visibilityState", { value: "hidden", configurable: true });
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.advanceTimersByTimeAsync(0);

    expect(server.renewals()).toHaveLength(0);
  });

  it("a renewal on return the server refuses ends at the sign-in page", async () => {
    storeSession(900_000, { hub: true });
    const { authService } = await load(refused);

    await comeBack();

    expect(reloads.reloads).toHaveLength(1);
    expect(reloads.reloads[0].pathname).toBe("/ui/");
    expect(reloads.reloads[0].hash.startsWith("#/login")).toBe(true);
    expect(authService.isAuthenticated()).toBe(false);
  });

  it("a renewal on return answered 502 keeps the session", async () => {
    storeSession(900_000, { hub: true });
    const { authService } = await load({
      status: 502,
      data: { detail: "The hub did not answer." },
    });

    await comeBack();

    expect(reloads.reloads).toHaveLength(0);
    expect(authService.isAuthenticated()).toBe(true);
    expect(localStorage.getItem("auth_refresh_token")).toBe("ana-refresh");
  });
});
