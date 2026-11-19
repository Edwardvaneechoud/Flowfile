// @vitest-environment happy-dom
// A sign-out in another tab of this browser reaches this one through localStorage's `storage` event; a new
// sign-in there, which clears the session and sets the new one, does not.
import axios from "axios";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStorage } from "../__fixtures__/mount";
import {
  fakeServer,
  loadSession,
  redirectOf,
  renewed,
  storeSession,
  WARM_UP_MS,
  warmSession,
  watchReloads,
} from "../__fixtures__/session";

const original = axios.defaults.adapter;
let reloads: ReturnType<typeof watchReloads>;
let stopFollowing: (() => void) | undefined;

/** What another tab's change of `key` looks like here: the event arrives after its writes are done. */
const changedElsewhere = (key: string | null) =>
  window.dispatchEvent(new StorageEvent("storage", { key, newValue: null }));

const signOutElsewhere = () => {
  for (const key of [
    "auth_token",
    "auth_token_expiration",
    "auth_refresh_token",
    "auth_username",
    "auth_hub_session",
  ]) {
    localStorage.removeItem(key);
  }
};

const load = async () => {
  const authService = await loadSession(fakeServer(() => renewed()).adapter);
  stopFollowing = authService.followSession();
  return authService;
};

beforeAll(() => warmSession(), WARM_UP_MS);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  vi.stubGlobal("sessionStorage", fakeStorage());
  window.history.replaceState(null, "", "/ui/#/main/designer?flow=3");
  reloads = watchReloads();
});

afterEach(() => {
  stopFollowing?.();
  stopFollowing = undefined;
  reloads.restore();
  axios.defaults.adapter = original;
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("other tabs", () => {
  it.each([
    ["a password session", false],
    ["a session through the hub", true],
  ])("a sign-out in another tab sends this tab to the sign-in page: %s", async (_case, hub) => {
    storeSession(3_600_000, { hub });
    const authService = await load();
    signOutElsewhere();

    changedElsewhere("auth_token");

    expect(reloads.reloads).toHaveLength(1);
    expect(reloads.reloads[0].pathname).toBe("/ui/");
    expect(redirectOf(reloads.reloads[0].hash)).toBe("/main/designer?flow=3");
    expect(authService.isAuthenticated()).toBe(false);
    expect(authService.takeSessionEnded()).toBe(true);
  });

  it("tells no one again: it writes nothing to localStorage", async () => {
    storeSession(3_600_000);
    await load();
    signOutElsewhere();
    const setItem = vi.spyOn(localStorage, "setItem");
    const removeItem = vi.spyOn(localStorage, "removeItem");

    changedElsewhere("auth_token");
    changedElsewhere("auth_refresh_token");

    expect(reloads.reloads).toHaveLength(1);
    expect(setItem).not.toHaveBeenCalled();
    expect(removeItem).not.toHaveBeenCalled();
  });

  it("a sign-in through the hub in another tab, which clears and then sets the session, signs nobody out", async () => {
    storeSession(3_600_000, { hub: true });
    const authService = await load();
    signOutElsewhere();
    localStorage.setItem("auth_token", "bob-token");
    localStorage.setItem("auth_token_expiration", String(Date.now() + 900_000));
    localStorage.setItem("auth_refresh_token", "bob-refresh");
    localStorage.setItem("auth_hub_session", "1");

    changedElsewhere("auth_token");
    changedElsewhere("auth_refresh_token");

    expect(reloads.reloads).toHaveLength(0);
    expect(authService.isAuthenticated()).toBe(true);
  });

  it("a renewal in another tab signs nobody out", async () => {
    storeSession(3_600_000);
    const authService = await load();
    localStorage.setItem("auth_token", "renewed-elsewhere");

    changedElsewhere("auth_token");

    expect(reloads.reloads).toHaveLength(0);
    expect(authService.isAuthenticated()).toBe(true);
  });

  it("a storage cleared in another tab counts as a sign-out; other keys do not", async () => {
    storeSession(3_600_000);
    await load();

    changedElsewhere("flowfile-theme");
    expect(reloads.reloads).toHaveLength(0);

    signOutElsewhere();
    changedElsewhere("flowfile-theme");
    expect(reloads.reloads).toHaveLength(0);

    changedElsewhere(null);
    expect(reloads.reloads).toHaveLength(1);
  });

  it("a tab that holds no session stays where it is", async () => {
    await load();

    changedElsewhere("auth_token");

    expect(reloads.reloads).toHaveLength(0);
  });
});
