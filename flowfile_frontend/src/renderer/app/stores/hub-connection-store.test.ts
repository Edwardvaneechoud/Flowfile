import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { desktop, none } from "../__fixtures__/hubConnection";
import { httpError } from "../__fixtures__/promises";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  connect: vi.fn(),
  disconnect: vi.fn(),
  start: vi.fn(),
  status: vi.fn(),
  cancel: vi.fn(),
}));

vi.mock("../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: mocks.connect,
  disconnectHub: mocks.disconnect,
  startBrowserSignIn: mocks.start,
  getBrowserSignIn: mocks.status,
  cancelBrowserSignIn: mocks.cancel,
  PASSWORD_ONLY: "password_only",
}));

import {
  BROWSER_POLL_MS,
  BrowserSignInUnavailable,
  useHubConnectionStore,
} from "./hub-connection-store";

const input = { hub_url: "https://hub.example.test", username: "ana", password: "s3cret" };

beforeEach(() => {
  setActivePinia(createPinia());
  mocks.get.mockReset().mockResolvedValue(none);
  mocks.connect.mockReset().mockResolvedValue(desktop);
  mocks.disconnect.mockReset().mockResolvedValue(undefined);
  mocks.start
    .mockReset()
    .mockResolvedValue({ flow_id: "f1", authorize_url: "https://hub/authorize" });
  mocks.status.mockReset().mockResolvedValue({ status: "waiting", detail: null, connection: null });
  mocks.cancel.mockReset().mockResolvedValue(undefined);
});

afterEach(() => {
  vi.useRealTimers();
});

const browserInput = { hub_url: "https://hub.example.test", device_name: "ana-laptop" };

describe("the hub connection store", () => {
  it("answers readers that load together from one request, and asks again for a later load", async () => {
    const hub = useHubConnectionStore();

    await Promise.all([hub.load(), hub.load(), hub.load()]);
    expect(mocks.get).toHaveBeenCalledTimes(1);
    expect(hub.connection).toEqual(none);
    expect(hub.loading).toBe(false);

    await hub.load();
    expect(mocks.get).toHaveBeenCalledTimes(2);
  });

  it("keeps the route's words when loading fails", async () => {
    mocks.get.mockRejectedValue(httpError(500, "The secure store could not be read"));
    const hub = useHubConnectionStore();

    await hub.load();

    expect(hub.connection).toBeNull();
    expect(hub.loadError).toBe("The secure store could not be read");
  });

  it("connects and takes the answered state, which every reader of the store then sees", async () => {
    const hub = useHubConnectionStore();
    await hub.load();

    const next = await hub.connect(input);

    expect(mocks.connect).toHaveBeenCalledWith(input);
    expect(next.connected).toBe(true);
    expect(hub.connection?.org_name).toBe("Acme Data");
    expect(hub.connected).toBe(true);
    expect(hub.busy).toBe(false);
    expect(mocks.get).toHaveBeenCalledTimes(1);
  });

  it("surfaces a version refusal in the hub's words and keeps the state", async () => {
    mocks.connect.mockRejectedValue(
      httpError(426, "client 0.24.0 is not supported by this hub (core 0.30.0)"),
    );
    const hub = useHubConnectionStore();
    await hub.load();

    await expect(hub.connect(input)).rejects.toThrow(
      "client 0.24.0 is not supported by this hub (core 0.30.0)",
    );
    expect(hub.connection).toEqual(none);
    expect(hub.connected).toBe(false);
    expect(hub.busy).toBe(false);
  });

  it("keeps the connection on a refused disconnect, then disconnects and reloads the not-connected state", async () => {
    mocks.get.mockResolvedValueOnce(desktop);
    const hub = useHubConnectionStore();
    await hub.load();
    expect(hub.connected).toBe(true);

    mocks.disconnect.mockRejectedValueOnce(httpError(403, "Only an administrator can disconnect."));
    await expect(hub.disconnect()).rejects.toThrow("Only an administrator can disconnect.");
    expect(hub.connected).toBe(true);
    expect(hub.busy).toBe(false);

    await hub.disconnect();

    expect(mocks.disconnect).toHaveBeenCalledTimes(2);
    expect(hub.connection).toEqual(none);
    expect(hub.connected).toBe(false);
  });

  it("signs in through the browser: opens the page, polls until connected, and takes the connection", async () => {
    vi.useFakeTimers();
    const hub = useHubConnectionStore();
    await hub.load();
    const open = vi.fn();

    const done = hub.signInWithBrowser(browserInput, open);
    await vi.advanceTimersByTimeAsync(0);

    expect(mocks.start).toHaveBeenCalledWith(browserInput);
    expect(open).toHaveBeenCalledWith("https://hub/authorize");
    expect(hub.waitingForBrowser).toBe(true);
    expect(hub.browserSignInUrl).toBe("https://hub/authorize");

    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);
    expect(mocks.status).toHaveBeenCalledTimes(1);
    mocks.status.mockResolvedValue({ status: "connected", detail: null, connection: desktop });
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);

    await expect(done).resolves.toEqual(desktop);
    expect(hub.connected).toBe(true);
    expect(hub.waitingForBrowser).toBe(false);
    expect(mocks.status).toHaveBeenCalledWith("f1");
  });

  it("throws a failed sign-in's reason and stops waiting", async () => {
    vi.useFakeTimers();
    mocks.status.mockResolvedValue({
      status: "failed",
      detail: "The sign-in took too long.",
      connection: null,
    });
    const hub = useHubConnectionStore();

    const done = hub.signInWithBrowser(browserInput, vi.fn());
    const outcome = expect(done).rejects.toThrow("The sign-in took too long.");
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);

    await outcome;
    expect(hub.waitingForBrowser).toBe(false);
    expect(hub.connected).toBe(false);
  });

  it("answers null on cancel, tells core, and polls no more", async () => {
    vi.useFakeTimers();
    const hub = useHubConnectionStore();

    const done = hub.signInWithBrowser(browserInput, vi.fn());
    await vi.advanceTimersByTimeAsync(0);
    await hub.cancelBrowserSignIn();
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS * 3);

    await expect(done).resolves.toBeNull();
    expect(mocks.cancel).toHaveBeenCalledWith("f1");
    expect(mocks.status).not.toHaveBeenCalled();
    expect(hub.waitingForBrowser).toBe(false);
  });

  it("throws BrowserSignInUnavailable with core's sentence for a hub that takes only a password", async () => {
    mocks.start.mockRejectedValue(
      httpError(400, { kind: "password_only", message: "The hub does not offer it." }),
    );
    const hub = useHubConnectionStore();
    const open = vi.fn();

    const error = await hub.signInWithBrowser(browserInput, open).catch((e) => e);

    expect(error).toBeInstanceOf(BrowserSignInUnavailable);
    expect(error.message).toBe("The hub does not offer it.");
    expect(open).not.toHaveBeenCalled();
    expect(hub.waitingForBrowser).toBe(false);
    expect(hub.busy).toBe(false);
  });

  it("throws any other refusal to start in the route's words", async () => {
    mocks.start.mockRejectedValue(
      httpError(502, "Could not reach the hub at https://hub.example.test."),
    );
    const hub = useHubConnectionStore();

    const error = await hub.signInWithBrowser(browserInput, vi.fn()).catch((e) => e);

    expect(error).not.toBeInstanceOf(BrowserSignInUnavailable);
    expect(error.message).toBe("Could not reach the hub at https://hub.example.test.");
  });
});
