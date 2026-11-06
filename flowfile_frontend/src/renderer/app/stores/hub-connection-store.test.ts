import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { desktop, none } from "../__fixtures__/hubConnection";
import { httpError } from "../__fixtures__/promises";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  connect: vi.fn(),
  disconnect: vi.fn(),
}));

vi.mock("../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: mocks.connect,
  disconnectHub: mocks.disconnect,
}));

import { useHubConnectionStore } from "./hub-connection-store";

const input = { hub_url: "https://hub.example.test", username: "ana", password: "s3cret" };

beforeEach(() => {
  setActivePinia(createPinia());
  mocks.get.mockReset().mockResolvedValue(none);
  mocks.connect.mockReset().mockResolvedValue(desktop);
  mocks.disconnect.mockReset().mockResolvedValue(undefined);
});

describe("the hub connection store", () => {
  it("knows nothing until loaded, then the connection as core reports it", async () => {
    const hub = useHubConnectionStore();
    expect(hub.connection).toBeNull();
    expect(hub.connected).toBe(false);

    await hub.load();

    expect(hub.connection).toEqual(none);
    expect(hub.loadError).toBe("");
    expect(hub.loading).toBe(false);
  });

  it("answers readers that load together from one request, and asks again for a later load", async () => {
    const hub = useHubConnectionStore();

    await Promise.all([hub.load(), hub.load(), hub.load()]);
    expect(mocks.get).toHaveBeenCalledTimes(1);

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

  it("derives connected and joined from the connection, so a server joined to its hub counts as connected", async () => {
    mocks.get.mockResolvedValue({ ...desktop, joined: true });
    const hub = useHubConnectionStore();

    await hub.load();

    expect(hub.connected).toBe(true);
    expect(hub.joined).toBe(true);

    mocks.get.mockResolvedValue(desktop);
    await hub.load();
    expect(hub.connected).toBe(true);
    expect(hub.joined).toBe(false);
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

  it("falls back to its own words when the refusal carries none", async () => {
    mocks.connect.mockRejectedValue(new Error("Network Error"));
    const hub = useHubConnectionStore();

    await expect(hub.connect(input)).rejects.toThrow("Couldn't connect to the hub.");
  });

  it("disconnects and reloads the not-connected state", async () => {
    mocks.get.mockResolvedValueOnce(desktop);
    const hub = useHubConnectionStore();
    await hub.load();
    expect(hub.connected).toBe(true);

    await hub.disconnect();

    expect(mocks.disconnect).toHaveBeenCalledOnce();
    expect(hub.connection).toEqual(none);
    expect(hub.connected).toBe(false);
  });

  it("surfaces a refused disconnect in the route's words and keeps the connection", async () => {
    mocks.get.mockResolvedValue(desktop);
    mocks.disconnect.mockRejectedValue(httpError(403, "Only an administrator can disconnect."));
    const hub = useHubConnectionStore();
    await hub.load();

    await expect(hub.disconnect()).rejects.toThrow("Only an administrator can disconnect.");
    expect(hub.connected).toBe(true);
    expect(hub.busy).toBe(false);
  });
});
