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
});
