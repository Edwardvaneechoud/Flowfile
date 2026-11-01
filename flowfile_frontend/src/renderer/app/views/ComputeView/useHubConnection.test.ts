import { beforeEach, describe, expect, it, vi } from "vitest";
import { httpError } from "../../__fixtures__/promises";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  connect: vi.fn(),
  disconnect: vi.fn(),
}));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: mocks.connect,
  disconnectHub: mocks.disconnect,
}));

import { useHubConnection } from "./useHubConnection";

const none: HubConnection = {
  connected: false,
  hubUrl: null,
  deviceId: null,
  deviceName: "ana-laptop",
  expiresAt: null,
  clientVersion: "0.24.0",
  contractVersion: "0.1.0",
  orgName: null,
  hubVersion: null,
  coreVersion: null,
  vendMode: null,
  reachable: false,
  error: null,
};

const connected: HubConnection = {
  ...none,
  connected: true,
  hubUrl: "https://hub.example.test",
  deviceId: 3,
  expiresAt: "2026-11-08T12:00:00Z",
  orgName: "Acme Data",
  hubVersion: "0.3.0",
  coreVersion: "0.24.0",
  vendMode: "sts",
  reachable: true,
};

const input = { hubUrl: "https://hub.example.test", username: "ana", password: "s3cret" };

beforeEach(() => {
  mocks.get.mockReset().mockResolvedValue(none);
  mocks.connect.mockReset().mockResolvedValue(connected);
  mocks.disconnect.mockReset().mockResolvedValue(undefined);
});

describe("useHubConnection", () => {
  it("loads the connection as core reports it", async () => {
    const hub = useHubConnection();

    await hub.load();

    expect(hub.state.value).toEqual(none);
    expect(hub.loadError.value).toBe("");
  });

  it("keeps the route's words when loading fails", async () => {
    mocks.get.mockRejectedValue(httpError(500, "The secure store could not be read"));
    const hub = useHubConnection();

    await hub.load();

    expect(hub.state.value).toBeNull();
    expect(hub.loadError.value).toBe("The secure store could not be read");
  });

  it("connects and takes the answered state", async () => {
    const hub = useHubConnection();
    await hub.load();

    const next = await hub.connect(input);

    expect(mocks.connect).toHaveBeenCalledWith(input);
    expect(next.connected).toBe(true);
    expect(hub.state.value?.orgName).toBe("Acme Data");
    expect(hub.busy.value).toBe(false);
  });

  it("surfaces a version refusal in the hub's words and keeps the state", async () => {
    mocks.connect.mockRejectedValue(
      httpError(426, "client 0.24.0 is not supported by this hub (core 0.30.0)"),
    );
    const hub = useHubConnection();
    await hub.load();

    await expect(hub.connect(input)).rejects.toThrow(
      "client 0.24.0 is not supported by this hub (core 0.30.0)",
    );
    expect(hub.state.value).toEqual(none);
    expect(hub.busy.value).toBe(false);
  });

  it("falls back to its own words when the refusal carries none", async () => {
    mocks.connect.mockRejectedValue(new Error("Network Error"));
    const hub = useHubConnection();

    await expect(hub.connect(input)).rejects.toThrow("Couldn't connect to the hub.");
  });

  it("disconnects and reloads the not-connected state", async () => {
    mocks.get.mockResolvedValueOnce(connected);
    const hub = useHubConnection();
    await hub.load();

    await hub.disconnect();

    expect(mocks.disconnect).toHaveBeenCalledOnce();
    expect(hub.state.value).toEqual(none);
  });
});
