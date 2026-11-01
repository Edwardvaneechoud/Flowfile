import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";

vi.mock("../services/axios.config", () => ({
  default: { get: vi.fn(), post: vi.fn(), delete: vi.fn() },
}));

const loadModule = async () => {
  vi.resetModules();
  const axios = (await import("../services/axios.config")).default;
  const api = await import("./hubConnection.api");
  return {
    get: axios.get as unknown as Mock,
    post: axios.post as unknown as Mock,
    del: axios.delete as unknown as Mock,
    api,
  };
};

const pyConnected = {
  connected: true,
  hub_url: "https://hub.example.test",
  device_id: 3,
  device_name: "ana-laptop",
  expires_at: "2026-11-08T12:00:00Z",
  client_version: "0.24.0",
  contract_version: "0.1.0",
  org_name: "Acme Data",
  hub_version: "0.3.0",
  core_version: "0.24.0",
  vend_mode: "static",
  reachable: true,
  error: null,
};

const connected = {
  connected: true,
  hubUrl: "https://hub.example.test",
  deviceId: 3,
  deviceName: "ana-laptop",
  expiresAt: "2026-11-08T12:00:00Z",
  clientVersion: "0.24.0",
  contractVersion: "0.1.0",
  orgName: "Acme Data",
  hubVersion: "0.3.0",
  coreVersion: "0.24.0",
  vendMode: "static",
  reachable: true,
  error: null,
};

describe("hubConnection.api", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("gets /hub_client/connection without a trailing slash and maps snake_case to camelCase", async () => {
    const { get, api } = await loadModule();
    get.mockResolvedValueOnce({ data: pyConnected });

    const result = await api.getHubConnection();

    expect(get).toHaveBeenCalledWith("/hub_client/connection");
    expect(result).toEqual(connected);
  });

  it("posts the URL, the account and the device name in the route's field names", async () => {
    const { post, api } = await loadModule();
    post.mockResolvedValueOnce({ data: pyConnected });

    const result = await api.connectHub({
      hubUrl: "https://hub.example.test",
      username: "ana",
      password: "s3cret",
      deviceName: "ana-laptop",
    });

    expect(post).toHaveBeenCalledWith("/hub_client/connection", {
      hub_url: "https://hub.example.test",
      username: "ana",
      password: "s3cret",
      device_name: "ana-laptop",
    });
    expect(result).toEqual(connected);
  });

  it("sends no device name when none was given, so core registers the hostname", async () => {
    const { post, api } = await loadModule();
    post.mockResolvedValueOnce({ data: pyConnected });

    await api.connectHub({ hubUrl: "https://hub.example.test", username: "ana", password: "x" });

    expect(post.mock.calls[0][1].device_name).toBeNull();
  });

  it("deletes /hub_client/connection to disconnect", async () => {
    const { del, api } = await loadModule();
    del.mockResolvedValueOnce({ data: { connected: false } });

    await api.disconnectHub();

    expect(del).toHaveBeenCalledWith("/hub_client/connection");
  });

  it("propagates a refusal so the panel can show the route's words", async () => {
    const { post, api } = await loadModule();
    post.mockRejectedValueOnce({
      response: { status: 426, data: { detail: "client 0.24.0 is not supported" } },
    });

    await expect(
      api.connectHub({ hubUrl: "https://hub.example.test", username: "ana", password: "x" }),
    ).rejects.toMatchObject({ response: { status: 426 } });
  });
});
