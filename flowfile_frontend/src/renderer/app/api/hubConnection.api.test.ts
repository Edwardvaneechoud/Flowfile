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

// Core answers more (device id, versions, the connect's sync count); the editor reads these.
const pyConnected = {
  connected: true,
  hub_url: "https://hub.example.test",
  device_id: 3,
  device_name: "ana-laptop",
  expires_at: "2026-11-08T12:00:00Z",
  client_version: "0.24.0",
  org_name: "Acme Data",
  hub_version: "0.3.0",
  reachable: true,
  error: null,
  account: "Ana Lima",
};

const connected = {
  connected: true,
  hubUrl: "https://hub.example.test",
  deviceName: "ana-laptop",
  expiresAt: "2026-11-08T12:00:00Z",
  orgName: "Acme Data",
  hubVersion: "0.3.0",
  reachable: true,
  error: null,
  account: "Ana Lima",
  joined: false,
  organisationStorage: null,
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

  it("reads no account from a core that does not answer one", async () => {
    const { get, api } = await loadModule();
    get.mockResolvedValueOnce({ data: { ...pyConnected, account: undefined } });

    const result = await api.getHubConnection();

    expect(result.account).toBeNull();
  });

  it("maps a joined server and the organisation's storage its hub shows", async () => {
    const { get, api } = await loadModule();
    get.mockResolvedValueOnce({
      data: { ...pyConnected, joined: true, organisation_storage: "s3://acme/flowfile" },
    });

    const result = await api.getHubConnection();

    expect([result.joined, result.organisationStorage]).toEqual([true, "s3://acme/flowfile"]);
  });

  it("posts /hub_client/compute_nodes/sync and returns the rows and the skipped ids", async () => {
    const { post, api } = await loadModule();
    const answer = {
      backends: [
        {
          id: "worker:team",
          url: "https://team-worker.example.test:63579",
          shares_core_disk: false,
          verdict: null,
          source: "hub",
          reachable: null,
          reason: null,
        },
      ],
      skipped: ["worker:gpu"],
    };
    post.mockResolvedValueOnce({ data: answer });

    const result = await api.syncHubComputeNodes();

    expect(post).toHaveBeenCalledWith("/hub_client/compute_nodes/sync");
    expect(result).toEqual(answer);
  });

  it("posts /hub_client/compute_nodes/check and returns the checked rows", async () => {
    const { post, api } = await loadModule();
    const rows = [
      {
        id: "worker:team",
        url: "https://team-worker.example.test:63579",
        shares_core_disk: false,
        verdict: null,
        source: "hub",
        reachable: false,
        reason:
          "can't reach https://team-worker.example.test:63579 from this computer: name not known",
      },
    ];
    post.mockResolvedValueOnce({ data: rows });

    const result = await api.checkHubComputeNodes();

    expect(post).toHaveBeenCalledWith("/hub_client/compute_nodes/check");
    expect(result).toEqual(rows);
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
