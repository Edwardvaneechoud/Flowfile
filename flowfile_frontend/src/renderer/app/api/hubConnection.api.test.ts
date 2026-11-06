import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), del: vi.fn() }));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post, delete: mocks.del },
}));

import {
  connectHub,
  disconnectHub,
  getHubConnection,
  syncHubComputeNodes,
} from "./hubConnection.api";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("hubConnection.api", () => {
  it("gets /hub_client/connection without a trailing slash", async () => {
    mocks.get.mockResolvedValueOnce({ data: { connected: false } });

    expect(await getHubConnection()).toEqual({ connected: false });
    expect(mocks.get).toHaveBeenCalledWith("/hub_client/connection");
  });

  it("posts the URL, the account and the device name to /hub_client/connection", async () => {
    const input = {
      hub_url: "https://hub.example.test",
      username: "ana",
      password: "s3cret",
      device_name: "ana-laptop",
    };
    mocks.post.mockResolvedValueOnce({ data: { connected: true } });

    expect(await connectHub(input)).toEqual({ connected: true });
    expect(mocks.post).toHaveBeenCalledWith("/hub_client/connection", input);
  });

  it("posts /hub_client/compute_nodes/sync and returns the rows and the skipped ids", async () => {
    const answer = { backends: [], skipped: ["worker:gpu"] };
    mocks.post.mockResolvedValueOnce({ data: answer });

    expect(await syncHubComputeNodes()).toEqual(answer);
    expect(mocks.post).toHaveBeenCalledWith("/hub_client/compute_nodes/sync");
  });

  it("deletes /hub_client/connection to disconnect", async () => {
    mocks.del.mockResolvedValueOnce({ data: { connected: false } });

    await disconnectHub();

    expect(mocks.del).toHaveBeenCalledWith("/hub_client/connection");
  });

  it("propagates a refusal so the panel can show the route's words", async () => {
    mocks.post.mockRejectedValueOnce({
      response: { status: 426, data: { detail: "client 0.24.0 is not supported" } },
    });

    await expect(
      connectHub({ hub_url: "https://hub.example.test", username: "ana", password: "x" }),
    ).rejects.toMatchObject({ response: { status: 426 } });
  });
});
