import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { httpError } from "../../__fixtures__/promises";
import type { HubConnection } from "../../api/hubConnection.api";
import type { ComputeBackend } from "../../api/computeBackends.api";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  connect: vi.fn(),
  disconnect: vi.fn(),
  sync: vi.fn(),
  check: vi.fn(),
  listBackends: vi.fn(),
}));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: mocks.connect,
  disconnectHub: mocks.disconnect,
  syncHubComputeNodes: mocks.sync,
  checkHubComputeNodes: mocks.check,
}));

vi.mock("../../api/computeBackends.api", () => ({
  listComputeBackends: mocks.listBackends,
}));

import {
  hubNodesOf,
  nodeNameOf,
  REREAD_DELAY_MS,
  syncNoticeOf,
  teamNodeRows,
  useHubConnection,
} from "./useHubConnection";

const teamNode: ComputeBackend = {
  id: "worker:team",
  url: "https://team-worker.example.test:63579",
  shares_core_disk: false,
  verdict: null,
  source: "hub",
  reachable: null,
  reason: null,
};
const ownWorker: ComputeBackend = {
  id: "worker:own",
  url: "https://own.example.test:63579",
  shares_core_disk: false,
  verdict: null,
  source: "local",
  reachable: null,
  reason: null,
};

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
  reachable: false,
  error: null,
  nodesSynced: null,
  syncError: null,
  nodeName: null,
  account: null,
  joined: false,
  organisationStorage: null,
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
  reachable: true,
};

const input = { hubUrl: "https://hub.example.test", username: "ana", password: "s3cret" };

beforeEach(() => {
  mocks.get.mockReset().mockResolvedValue(none);
  mocks.connect.mockReset().mockResolvedValue({ ...connected, nodesSynced: 1 });
  mocks.disconnect.mockReset().mockResolvedValue(undefined);
  mocks.sync.mockReset().mockResolvedValue({ backends: [ownWorker, teamNode], skipped: [] });
  mocks.check.mockReset().mockResolvedValue([{ ...teamNode, reachable: true }]);
  mocks.listBackends.mockReset().mockResolvedValue([ownWorker, teamNode]);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("hub nodes", () => {
  it("keeps the rows the hub filled and drops the ones added here", () => {
    expect(hubNodesOf([ownWorker, teamNode])).toEqual([teamNode]);
    expect(nodeNameOf(teamNode)).toBe("team");
  });

  it("lists this core's own node first, as This node with nothing behind it, only when it is one", () => {
    const own = { ...connected, nodeName: "team-local" };

    expect(teamNodeRows(own, [teamNode])).toEqual([
      { key: "this-node", name: "team-local", label: "This node", backend: null },
      { key: "worker:team", name: "team", label: null, backend: teamNode },
    ]);
    expect(teamNodeRows(connected, [teamNode])).toEqual([
      { key: "worker:team", name: "team", label: null, backend: teamNode },
    ]);
    expect(teamNodeRows(null, [])).toEqual([]);
    expect(teamNodeRows(own, [])).toEqual([
      { key: "this-node", name: "team-local", label: "This node", backend: null },
    ]);
  });

  it("words a connect's sync outcome", () => {
    expect(syncNoticeOf(none)).toBe("");
    expect(syncNoticeOf({ ...connected, nodesSynced: 1 })).toBe("1 team node synced");
    expect(syncNoticeOf({ ...connected, nodesSynced: 3 })).toBe("3 team nodes synced");
    expect(syncNoticeOf({ ...connected, syncError: "Could not reach the hub" })).toBe(
      "Team nodes were not synced: Could not reach the hub",
    );
  });
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

  it("disconnects, reloads the not-connected state and empties the team nodes", async () => {
    mocks.get.mockResolvedValueOnce(connected);
    const hub = useHubConnection();
    await hub.load();
    expect(hub.nodes.value).toEqual([teamNode]);

    await hub.disconnect();

    expect(mocks.disconnect).toHaveBeenCalledOnce();
    expect(hub.state.value).toEqual(none);
    expect(hub.nodes.value).toEqual([]);
    expect(hub.syncNotice.value).toBe("");
  });

  it("lists the hub's team nodes only while connected", async () => {
    const hub = useHubConnection();

    await hub.load();

    expect(mocks.listBackends).not.toHaveBeenCalled();
    expect(hub.nodes.value).toEqual([]);
  });

  it("lists no team nodes on a joined server, whose compute page shows the hub's servers", async () => {
    mocks.get.mockResolvedValue({ ...connected, joined: true });
    const hub = useHubConnection();

    await hub.load();

    expect(hub.state.value?.joined).toBe(true);
    expect(mocks.listBackends).not.toHaveBeenCalled();
  });

  it("reads the team nodes from the backends after a connect and keeps the sync count", async () => {
    const hub = useHubConnection();
    await hub.load();

    await hub.connect(input);

    expect(hub.nodes.value).toEqual([teamNode]);
    expect(hub.syncNotice.value).toBe("1 team node synced");
  });

  it("shows why a connect's node sync failed while the connection stands", async () => {
    mocks.connect.mockResolvedValue({ ...connected, syncError: "compute nodes unavailable" });
    mocks.listBackends.mockResolvedValue([ownWorker]);
    const hub = useHubConnection();

    const next = await hub.connect(input);

    expect(next.connected).toBe(true);
    expect(hub.nodes.value).toEqual([]);
    expect(hub.syncNotice.value).toBe("Team nodes were not synced: compute nodes unavailable");
  });

  it("syncs the nodes on request and takes the rows the route answers", async () => {
    const gpu: ComputeBackend = { ...teamNode, id: "worker:gpu" };
    mocks.sync.mockResolvedValue({ backends: [ownWorker, teamNode, gpu], skipped: [] });
    const hub = useHubConnection();

    const count = await hub.syncNodes();

    expect(mocks.sync).toHaveBeenCalledOnce();
    expect(count).toBe(2);
    expect(hub.nodes.value).toEqual([teamNode, gpu]);
    expect(hub.syncNotice.value).toBe("2 team nodes synced");
    expect(hub.syncing.value).toBe(false);
  });

  it("checks the nodes on request and replaces them with the checked rows", async () => {
    const gpu: ComputeBackend = { ...teamNode, id: "worker:gpu" };
    mocks.get.mockResolvedValueOnce(connected);
    mocks.listBackends.mockResolvedValue([ownWorker, teamNode, gpu]);
    const unreachable = {
      ...gpu,
      reachable: false,
      reason: "can't reach https://team-worker.example.test:63579 from this computer: no route",
    };
    mocks.check.mockResolvedValue([ownWorker, { ...teamNode, reachable: true }, unreachable]);
    const hub = useHubConnection();
    await hub.load();
    expect(hub.nodes.value).toEqual([teamNode, gpu]);

    const checked = await hub.checkNodes();

    expect(mocks.check).toHaveBeenCalledOnce();
    expect(checked).toEqual([{ ...teamNode, reachable: true }, unreachable]);
    expect(hub.nodes.value).toEqual([{ ...teamNode, reachable: true }, unreachable]);
    expect(hub.checking.value).toBe(false);
  });

  it("surfaces a refused check in the route's words and keeps the list", async () => {
    mocks.check.mockRejectedValue(httpError(400, "Connect to a hub first (Compute → Hub)."));
    mocks.get.mockResolvedValueOnce(connected);
    const hub = useHubConnection();
    await hub.load();

    await expect(hub.checkNodes()).rejects.toThrow("Connect to a hub first (Compute → Hub).");
    expect(hub.nodes.value).toEqual([teamNode]);
    expect(hub.checking.value).toBe(false);
  });

  it("names the worker added here that a sync left alone", async () => {
    mocks.sync.mockResolvedValue({ backends: [ownWorker, teamNode], skipped: ["worker:own"] });
    const hub = useHubConnection();

    await hub.syncNodes();

    expect(hub.syncNotice.value).toBe(
      "1 team node synced; kept the worker added here under worker:own",
    );
  });

  it("surfaces a refused sync in the route's words and keeps the list", async () => {
    mocks.sync.mockRejectedValue(
      httpError(502, "Could not reach the hub at https://hub.example.test."),
    );
    mocks.get.mockResolvedValueOnce(connected);
    const hub = useHubConnection();
    await hub.load();

    await expect(hub.syncNodes()).rejects.toThrow(
      "Could not reach the hub at https://hub.example.test.",
    );
    expect(hub.nodes.value).toEqual([teamNode]);
    expect(hub.syncNotice.value).toBe("Could not reach the hub at https://hub.example.test.");
    expect(hub.syncing.value).toBe(false);
  });

  describe("the delayed re-read", () => {
    const checked: ComputeBackend = { ...teamNode, reachable: true };

    it("reads the nodes once more after a connect, once the check behind it has had time to answer", async () => {
      vi.useFakeTimers();
      mocks.listBackends
        .mockResolvedValueOnce([ownWorker, teamNode])
        .mockResolvedValue([ownWorker, checked]);
      const hub = useHubConnection();

      await hub.connect(input);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);
      expect(hub.nodes.value).toEqual([teamNode]);

      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS - 1);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(mocks.listBackends).toHaveBeenCalledTimes(2);
      expect(hub.nodes.value).toEqual([checked]);

      await vi.advanceTimersByTimeAsync(10 * REREAD_DELAY_MS);
      expect(mocks.listBackends).toHaveBeenCalledTimes(2);
    });

    it("reads the nodes once more after a sync, and once for two syncs within the delay", async () => {
      vi.useFakeTimers();
      mocks.listBackends.mockResolvedValue([ownWorker, checked]);
      const hub = useHubConnection();

      await hub.syncNodes();
      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS / 2);
      await hub.syncNodes();
      expect(hub.nodes.value).toEqual([teamNode]);
      expect(mocks.listBackends).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS / 2);
      expect(mocks.listBackends).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS / 2);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);
      expect(hub.nodes.value).toEqual([checked]);

      await vi.advanceTimersByTimeAsync(10 * REREAD_DELAY_MS);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);
    });

    it("is dropped by a disconnect, and by a check that already answered the rows", async () => {
      vi.useFakeTimers();
      const hub = useHubConnection();

      await hub.connect(input);
      await hub.disconnect();
      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);
      expect(hub.nodes.value).toEqual([]);

      await hub.syncNodes();
      await hub.checkNodes();
      await vi.advanceTimersByTimeAsync(REREAD_DELAY_MS);
      expect(mocks.listBackends).toHaveBeenCalledTimes(1);
      expect(hub.nodes.value).toEqual([checked]);
    });
  });
});
