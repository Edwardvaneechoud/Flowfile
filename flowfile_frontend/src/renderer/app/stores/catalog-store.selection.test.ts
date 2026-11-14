// A click on a run, a schedule or a flow shows that item at once, and a response that lands after the
// selection moved on never puts the previous item's data under the current one.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type {
  FlowRun,
  FlowRunDetail,
  FlowSchedule,
  GlobalArtifact,
  PaginatedFlowRuns,
} from "../types";

const api = vi.hoisted(() => ({
  getRuns: vi.fn(),
  getRunDetail: vi.fn(),
  getSchedule: vi.fn(),
  getSchedules: vi.fn(),
  getFlowArtifacts: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({ CatalogApi: api }));

import { useCatalogStore } from "./catalog-store";

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const run = (id: number, registrationId = 1): FlowRun =>
  ({ id, registration_id: registrationId, flow_name: `flow ${registrationId}` }) as FlowRun;

const detail = (id: number): FlowRunDetail =>
  ({ ...run(id), flow_snapshot: null, node_results_json: "[]" }) as FlowRunDetail;

const page = (...items: FlowRun[]): PaginatedFlowRuns => ({
  items,
  total: items.length,
  total_success: 0,
  total_failed: 0,
  total_running: 0,
});

const schedule = (id: number, name = `Schedule ${id}`): FlowSchedule =>
  ({ id, registration_id: 1, name }) as FlowSchedule;

const never = () => deferred<never>().promise;

const flush = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

beforeEach(() => {
  setActivePinia(createPinia());
  vi.resetAllMocks();
});

describe("catalog-store run selection", () => {
  it("selects a run at once from its list row and swaps in the detail when it lands", async () => {
    const store = useCatalogStore();
    store.applyRunsPage(page(run(5)));
    const pending = deferred<FlowRunDetail>();
    api.getRunDetail.mockReturnValue(pending.promise);

    const loading = store.loadRunDetail(5);
    expect(store.selectedRunId).toBe(5);
    expect(store.selectedRun?.id).toBe(5);
    expect(store.selectedRunDetail).toBeNull();

    pending.resolve(detail(5));
    await loading;
    expect(store.selectedRunDetail?.id).toBe(5);
    expect(store.selectedRun).toBe(store.selectedRunDetail);
  });

  it("drops a slower earlier run's detail that lands after a later click", async () => {
    const store = useCatalogStore();
    const first = deferred<FlowRunDetail>();
    const second = deferred<FlowRunDetail>();
    api.getRunDetail.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);

    const a = store.loadRunDetail(1);
    const b = store.loadRunDetail(2);
    second.resolve(detail(2));
    await b;
    first.resolve(detail(1));
    await a;

    expect(store.selectedRunId).toBe(2);
    expect(store.selectedRunDetail?.id).toBe(2);
  });

  it("never shows the previous run's detail under the next run's id while it loads", async () => {
    const store = useCatalogStore();
    api.getRunDetail.mockResolvedValueOnce(detail(1));
    await store.loadRunDetail(1);
    api.getRunDetail.mockReturnValueOnce(never());

    void store.loadRunDetail(2);

    expect(store.selectedRunDetail).toBeNull();
    expect(store.selectedRun).toBeNull();
  });

  it("drops a detail that lands after the run was closed", async () => {
    const store = useCatalogStore();
    const pending = deferred<FlowRunDetail>();
    api.getRunDetail.mockReturnValue(pending.promise);

    const loading = store.loadRunDetail(3);
    store.clearRunSelection();
    pending.resolve(detail(3));
    await loading;

    expect(store.selectedRunId).toBeNull();
    expect(store.selectedRunDetail).toBeNull();
  });

  it("keeps a failed detail's error for the selected run only", async () => {
    const store = useCatalogStore();
    api.getRunDetail.mockRejectedValue(new Error("gone"));

    await store.loadRunDetail(4);

    expect(store.selectedRunId).toBe(4);
    expect(store.runDetailError).toBe("gone");
  });
});

describe("catalog-store schedule selection", () => {
  it("shows the schedule's row at once and empties the previous schedule's runs", async () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2)];
    api.getSchedule.mockResolvedValueOnce(schedule(1));
    api.getRuns.mockResolvedValueOnce(page(run(10)));
    await store.selectSchedule(1);
    expect(store.scheduleRunsScheduleId).toBe(1);
    api.getSchedule.mockReturnValue(never());
    api.getRuns.mockReturnValue(never());

    void store.selectSchedule(2);

    expect(store.selectedScheduleId).toBe(2);
    expect(store.selectedSchedule?.id).toBe(2);
    expect(store.scheduleRuns).toEqual([]);
    expect(store.scheduleRunsScheduleId).toBeNull();
  });

  it("drops a slower earlier schedule's detail and runs that land after a later click", async () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2)];
    const detailA = deferred<FlowSchedule>();
    const runsA = deferred<PaginatedFlowRuns>();
    api.getSchedule
      .mockReturnValueOnce(detailA.promise)
      .mockResolvedValueOnce(schedule(2, "B fetched"));
    api.getRuns.mockReturnValueOnce(runsA.promise).mockResolvedValueOnce(page(run(20)));

    const a = store.selectSchedule(1);
    await store.selectSchedule(2);
    detailA.resolve(schedule(1, "A fetched"));
    runsA.resolve(page(run(10)));
    await a;

    expect(store.selectedSchedule?.name).toBe("B fetched");
    expect(store.scheduleRuns.map((r) => r.id)).toEqual([20]);
    expect(store.scheduleRunsScheduleId).toBe(2);
  });

  it("has no schedule object for a schedule missing from the list until its detail lands", async () => {
    const store = useCatalogStore();
    const pending = deferred<FlowSchedule>();
    api.getSchedule.mockReturnValue(pending.promise);
    api.getRuns.mockResolvedValue(page());

    const selecting = store.selectSchedule(9);
    expect(store.selectedScheduleId).toBe(9);
    expect(store.selectedSchedule).toBeNull();

    pending.resolve(schedule(9));
    await selecting;
    expect(store.selectedSchedule?.id).toBe(9);
  });
});

describe("catalog-store flow page slices", () => {
  it("never writes a flow's runs into the global runs page", async () => {
    const store = useCatalogStore();
    store.applyRunsPage(page(run(1, 1), run(2, 2)));
    api.getRuns.mockResolvedValue(page(run(2, 2)));
    api.getSchedules.mockResolvedValue([]);
    api.getFlowArtifacts.mockResolvedValue([]);

    store.selectFlow(2);
    await flush();

    expect(store.runs.map((r) => r.id)).toEqual([1, 2]);
    expect(store.flowRunsFlowId).toBe(2);
    expect(store.flowRunsPageFor(2)?.items.map((r) => r.id)).toEqual([2]);
    expect(store.flowRunsPageFor(1)).toBeNull();
    expect(api.getRuns).toHaveBeenCalledWith(2, 25, 0, undefined, undefined);
  });

  it("empties another flow's runs, schedules and artifacts the moment a flow is selected", async () => {
    const store = useCatalogStore();
    api.getRuns.mockResolvedValueOnce(page(run(1, 1)));
    api.getSchedules.mockResolvedValueOnce([schedule(1)]);
    api.getFlowArtifacts.mockResolvedValueOnce([{ id: 1 } as GlobalArtifact]);
    store.selectFlow(1);
    await flush();
    api.getRuns.mockReturnValue(never());
    api.getSchedules.mockReturnValue(never());
    api.getFlowArtifacts.mockReturnValue(never());

    store.selectFlow(2);

    expect(store.flowRunsFlowId).toBeNull();
    expect(store.flowRunsPageFor(2)).toBeNull();
    expect(store.flowSchedules).toEqual([]);
    expect(store.flowSchedulesFlowId).toBeNull();
    expect(store.flowArtifacts).toEqual([]);
    expect(store.flowArtifactsFlowId).toBeNull();
  });

  it("drops a slower earlier flow's runs, schedules and artifacts that land after a switch", async () => {
    const store = useCatalogStore();
    const runsA = deferred<PaginatedFlowRuns>();
    const schedulesA = deferred<FlowSchedule[]>();
    const artifactsA = deferred<GlobalArtifact[]>();
    api.getRuns.mockReturnValueOnce(runsA.promise).mockResolvedValueOnce(page(run(20, 2)));
    api.getSchedules.mockReturnValueOnce(schedulesA.promise).mockResolvedValueOnce([]);
    api.getFlowArtifacts.mockReturnValueOnce(artifactsA.promise).mockResolvedValueOnce([]);

    store.selectFlow(1);
    store.selectFlow(2);
    await flush();
    runsA.resolve(page(run(10, 1)));
    schedulesA.resolve([schedule(1)]);
    artifactsA.resolve([{ id: 1 } as GlobalArtifact]);
    await flush();

    expect(store.flowRunsFlowId).toBe(2);
    expect(store.flowRunsPageFor(2)?.items.map((r) => r.id)).toEqual([20]);
    expect(store.flowSchedulesFlowId).toBe(2);
    expect(store.flowSchedules).toEqual([]);
    expect(store.flowArtifactsFlowId).toBe(2);
    expect(store.flowArtifacts).toEqual([]);
  });

  it("drops a flow's runs that land after the flow was left for another page", async () => {
    const store = useCatalogStore();
    const runsA = deferred<PaginatedFlowRuns>();
    api.getRuns.mockReturnValueOnce(runsA.promise);
    api.getSchedules.mockResolvedValue([]);
    api.getFlowArtifacts.mockResolvedValue([]);

    store.selectFlow(1);
    store.selectedFlowId = null;
    runsA.resolve(page(run(10, 1)));
    await flush();

    expect(store.flowRunsFlowId).toBeNull();
    expect(store.flowRunsData).toBeNull();
  });

  it("starts a flow page on page 1 with no filter, leaving the global page and filter alone", async () => {
    const store = useCatalogStore();
    store.runsPage = 3;
    store.runsTriggerFilter = "manual";
    store.runsSearch = "orders";
    store.flowRunsPage = 2;
    store.flowRunsTriggerFilter = "scheduled";
    api.getRuns.mockResolvedValue(page());
    api.getSchedules.mockResolvedValue([]);
    api.getFlowArtifacts.mockResolvedValue([]);

    store.selectFlow(4);
    await flush();

    expect(store.flowRunsPage).toBe(1);
    expect(store.flowRunsTriggerFilter).toBeNull();
    expect(store.runsPage).toBe(3);
    expect(store.runsTriggerFilter).toBe("manual");
    expect(api.getRuns).toHaveBeenCalledWith(4, 25, 0, undefined, undefined);
  });

  it("refreshes the global page and the selected flow's page apart", async () => {
    const store = useCatalogStore();
    store.selectedFlowId = 3;
    api.getRuns.mockImplementation(async (registrationId: number | null) =>
      registrationId === 3 ? page(run(30, 3)) : page(run(1, 1), run(30, 3)),
    );

    await store.refreshRuns();

    expect(store.runs.map((r) => r.id)).toEqual([1, 30]);
    expect(store.flowRunsPageFor(3)?.items.map((r) => r.id)).toEqual([30]);
  });

  it("keeps the newest global runs page when an older request lands last", async () => {
    const store = useCatalogStore();
    const older = deferred<PaginatedFlowRuns>();
    api.getRuns.mockReturnValueOnce(older.promise).mockResolvedValueOnce(page(run(2)));

    const first = store.loadRuns();
    await store.loadRuns();
    older.resolve(page(run(1)));
    await first;

    expect(store.runs.map((r) => r.id)).toEqual([2]);
  });
});

describe("catalog-store review follow-ups", () => {
  it("drops an unconfirmed schedule row when its fetch fails, and keeps a fetched one on a failed refresh", async () => {
    const store = useCatalogStore();
    store.schedules = [schedule(7)];
    api.getRuns.mockResolvedValue(page());
    api.getSchedule.mockRejectedValueOnce(new Error("gone"));

    await store.selectSchedule(7);
    expect(store.selectedScheduleId).toBe(7);
    expect(store.selectedSchedule).toBeNull();
    expect(store.scheduleDetailError).toBe("gone");

    api.getSchedule.mockResolvedValueOnce(schedule(7, "Nightly"));
    await store.loadScheduleDetail(7);
    expect(store.selectedSchedule?.name).toBe("Nightly");
    expect(store.scheduleDetailError).toBeNull();

    api.getSchedule.mockRejectedValueOnce(new Error("offline"));
    await store.loadScheduleDetail(7);
    expect(store.selectedSchedule?.name).toBe("Nightly");
    expect(store.scheduleDetailError).toBe("offline");
  });

  it("keeps the latest snapshot from the flow's newest runs while another page or filter is shown", async () => {
    const store = useCatalogStore();
    const withSnapshot = (id: number) => ({ ...run(id, 4), has_snapshot: true }) as FlowRun;
    api.getSchedules.mockResolvedValue([]);
    api.getFlowArtifacts.mockResolvedValue([]);
    api.getRuns.mockResolvedValueOnce(page(run(90, 4), withSnapshot(80)));
    store.selectFlow(4);
    await flush();
    expect(store.flowLatestSnapshotRunId).toBe(80);

    api.getRuns.mockResolvedValueOnce(page(withSnapshot(10)));
    store.setFlowRunsPage(2, 4);
    await flush();
    expect(store.flowRunsPageFor(4)?.items.map((r) => r.id)).toEqual([10]);
    expect(store.flowLatestSnapshotRunId).toBe(80);

    api.getRuns.mockResolvedValueOnce(page(withSnapshot(5)));
    store.setFlowRunsTriggerFilter("manual", 4);
    await flush();
    expect(store.flowLatestSnapshotRunId).toBe(80);

    api.getRuns.mockReturnValue(never());
    store.selectFlow(5);
    expect(store.flowLatestSnapshotRunId).toBeNull();
  });

  it("records a failed global runs load and clears it when a page lands", async () => {
    const store = useCatalogStore();
    api.getRuns.mockRejectedValueOnce(new Error("boom"));
    await store.loadRuns();
    expect(store.runsError).toBe("boom");

    api.getRuns.mockResolvedValueOnce(page(run(1)));
    await store.loadRuns();
    expect(store.runsError).toBeNull();
    expect(store.runs.map((r) => r.id)).toEqual([1]);
  });
});
