import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { ActiveFlowRun, FlowSchedule } from "../types";

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {},
}));

import { useCatalogStore } from "./catalog-store";

const REGISTRATION_ID = 7;

const schedule = (id: number): FlowSchedule =>
  ({
    id,
    registration_id: REGISTRATION_ID,
    enabled: true,
    name: `Schedule ${id}`,
    schedule_type: "cron",
  }) as unknown as FlowSchedule;

const activeRun = (id: number, scheduleId: number | null): ActiveFlowRun =>
  ({
    id,
    registration_id: REGISTRATION_ID,
    schedule_id: scheduleId,
    flow_name: "Nightly",
    run_type: scheduleId === null ? "manual" : "scheduled",
  }) as unknown as ActiveFlowRun;

describe("catalog-store enrichedSchedules isRunning", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("marks only the schedules that own an active run", () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2), schedule(3)];
    store.activeRuns = [activeRun(100, 1), activeRun(101, 2), activeRun(102, null)];

    const running = Object.fromEntries(store.enrichedSchedules.map((s) => [s.id, s.isRunning]));

    expect(running).toEqual({ 1: true, 2: true, 3: false });
  });

  it("does not mark any schedule for a run without a schedule_id", () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2)];
    store.activeRuns = [activeRun(100, null)];

    expect(store.enrichedSchedules.every((s) => !s.isRunning)).toBe(true);
  });
});

describe("catalog-store enrichedSchedules isFlowRunning", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("marks a sibling schedule flow-running while another schedule of the flow runs", () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2)];
    store.activeRuns = [activeRun(100, 1)];

    const b = store.enrichedSchedules.find((s) => s.id === 2)!;

    expect(b.isRunning).toBe(false);
    expect(b.isFlowRunning).toBe(true);
  });

  it("counts a manual run with no schedule_id as flow-running", () => {
    const store = useCatalogStore();
    store.schedules = [schedule(1), schedule(2)];
    store.activeRuns = [activeRun(100, null)];

    expect(store.enrichedSchedules.map((s) => s.isRunning)).toEqual([false, false]);
    expect(store.enrichedSchedules.map((s) => s.isFlowRunning)).toEqual([true, true]);
  });

  it("leaves schedules of other flows alone", () => {
    const store = useCatalogStore();
    const otherFlowSchedule = { ...schedule(3), registration_id: 99 };
    store.schedules = [schedule(1), otherFlowSchedule];
    store.activeRuns = [activeRun(100, 1)];

    const other = store.enrichedSchedules.find((s) => s.id === 3)!;

    expect(other.isRunning).toBe(false);
    expect(other.isFlowRunning).toBe(false);
  });
});

describe("catalog-store enrichedSchedules of a hub", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("names a hub schedule's flow from the tree and marks it running by the hub's run", () => {
    const store = useCatalogStore();
    const hubFlow = { id: -201, name: "orders-clean", source: "hub" };
    store.tree = [{ id: -1, flows: [], children: [{ id: -2, flows: [hubFlow], children: [] }] }] as any;
    store.schedules = [
      schedule(7),
      { ...schedule(-7), registration_id: -201, source: "hub", backend: "worker:bmax" },
    ];
    store.activeRuns = [{ ...activeRun(-6, -7), registration_id: -201, source: "hub" }];
    const [local, hub] = store.enrichedSchedules;
    expect([local.flowName, local.isRunning]).toEqual([`Flow #${REGISTRATION_ID}`, false]);
    expect([hub.flowName, hub.isRunning, hub.isFlowRunning]).toEqual(["orders-clean", true, true]);
    expect([hub.source, hub.backend]).toEqual(["hub", "worker:bmax"]);
  });
});
