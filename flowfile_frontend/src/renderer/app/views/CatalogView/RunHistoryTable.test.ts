// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));

import { mount, type Mounted } from "../../__fixtures__/mount";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowRun, PaginatedFlowRuns } from "../../types";
import RunHistoryTable from "./RunHistoryTable.vue";

const run = (id: number, fields: Partial<FlowRun>): FlowRun => ({
  id,
  registration_id: 1,
  flow_name: `flow ${id}`,
  flow_path: null,
  user_id: 1,
  started_at: "2026-11-10T10:00:00",
  ended_at: "2026-11-10T10:00:05",
  success: false,
  nodes_completed: 0,
  number_of_nodes: 3,
  duration_seconds: 5,
  run_type: "manual",
  schedule_id: null,
  has_snapshot: false,
  has_log: false,
  ...fields,
});

const page: PaginatedFlowRuns = {
  items: [
    run(1, { cancelled: true }),
    run(2, { cancelled: false }),
    run(3, {}),
    run(4, { success: true }),
    run(5, { success: null, ended_at: null }),
  ],
  total: 5,
  total_success: 1,
  total_failed: 2,
  total_running: 1,
  total_cancelled: 1,
};

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const badges = () =>
  Array.from(mounted!.el.querySelectorAll(".table-row .status-badge")).map((badge) => ({
    label: badge.textContent!.trim(),
    classes: Array.from(badge.classList).filter((name) => name !== "status-badge"),
    icon: badge.querySelector("i")!.className,
  }));

const summary = () =>
  Object.fromEntries(
    Array.from(mounted!.el.querySelectorAll(".summary-card")).map((card) => [
      card.querySelector(".summary-label")!.textContent,
      card.querySelector(".summary-value")!.textContent,
    ]),
  );

describe("RunHistoryTable", () => {
  it("holds the list's place instead of the empty state until the overview lands", () => {
    mounted = mount(RunHistoryTable);

    expect(mounted.el.querySelector('[data-testid="runs-pending"]')).not.toBeNull();
    expect(mounted.text()).not.toContain("No runs recorded yet");
    expect(Object.values(summary())).toEqual(["", "", "", ""]);
  });

  it("shows the empty state once the overview loaded no runs", () => {
    mounted = mount(RunHistoryTable, {}, () => {
      useCatalogStore().overviewLoadedAt = Date.now();
    });

    expect(mounted.el.querySelector('[data-testid="runs-pending"]')).toBeNull();
    expect(mounted.text()).toContain("No runs recorded yet");
    expect(summary()).toMatchObject({ Total: "0" });
  });

  it("offers a retry instead of a placeholder when the first overview failed", () => {
    mounted = mount(RunHistoryTable, {}, () => {
      useCatalogStore().overviewError = "boom";
    });

    expect(mounted.el.querySelector('[data-testid="runs-pending"]')).toBeNull();
    expect(mounted.text()).toContain("Couldn't load runs: boom");
    expect(mounted.text()).not.toContain("No runs recorded yet");
  });

  it("does not wait on the overview for a schedule's runs", () => {
    mounted = mount(RunHistoryTable, { scheduleId: 9 }, () => {
      useCatalogStore().scheduleRunsScheduleId = 9;
    });

    expect(mounted.el.querySelector('[data-testid="runs-pending"]')).toBeNull();
    expect(mounted.text()).toContain("No runs recorded yet");
  });

  it("holds a schedule's list until that schedule's runs arrived, not another's", () => {
    mounted = mount(RunHistoryTable, { scheduleId: 9 }, () => {
      const store = useCatalogStore();
      store.overviewLoadedAt = Date.now();
      store.scheduleRunsScheduleId = 8;
      store.scheduleRuns = [run(1, { schedule_id: 8 })];
    });

    expect(mounted.el.querySelector('[data-testid="runs-pending"]')).not.toBeNull();
    expect(badges()).toEqual([]);
  });

  it("shows a cancelled run as Cancelled, neutral, and counts it apart from the failures", () => {
    mounted = mount(RunHistoryTable, {}, () => useCatalogStore().applyRunsPage(page));

    expect(badges()).toEqual([
      { label: "Cancelled", classes: ["cancelled"], icon: "fa-solid fa-ban" },
      { label: "Failed", classes: ["failure"], icon: "fa-solid fa-circle-xmark" },
      { label: "Failed", classes: ["failure"], icon: "fa-solid fa-circle-xmark" },
      { label: "Success", classes: ["success"], icon: "fa-solid fa-circle-check" },
      { label: "Running", classes: ["running"], icon: "fa-solid fa-spinner fa-spin" },
    ]);
    expect(summary()).toEqual({
      Total: "5",
      Successful: "1",
      Failed: "2",
      Running: "1",
      Cancelled: "1",
    });
  });

  it("shows no Cancelled card while no run was cancelled, also against an older core", () => {
    const older: PaginatedFlowRuns = { ...page, items: page.items.slice(1) };
    delete older.total_cancelled;
    mounted = mount(RunHistoryTable, {}, () => useCatalogStore().applyRunsPage(older));

    expect(Object.keys(summary())).toEqual(["Total", "Successful", "Failed", "Running"]);
    expect(mounted.text()).not.toContain("Cancelled");
  });

  it("shows a schedule's cancelled run the same way", () => {
    mounted = mount(RunHistoryTable, { scheduleId: 9 }, () => {
      const store = useCatalogStore();
      store.scheduleRunsScheduleId = 9;
      store.scheduleRuns = [run(1, { cancelled: true, run_type: "scheduled", schedule_id: 9 })];
      store.scheduleRunsTotal = 1;
      store.scheduleRunsTotalCancelled = 1;
    });

    expect(badges().map((badge) => badge.label)).toEqual(["Cancelled"]);
    expect(summary()).toMatchObject({ Failed: "0", Cancelled: "1" });
  });

  it("offers the Dispatched runs filter only where such a run is listed", () => {
    const triggers = () => mounted!.el.querySelectorAll('[data-stub="el-option"]').length;
    mounted = mount(RunHistoryTable, {}, () => useCatalogStore().applyRunsPage(page));
    expect(triggers()).toBe(4);
    mounted.unmount();

    const sent = { ...page, items: [run(9, { run_type: "dispatched" })] };
    mounted = mount(RunHistoryTable, {}, () => useCatalogStore().applyRunsPage(sent));
    expect(triggers()).toBe(5);
  });

  describe("on a flow page", () => {
    const flowPage = (registrationId: number): PaginatedFlowRuns => ({
      items: [run(70, { registration_id: registrationId, success: true })],
      total: 1,
      total_success: 1,
      total_failed: 0,
      total_running: 0,
    });

    it("holds its place instead of showing the global runs while the flow's are on their way", () => {
      mounted = mount(RunHistoryTable, { registrationId: 7, collapsible: true }, () => {
        const store = useCatalogStore();
        store.overviewLoadedAt = Date.now();
        store.applyRunsPage(page);
      });

      expect(mounted.el.querySelector('[data-testid="runs-pending"]')).not.toBeNull();
      expect(badges()).toEqual([]);
      expect(mounted.text()).not.toContain("Show all");
      mounted.unmount();

      mounted = mount(RunHistoryTable, { registrationId: 7 }, () => {
        useCatalogStore().applyRunsPage(page);
      });
      expect(Object.values(summary())).toEqual(["", "", "", ""]);
    });

    it("never shows another flow's runs", () => {
      mounted = mount(RunHistoryTable, { registrationId: 7 }, () => {
        const store = useCatalogStore();
        store.overviewLoadedAt = Date.now();
        store.flowRunsFlowId = 6;
        store.flowRunsData = flowPage(6);
      });

      expect(mounted.el.querySelector('[data-testid="runs-pending"]')).not.toBeNull();
      expect(badges()).toEqual([]);
    });

    it("shows the flow's own runs and counts once they arrived", () => {
      mounted = mount(RunHistoryTable, { registrationId: 7 }, () => {
        const store = useCatalogStore();
        store.applyRunsPage(page);
        store.flowRunsFlowId = 7;
        store.flowRunsData = flowPage(7);
      });

      expect(badges().map((badge) => badge.label)).toEqual(["Success"]);
      expect(summary()).toMatchObject({ Total: "1", Successful: "1", Failed: "0" });
    });

    it("offers a retry for the flow's runs when they failed to load", () => {
      mounted = mount(RunHistoryTable, { registrationId: 7 }, () => {
        useCatalogStore().flowRunsError = "boom";
      });

      expect(mounted.text()).toContain("Couldn't load runs: boom");
    });

    it("leaves the global table on the global runs while a flow's runs are loaded", () => {
      mounted = mount(RunHistoryTable, {}, () => {
        const store = useCatalogStore();
        store.applyRunsPage(page);
        store.flowRunsFlowId = 7;
        store.flowRunsData = flowPage(7);
      });

      expect(badges()).toHaveLength(5);
      expect(summary()).toMatchObject({ Total: "5" });
    });
  });
});
