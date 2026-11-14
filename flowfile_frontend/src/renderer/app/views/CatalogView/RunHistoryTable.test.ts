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
});
