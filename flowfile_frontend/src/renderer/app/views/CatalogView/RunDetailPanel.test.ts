// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

const { openExternal, getRunLog } = vi.hoisted(() => ({
  openExternal: vi.fn(),
  getRunLog: vi.fn(),
}));

vi.mock("../../api/catalog.api", () => ({ CatalogApi: { getRunLog } }));
vi.mock("../../../lib/desktop", () => ({ desktop: { openExternal } }));
vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: vi.fn().mockResolvedValue(null),
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
}));

import { mount, type Mounted } from "../../__fixtures__/mount";
import type { HubConnection } from "../../api/hubConnection.api";
import { useHubConnectionStore } from "../../stores/hub-connection-store";
import type { FlowRunDetail } from "../../types";
import RunDetailPanel from "./RunDetailPanel.vue";

const node = (node_id: number, success: boolean | null, error = "") => ({
  node_id,
  node_name: `node_${node_id}`,
  success,
  error,
  run_time_ms: 5,
  is_running: false,
});

const run = (fields: Partial<FlowRunDetail>): FlowRunDetail => ({
  id: 7,
  registration_id: 1,
  flow_name: "orders",
  flow_path: "/flows/orders.yaml",
  user_id: 1,
  started_at: "2026-11-10T10:00:00",
  ended_at: "2026-11-10T10:00:05",
  success: false,
  nodes_completed: 1,
  number_of_nodes: 3,
  duration_seconds: 5,
  run_type: "manual",
  schedule_id: null,
  has_snapshot: false,
  has_log: false,
  flow_snapshot: null,
  node_results_json: null,
  ...fields,
});

let mounted: Mounted | undefined;
afterEach(() => {
  mounted?.unmount();
  openExternal.mockReset();
  getRunLog.mockReset();
});

const status = () => mounted!.el.querySelector(".meta-card:nth-child(2) .meta-value")!;
const nodeBadges = () =>
  Array.from(mounted!.el.querySelectorAll(".results-table .status-badge")).map((badge) => ({
    label: badge.textContent!.trim(),
    classes: Array.from(badge.classList).filter((name) => name !== "status-badge"),
  }));

describe("RunDetailPanel", () => {
  it("shows a cancelled run as Cancelled in a neutral colour, with its stopped node", () => {
    mounted = mount(RunDetailPanel, {
      run: run({
        cancelled: true,
        node_results_json: JSON.stringify([node(1, true), { ...node(2, null), cancelled: true }]),
      }),
    });

    expect(status().textContent).toBe("Cancelled");
    expect(Array.from(status().classList)).toContain("text-cancelled");
    expect(Array.from(status().classList)).not.toContain("text-danger");
    expect(nodeBadges()).toEqual([
      { label: "OK", classes: ["success"] },
      { label: "Cancelled", classes: ["cancelled"] },
    ]);
    expect(mounted.text()).not.toContain("Failed");
  });

  it("shows a cancelled run that reported no node without a node table", () => {
    mounted = mount(RunDetailPanel, { run: run({ cancelled: true, nodes_completed: 0 }) });

    expect(status().textContent).toBe("Cancelled");
    expect(mounted.el.querySelector(".results-table")).toBeNull();
  });

  it("shows a failed run as Failed, also one recorded before the mark existed", () => {
    const nodes = JSON.stringify([node(1, true), node(2, false, "boom")]);
    for (const fields of [{ cancelled: false }, {}]) {
      mounted?.unmount();
      mounted = mount(RunDetailPanel, { run: run({ ...fields, node_results_json: nodes }) });

      expect(status().textContent).toBe("Failed");
      expect(Array.from(status().classList)).toContain("text-danger");
      expect(nodeBadges()[1]).toEqual({ label: "Failed", classes: ["failure"] });
    }
  });

  it("shows a run from its list row at once, holding the detail's sections until it lands", () => {
    const detail: Partial<FlowRunDetail> = run({ has_snapshot: true, has_log: true });
    delete detail.flow_snapshot;
    delete detail.node_results_json;
    mounted = mount(RunDetailPanel, { run: detail, detailPending: true });

    expect(mounted.text()).toContain("Run #7");
    expect(status().textContent).toBe("Failed");
    expect(mounted.el.querySelector('[data-testid="run-detail-pending"]')).not.toBeNull();
    expect(mounted.el.querySelector(".snapshot-pending")).not.toBeNull();
    expect(mounted.text()).not.toContain("View log");
  });

  it("says the detail could not be loaded and offers a retry, in place of the placeholders", () => {
    const detail: Partial<FlowRunDetail> = run({ has_snapshot: true, has_log: true });
    delete detail.flow_snapshot;
    delete detail.node_results_json;
    const retried = vi.fn();
    mounted = mount(RunDetailPanel, {
      run: detail,
      detailPending: true,
      detailError: "Request failed with status code 500",
      onRetryDetail: retried,
    });

    const error = mounted.el.querySelector('[data-testid="run-detail-error"]')!;
    expect(error.textContent).toContain("Request failed with status code 500");
    expect(mounted.el.querySelector('[data-testid="run-detail-pending"]')).toBeNull();
    expect(mounted.el.querySelector(".snapshot-pending")).toBeNull();
    expect(mounted.text()).toContain("View log");
    error.querySelector("button")!.click();
    expect(retried).toHaveBeenCalledTimes(1);
  });
});

describe("RunDetailPanel: the log of a run kept elsewhere", () => {
  const PAGE = "http://bmax.example:63578/ui/#/main/catalog?tab=runs&runId=17";
  const ranOnBmax = (fields: Partial<FlowRunDetail>) =>
    run({
      has_log: true,
      executed_on: "bmax",
      log_ref: "backend:bmax:17",
      log_url: "http://bmax.example:63578/catalog/runs/17/log",
      log_page_url: PAGE,
      ...fields,
    });
  /** Mounted on a server named `here`, or on one that joined no hub. */
  const show = (detail: FlowRunDetail, here?: string) => {
    mounted = mount(RunDetailPanel, { run: detail }, () => {
      if (here) useHubConnectionStore().connection = { server_name: here } as HubConnection;
    });
  };
  const openButton = () =>
    mounted!.el.querySelector<HTMLButtonElement>('[data-testid="run-log-open-elsewhere"]');
  const viewLog = () =>
    Array.from(mounted!.el.querySelectorAll("button")).find((button) =>
      button.textContent!.includes("View log"),
    );
  const note = () => mounted!.el.querySelector(".log-viewer")?.textContent?.trim();

  it("sends a person to the run's page on the server that ran it", () => {
    show(ranOnBmax({}));

    expect(openButton()?.textContent?.trim()).toBe("Open the run on bmax");
    expect(viewLog()).toBeUndefined();
    expect(note()).toContain("Open the run there to read it");

    openButton()!.click();
    expect(openExternal).toHaveBeenCalledWith(PAGE);
  });

  it("sends a person to the run's page also when core names no log route", () => {
    show(ranOnBmax({ log_url: null }));

    expect(openButton()).not.toBeNull();
    expect(viewLog()).toBeUndefined();
  });

  it("says the server that ran it has no editor when core names no page", () => {
    show(ranOnBmax({ log_page_url: null }));

    expect(openButton()).toBeNull();
    expect(viewLog()).toBeUndefined();
    expect(note()).toBe("The log is kept on bmax, which has no editor to read it in.");
  });

  it("claims no missing editor for an older core, which names no page at all", () => {
    const older = ranOnBmax({});
    delete older.log_page_url;
    show(older);

    expect(openButton()).toBeNull();
    expect(viewLog()).toBeUndefined();
    expect(note()).toBe("The log is kept on bmax. Open it in that server's editor to read it.");
  });

  it("reads the log here, under this server's own id, for a run this server ran", async () => {
    getRunLog.mockResolvedValue("run 17 finished");
    show(ranOnBmax({}), "bmax");
    await new Promise((resolve) => setTimeout(resolve));

    expect(openButton()).toBeNull();
    expect(getRunLog).toHaveBeenCalledWith(17);
    expect(note()).toBe("run 17 finished");
  });
});
