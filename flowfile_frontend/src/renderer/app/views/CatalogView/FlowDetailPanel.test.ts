// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
// The panel's children that reach the file browser, sharing or the API layer are not what is shown here.
vi.mock("./ApiEndpointPanel.vue", () => ({ default: { render: () => null } }));
vi.mock("./components/ScheduleTable.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/common", () => ({
  CollapsibleSection: { render: () => null },
  EmptyState: { render: () => null },
}));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canShare: () => false,
    canManage: () => true,
    canManageGrants: () => false,
    isMultiUser: { value: false },
    isOwned: () => true,
  }),
}));

import { mount, type Mounted } from "../../__fixtures__/mount";
import type { FlowRegistration } from "../../types";
import FlowDetailPanel from "./FlowDetailPanel.vue";

const flow = (fields: Partial<FlowRegistration>): FlowRegistration =>
  ({
    id: 1,
    name: "orders",
    description: null,
    flow_path: "/flows/orders.yaml",
    namespace_id: 1,
    owner_id: 1,
    created_at: "2026-11-01T10:00:00",
    updated_at: "2026-11-01T10:00:00",
    is_favorite: false,
    is_following: false,
    run_count: 1,
    last_run_at: "2026-11-10T10:00:00",
    last_run_success: false,
    file_exists: true,
    is_api_compatible: false,
    artifact_count: 0,
    tables_produced: [],
    tables_read: [],
    ...fields,
  }) as FlowRegistration;

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const lastRunStatus = () => {
  const card = Array.from(mounted!.el.querySelectorAll(".meta-card")).find(
    (candidate) => candidate.querySelector(".meta-label")?.textContent === "Status",
  )!;
  const value = card.querySelector(".meta-value")!;
  return {
    label: value.textContent,
    classes: Array.from(value.classList).filter((c) => c !== "meta-value"),
  };
};

describe("FlowDetailPanel", () => {
  it("shows a flow whose last run was cancelled as Cancelled, not in the failure colour", () => {
    mounted = mount(FlowDetailPanel, { flow: flow({ last_run_cancelled: true }), artifacts: [] });

    expect(lastRunStatus()).toEqual({ label: "Cancelled", classes: ["text-cancelled"] });
  });

  it("shows a flow whose last run failed as Failed, also against an older core", () => {
    for (const fields of [{ last_run_cancelled: false }, {}]) {
      mounted?.unmount();
      mounted = mount(FlowDetailPanel, { flow: flow(fields), artifacts: [] });

      expect(lastRunStatus()).toEqual({ label: "Failed", classes: ["text-danger"] });
    }
  });

  it("keeps a successful last run and a flow that never ran as they were", () => {
    mounted = mount(FlowDetailPanel, { flow: flow({ last_run_success: true }), artifacts: [] });
    expect(lastRunStatus()).toEqual({ label: "Success", classes: ["text-success"] });

    mounted.unmount();
    mounted = mount(FlowDetailPanel, {
      flow: flow({ last_run_success: null, last_run_at: null, run_count: 0 }),
      artifacts: [],
    });
    expect(lastRunStatus().label).toBe("No runs");
  });
});
