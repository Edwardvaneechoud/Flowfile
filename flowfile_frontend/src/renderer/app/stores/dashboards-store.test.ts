import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { Dashboard, DashboardLayout } from "../types";

const { updateDashboardMock } = vi.hoisted(() => ({
  updateDashboardMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    updateDashboard: updateDashboardMock,
  },
}));

import { AUTOSAVE_REQUEST_TIMEOUT_MS } from "../composables/autosaveEngine";
import { useDashboardsStore } from "./dashboards-store";

const layout = (rows: number): DashboardLayout => ({
  tiles: [{ id: "t1", type: "viz", viz_id: 1, chart_index: 0, x: 0, y: 0, w: 6, h: rows }],
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
});

const dashboard = (overrides: Partial<Dashboard> = {}): Dashboard => ({
  id: 5,
  name: "Sales",
  description: null,
  layout: layout(4),
  layout_version: 1,
  namespace_id: null,
  namespace_name: null,
  created_by: 1,
  created_at: "2026-01-01T00:00:00",
  updated_at: "2026-01-01T00:00:00",
  access: { is_owner: true, access_level: "owner", shared_by: null } as Dashboard["access"],
  ...overrides,
});

describe("dashboards-store autosaveDashboard", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    updateDashboardMock.mockReset();
  });

  it("sends the CAS token and a request timeout", async () => {
    const store = useDashboardsStore();
    store.current = dashboard();
    updateDashboardMock.mockResolvedValue(dashboard({ layout_version: 2 }));

    await store.autosaveDashboard(5, {
      name: "Sales",
      layout: layout(4),
      expected_layout_version: 1,
    });

    expect(updateDashboardMock).toHaveBeenCalledWith(
      5,
      expect.objectContaining({ expected_layout_version: 1 }),
      { timeout: AUTOSAVE_REQUEST_TIMEOUT_MS },
    );
  });

  it("merges metadata only — never replaces the local layout", async () => {
    const store = useDashboardsStore();
    const localLayout = layout(9);
    store.current = dashboard({ layout: localLayout });
    // Response carries an older layout (the state as-of the request).
    updateDashboardMock.mockResolvedValue(
      dashboard({ layout: layout(4), layout_version: 3, updated_at: "2026-01-02T00:00:00" }),
    );

    await store.autosaveDashboard(5, { name: "Sales", layout: layout(4) });

    // The local (newer) layout survives; only metadata is adopted.
    expect(store.current?.layout.tiles[0]?.h).toBe(9);
    expect(store.current?.layout_version).toBe(3);
    expect(store.current?.updated_at).toBe("2026-01-02T00:00:00");
  });

  it("preserves the access stamp when the response lacks one", async () => {
    const store = useDashboardsStore();
    store.current = dashboard();
    store.library = [dashboard()];
    updateDashboardMock.mockResolvedValue(dashboard({ access: null, layout_version: 2 }));

    await store.autosaveDashboard(5, { name: "Sales" });

    expect(store.current?.access?.access_level).toBe("owner");
    expect(store.library[0]?.access?.access_level).toBe("owner");
  });

  it("does not toggle the saving flag", async () => {
    const store = useDashboardsStore();
    store.current = dashboard();
    let savingDuringCall: boolean | null = null;
    updateDashboardMock.mockImplementation(async () => {
      savingDuringCall = store.saving;
      return dashboard({ layout_version: 2 });
    });

    await store.autosaveDashboard(5, { name: "Sales" });

    expect(savingDuringCall).toBe(false);
    expect(store.saving).toBe(false);
  });

  it("propagates rejections for the engine to classify", async () => {
    const store = useDashboardsStore();
    store.current = dashboard();
    const conflict = Object.assign(new Error("409"), {
      response: { status: 409, data: { detail: { error: "stale_write" } } },
    });
    updateDashboardMock.mockRejectedValue(conflict);

    await expect(store.autosaveDashboard(5, { name: "Sales" })).rejects.toBe(conflict);
  });
});
