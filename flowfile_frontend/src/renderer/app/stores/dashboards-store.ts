import { defineStore } from "pinia";
import { CatalogApi } from "../api/catalog.api";
import { AUTOSAVE_REQUEST_TIMEOUT_MS } from "../composables/autosaveEngine";
import type {
  Dashboard,
  DashboardCreatePayload,
  DashboardLayout,
  DashboardUpdatePayload,
} from "../types";
import { EMPTY_DASHBOARD_LAYOUT } from "../types";
import { upgradeLayoutGrid } from "../views/DashboardsView/gridVersion";

interface DashboardsState {
  library: Dashboard[];
  loadingLibrary: boolean;
  current: Dashboard | null;
  loadingCurrent: boolean;
  saving: boolean;
  error: string | null;
}

export const useDashboardsStore = defineStore("dashboards", {
  state: (): DashboardsState => ({
    library: [],
    loadingLibrary: false,
    current: null,
    loadingCurrent: false,
    saving: false,
    error: null,
  }),

  actions: {
    async loadLibrary() {
      this.loadingLibrary = true;
      this.error = null;
      try {
        this.library = await CatalogApi.listDashboards();
      } catch (err) {
        this.error = err instanceof Error ? err.message : "Failed to load dashboards";
        throw err;
      } finally {
        this.loadingLibrary = false;
      }
    },

    async loadDashboard(id: number) {
      this.loadingCurrent = true;
      this.error = null;
      try {
        const loaded = await CatalogApi.getDashboard(id);
        this.current = { ...loaded, layout: upgradeLayoutGrid(loaded.layout) };
      } catch (err) {
        this.error = err instanceof Error ? err.message : "Failed to load dashboard";
        throw err;
      } finally {
        this.loadingCurrent = false;
      }
    },

    async createDashboard(payload: DashboardCreatePayload): Promise<Dashboard> {
      this.saving = true;
      this.error = null;
      try {
        const created = await CatalogApi.createDashboard(payload);
        this.current = created;
        this.library.unshift(created);
        return created;
      } catch (err) {
        this.error = err instanceof Error ? err.message : "Failed to create dashboard";
        throw err;
      } finally {
        this.saving = false;
      }
    },

    async updateDashboard(id: number, patch: DashboardUpdatePayload): Promise<Dashboard> {
      this.saving = true;
      this.error = null;
      try {
        const updated = await CatalogApi.updateDashboard(id, patch);
        const withAccess = (prev: Dashboard): Dashboard => ({
          ...updated,
          access: updated.access ?? prev.access,
        });
        if (this.current?.id === id) this.current = withAccess(this.current);
        const idx = this.library.findIndex((d) => d.id === id);
        if (idx >= 0) this.library[idx] = withAccess(this.library[idx]);
        return updated;
      } catch (err) {
        this.error = err instanceof Error ? err.message : "Failed to save dashboard";
        throw err;
      } finally {
        this.saving = false;
      }
    },

    // Background autosave PUT: no saving-flag toggle, metadata-only merge.
    async autosaveDashboard(id: number, patch: DashboardUpdatePayload): Promise<Dashboard> {
      const updated = await CatalogApi.updateDashboard(id, patch, {
        timeout: AUTOSAVE_REQUEST_TIMEOUT_MS,
      });
      const merge = (prev: Dashboard): Dashboard => ({
        ...prev,
        name: updated.name,
        description: updated.description,
        updated_at: updated.updated_at,
        layout_version: updated.layout_version,
        namespace_id: updated.namespace_id,
        namespace_name: updated.namespace_name,
        access: updated.access ?? prev.access,
      });
      if (this.current?.id === id) this.current = merge(this.current);
      const idx = this.library.findIndex((d) => d.id === id);
      if (idx >= 0) this.library[idx] = merge(this.library[idx]);
      return updated;
    },

    async deleteDashboard(id: number): Promise<void> {
      this.error = null;
      try {
        await CatalogApi.deleteDashboard(id);
        this.library = this.library.filter((d) => d.id !== id);
        if (this.current?.id === id) this.current = null;
      } catch (err) {
        this.error = err instanceof Error ? err.message : "Failed to delete dashboard";
        throw err;
      }
    },

    setLayout(layout: DashboardLayout) {
      if (this.current) this.current = { ...this.current, layout };
    },

    newBlankDashboard(): Dashboard {
      const blank: Dashboard = {
        id: 0,
        name: "Untitled dashboard",
        description: null,
        layout: { ...EMPTY_DASHBOARD_LAYOUT, tiles: [], filters: [] },
        layout_version: 1,
        namespace_id: null,
        namespace_name: null,
        created_by: null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      this.current = blank;
      return blank;
    },

    reset() {
      this.current = null;
      this.error = null;
    },
  },
});
