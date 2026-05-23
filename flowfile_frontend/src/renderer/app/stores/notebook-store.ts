import { defineStore } from "pinia";

import { KernelApi } from "../api/kernel.api";
import type { KernelInfo, Notebook, NotebookCellData, NotebookSummary } from "../types";
import { NotebookApi } from "../views/NotebookView/api";

interface NotebookState {
  notebooks: NotebookSummary[];
  active: Notebook | null;
  kernels: KernelInfo[];
  selectedKernelId: string | null;
  loading: boolean;
  saving: boolean;
  error: string | null;
}

// Non-reactive debounce handle for cell persistence.
let _saveTimer: ReturnType<typeof setTimeout> | null = null;

export const useNotebookStore = defineStore("notebook", {
  state: (): NotebookState => ({
    notebooks: [],
    active: null,
    kernels: [],
    selectedKernelId: null,
    loading: false,
    saving: false,
    error: null,
  }),

  getters: {
    activeCells: (state): NotebookCellData[] => state.active?.cells ?? [],
    selectedKernel: (state): KernelInfo | null =>
      state.kernels.find((k) => k.id === state.selectedKernelId) ?? null,
    kernelReady(): boolean {
      const k = this.selectedKernel;
      return !!k && (k.state === "idle" || k.state === "executing");
    },
  },

  actions: {
    async loadNotebooks(): Promise<void> {
      this.loading = true;
      this.error = null;
      try {
        this.notebooks = await NotebookApi.list();
      } catch (e) {
        this.error = e instanceof Error ? e.message : "Failed to load notebooks";
      } finally {
        this.loading = false;
      }
    },

    async loadKernels(): Promise<void> {
      try {
        this.kernels = await KernelApi.getAll();
      } catch {
        this.kernels = [];
      }
    },

    async openNotebook(id: string): Promise<void> {
      this.loading = true;
      this.error = null;
      try {
        const notebook = await NotebookApi.get(id);
        this.active = notebook;
        this.selectedKernelId =
          notebook.kernel_id ??
          this.kernels.find((k) => k.state === "idle")?.id ??
          this.kernels[0]?.id ??
          null;
      } catch (e) {
        this.error = e instanceof Error ? e.message : "Failed to open notebook";
      } finally {
        this.loading = false;
      }
    },

    async createNotebook(name?: string): Promise<Notebook | null> {
      try {
        const notebook = await NotebookApi.create({ name, kernel_id: this.selectedKernelId });
        await this.loadNotebooks();
        this.active = notebook;
        return notebook;
      } catch (e) {
        this.error = e instanceof Error ? e.message : "Failed to create notebook";
        return null;
      }
    },

    async deleteNotebook(id: string): Promise<void> {
      await NotebookApi.remove(id);
      if (this.active?.id === id) this.active = null;
      await this.loadNotebooks();
    },

    /** Update cells locally and persist (debounced) so typing stays snappy. */
    persistCells(cells: NotebookCellData[]): void {
      if (!this.active) return;
      this.active.cells = cells;
      const id = this.active.id;
      if (_saveTimer) clearTimeout(_saveTimer);
      _saveTimer = setTimeout(() => {
        void this._save(id, { cells });
      }, 600);
    },

    async selectKernel(kernelId: string | null): Promise<void> {
      this.selectedKernelId = kernelId;
      if (this.active) await this._save(this.active.id, { kernel_id: kernelId });
    },

    async renameActive(name: string): Promise<void> {
      if (!this.active) return;
      this.active.name = name;
      await this._save(this.active.id, { name });
    },

    async startSelectedKernel(): Promise<void> {
      if (!this.selectedKernelId) return;
      try {
        await KernelApi.start(this.selectedKernelId);
      } finally {
        await this.loadKernels();
      }
    },

    async _save(
      id: string,
      payload: { cells?: NotebookCellData[]; kernel_id?: string | null; name?: string },
    ): Promise<void> {
      this.saving = true;
      try {
        await NotebookApi.update(id, payload);
      } catch (e) {
        this.error = e instanceof Error ? e.message : "Failed to save notebook";
      } finally {
        this.saving = false;
      }
    },
  },
});
