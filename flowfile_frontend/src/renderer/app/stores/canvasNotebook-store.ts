import { defineStore } from "pinia";
import {
  CanvasNotebookApi,
  type CanvasNotebookStatus,
  type NotebookRendering,
  type RenderedCellKind,
  type RenderedCellStatus,
} from "../api/canvasNotebook.api";

export const DOCK_WIDTH_KEY = "flowfile.canvasNotebook.width.v1";
export const DOCK_MIN_WIDTH = 280;
export const DOCK_DEFAULT_WIDTH = 460;

/** A rendered cell as the dock holds it; `base` is the last rendered text (drafts come later). */
export interface DockCell {
  cellId: string;
  kind: RenderedCellKind;
  nodeIds: number[];
  status: RenderedCellStatus;
  reason: string | null;
  base: string;
}

interface CanvasNotebookState {
  status: CanvasNotebookStatus | null;
  statusLoaded: boolean;
  open: boolean;
  width: number;
  flowId: number | null;
  cells: DockCell[];
  warnings: string[];
  fingerprint: string | null;
  /** Renderings applied (a changed fingerprint); layout-only refreshes leave it alone. */
  renderCount: number;
  loading: boolean;
  error: string | null;
}

const KIND_ORDER: Record<RenderedCellKind, number> = { imports: 0, parameters: 1, node: 2 };

/** Imports, then parameters, then node cells in the renderer's order. */
export function toDockCells(rendering: NotebookRendering): DockCell[] {
  return rendering.cells
    .map((cell, index) => ({ cell, index }))
    .sort((a, b) => KIND_ORDER[a.cell.kind] - KIND_ORDER[b.cell.kind] || a.index - b.index)
    .map(({ cell }) => ({
      cellId: cell.cell_id,
      kind: cell.kind,
      nodeIds: cell.node_ids,
      status: cell.status,
      reason: cell.reason,
      base: cell.code,
    }));
}

export function clampDockWidth(width: number, viewportWidth: number): number {
  const max = Math.max(DOCK_MIN_WIDTH, Math.floor(viewportWidth * 0.7));
  return Math.min(max, Math.max(DOCK_MIN_WIDTH, Math.round(width)));
}

function readStoredWidth(): number {
  try {
    const raw = Number(localStorage.getItem(DOCK_WIDTH_KEY));
    return Number.isFinite(raw) && raw >= DOCK_MIN_WIDTH ? raw : DOCK_DEFAULT_WIDTH;
  } catch {
    return DOCK_DEFAULT_WIDTH;
  }
}

export const useCanvasNotebookStore = defineStore("canvasNotebook", {
  state: (): CanvasNotebookState => ({
    status: null,
    statusLoaded: false,
    open: false,
    width: readStoredWidth(),
    flowId: null,
    cells: [],
    warnings: [],
    fingerprint: null,
    renderCount: 0,
    loading: false,
    error: null,
  }),

  getters: {
    available: (state): boolean => !!state.status?.canvas_notebook,
    sessionsEnabled: (state): boolean => !!state.status?.sessions,
  },

  actions: {
    /** Load-once; a failure or a 503 leaves the feature unavailable, never throws. */
    async loadStatus(force = false): Promise<void> {
      if (this.statusLoaded && !force) return;
      try {
        this.status = await CanvasNotebookApi.getStatus();
      } catch {
        this.status = null;
      }
      this.statusLoaded = true;
      if (!this.available) this.open = false;
    },

    toggle(): void {
      this.open = this.available && !this.open;
    },

    setOpen(open: boolean): void {
      this.open = this.available && open;
    },

    /** Forget the previous flow's cells so its fingerprint cannot mask the next flow's first render. */
    resetForFlow(flowId: number | null): void {
      if (this.flowId === flowId) return;
      this.flowId = flowId;
      this.cells = [];
      this.warnings = [];
      this.fingerprint = null;
      this.error = null;
    },

    /** Apply a rendering for `flowId`; false when it is stale or its code is unchanged. */
    applyRendering(flowId: number, rendering: NotebookRendering): boolean {
      if (flowId !== this.flowId) return false;
      this.error = null;
      if (rendering.code_fingerprint === this.fingerprint) return false;
      this.fingerprint = rendering.code_fingerprint;
      this.cells = toDockCells(rendering);
      this.warnings = rendering.warnings;
      this.renderCount += 1;
      return true;
    },

    setError(flowId: number, message: string | null): void {
      if (flowId === this.flowId) this.error = message;
    },

    setWidth(width: number, persist = false): void {
      this.width = width;
      if (!persist) return;
      try {
        localStorage.setItem(DOCK_WIDTH_KEY, String(Math.round(width)));
      } catch {
        /* storage unavailable: the width still applies for this session */
      }
    },
  },
});
