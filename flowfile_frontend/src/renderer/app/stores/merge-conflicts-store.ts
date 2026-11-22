// The merge conflicts the open flow holds (a hub merge or a project pull's), settled one side at a time.
import { defineStore } from "pinia";
import {
  listMergeConflicts,
  resolveMergeConflict,
  type MergeConflictRow,
} from "../api/hubFlows.api";
import { FlowApi } from "../api/flow.api";
import { detailOf } from "../utils/apiDetail";
import { settledNotice, type SettledNotice } from "../features/designer/mergeConflicts";
import { useFlowStore } from "./flow-store";
import { useProjectStore } from "./project-store";

let loadSeq = 0;

export const useMergeConflictsStore = defineStore("mergeConflicts", {
  state: () => ({
    flowId: null as number | null,
    conflicts: [] as MergeConflictRow[],
    expanded: false,
    busy: false,
    error: "",
    settled: null as SettledNotice | null,
  }),

  actions: {
    /** Re-read the open flow's conflicts; another flow starts from nothing. */
    async load(flowId: number): Promise<void> {
      if (flowId !== this.flowId) {
        this.flowId = flowId;
        this.conflicts = [];
        this.expanded = false;
        this.error = "";
        this.settled = null;
      }
      if (flowId < 0) return;
      const seq = ++loadSeq;
      let rows: MergeConflictRow[];
      try {
        rows = await listMergeConflicts(flowId);
      } catch {
        rows = [];
      }
      if (seq !== loadSeq || this.flowId !== flowId) return;
      this.conflicts = rows;
      if (rows.length) this.settled = null;
    },

    /** Open the list, as a run refused for the conflicts does. */
    async reveal(flowId: number): Promise<void> {
      await this.load(flowId);
      if (this.flowId === flowId) this.expanded = true;
    },

    /** Take one side of one conflict; core applies it as one undo step and answers the conflicts left. */
    async resolve(key: string, take: "ours" | "theirs"): Promise<void> {
      const flowId = this.flowId;
      if (flowId === null || this.busy) return;
      this.busy = true;
      this.error = "";
      try {
        const rows = await resolveMergeConflict(flowId, key, take);
        if (this.flowId !== flowId) return;
        loadSeq++;
        this.conflicts = rows;
        // The mutation channel applied the history state; the canvas still has to redraw the step.
        useFlowStore().requestReload();
        if (!rows.length) {
          const notice = await this.nextStep(flowId);
          if (this.flowId === flowId && !this.conflicts.length) this.settled = notice;
        }
      } catch (error) {
        this.error = detailOf(error, "Couldn't settle the conflict.");
        await this.load(flowId);
      } finally {
        this.busy = false;
      }
    },

    async nextStep(flowId: number): Promise<SettledNotice> {
      const project = useProjectStore();
      if (project.isMergeFlow(flowId)) {
        const outcome = await project.afterEditorSettle().catch(() => null);
        return settledNotice({ project: outcome ?? { state: "conclude" } });
      }
      const settings = await FlowApi.getFlowSettings(flowId).catch(() => null);
      return settledNotice({ hub: !!settings?.hub_origin });
    },

    dismissSettled(): void {
      this.settled = null;
    },
  },
});
