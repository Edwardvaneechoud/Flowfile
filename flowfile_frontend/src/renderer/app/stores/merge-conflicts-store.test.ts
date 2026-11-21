// The open flow's merge conflicts: loaded per flow, settled one side at a time, and what comes next at zero.
import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  listMergeConflicts: vi.fn(),
  resolveMergeConflict: vi.fn(),
  getFlowSettings: vi.fn(),
  requestReload: vi.fn(),
  isMergeFlow: vi.fn(),
  afterEditorSettle: vi.fn(),
}));

vi.mock("../api/hubFlows.api", () => ({
  listMergeConflicts: mocks.listMergeConflicts,
  resolveMergeConflict: mocks.resolveMergeConflict,
}));
vi.mock("../api/flow.api", () => ({ FlowApi: { getFlowSettings: mocks.getFlowSettings } }));
vi.mock("./flow-store", () => ({ useFlowStore: () => ({ requestReload: mocks.requestReload }) }));
vi.mock("./project-store", () => ({
  useProjectStore: () => ({
    isMergeFlow: mocks.isMergeFlow,
    afterEditorSettle: mocks.afterEditorSettle,
  }),
}));

import { useMergeConflictsStore } from "./merge-conflicts-store";

const row = (key: string) => ({
  key,
  kind: "both_changed",
  label: key,
  message: "",
  step_id: 2,
  ours_label: "main (this copy)",
  theirs_label: "origin/main",
  ours: [],
  theirs: [],
});

beforeEach(() => {
  setActivePinia(createPinia());
  vi.clearAllMocks();
  mocks.isMergeFlow.mockReturnValue(false);
  mocks.getFlowSettings.mockResolvedValue({ hub_origin: null });
});

describe("merge conflicts store", () => {
  it("loads the open flow's conflicts and starts over on another flow", async () => {
    const store = useMergeConflictsStore();
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2")]);
    await store.load(7);
    expect(store.conflicts.map((c) => c.key)).toEqual(["step:2"]);

    store.expanded = true;
    mocks.listMergeConflicts.mockResolvedValueOnce([]);
    await store.load(8);
    expect(store.conflicts).toEqual([]);
    expect(store.expanded).toBe(false);
  });

  it("drops a late answer for a flow the designer left", async () => {
    const store = useMergeConflictsStore();
    let answer!: (rows: unknown[]) => void;
    mocks.listMergeConflicts.mockReturnValueOnce(new Promise((resolve) => (answer = resolve)));
    const first = store.load(7);
    mocks.listMergeConflicts.mockResolvedValueOnce([]);
    await store.load(8);
    answer([row("step:2")]);
    await first;
    expect(store.flowId).toBe(8);
    expect(store.conflicts).toEqual([]);
  });

  it("settles a side, redraws the canvas and keeps the conflicts left", async () => {
    const store = useMergeConflictsStore();
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2"), row("step:3")]);
    await store.load(7);
    mocks.resolveMergeConflict.mockResolvedValueOnce([row("step:3")]);
    await store.resolve("step:2", "theirs");
    expect(mocks.resolveMergeConflict).toHaveBeenCalledWith(7, "step:2", "theirs");
    expect(mocks.requestReload).toHaveBeenCalled();
    expect(store.conflicts.map((c) => c.key)).toEqual(["step:3"]);
    expect(store.settled).toBeNull();
  });

  it("at zero on a flow from a project pull, says to finish the merge on the project page", async () => {
    const store = useMergeConflictsStore();
    mocks.isMergeFlow.mockReturnValue(true);
    mocks.afterEditorSettle.mockResolvedValue({ state: "conclude" });
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2")]);
    await store.load(7);
    mocks.resolveMergeConflict.mockResolvedValueOnce([]);
    await store.resolve("step:2", "ours");
    expect(store.conflicts).toEqual([]);
    expect(store.settled).toEqual({
      message:
        "Every conflict in this flow is settled. Go back to the project page to finish the merge.",
      toProject: true,
    });
  });

  it("at zero on a hub flow, says to publish", async () => {
    const store = useMergeConflictsStore();
    mocks.getFlowSettings.mockResolvedValue({ hub_origin: { version: 5 } });
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2")]);
    await store.load(7);
    mocks.resolveMergeConflict.mockResolvedValueOnce([]);
    await store.resolve("step:2", "ours");
    expect(store.settled?.message).toMatch(/Publish/);
    expect(store.settled?.toProject).toBe(false);
  });

  it("re-reads the conflicts after a failed settle, so no row stays that core no longer holds", async () => {
    const store = useMergeConflictsStore();
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2"), row("step:3")]);
    await store.load(7);
    mocks.resolveMergeConflict.mockRejectedValueOnce({
      response: { status: 404, data: { detail: "No open merge conflict step:2 in this flow." } },
    });
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:3")]);
    await store.resolve("step:2", "ours");
    expect(store.error).toBe("No open merge conflict step:2 in this flow.");
    expect(store.conflicts.map((c) => c.key)).toEqual(["step:3"]);
    expect(store.busy).toBe(false);
  });

  it("opens the list when a run is refused for the conflicts", async () => {
    const store = useMergeConflictsStore();
    mocks.listMergeConflicts.mockResolvedValueOnce([row("step:2")]);
    await store.reveal(7);
    expect(store.expanded).toBe(true);
    expect(store.conflicts).toHaveLength(1);
  });
});
