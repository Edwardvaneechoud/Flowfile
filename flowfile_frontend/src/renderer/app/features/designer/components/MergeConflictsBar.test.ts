// @vitest-environment happy-dom
// The designer's merge conflict bar: shown for a flow holding conflicts, settled side by side, gone at zero.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { element, mountIn, settle, text } from "../../../__fixtures__/mount";

vi.hoisted(() => {
  const store = () => {
    const map = new Map<string, string>();
    return {
      getItem: (k: string) => map.get(k) ?? null,
      setItem: (k: string, v: string) => void map.set(k, v),
      removeItem: (k: string) => void map.delete(k),
      clear: () => map.clear(),
    };
  };
  Object.defineProperty(globalThis, "localStorage", { value: store(), configurable: true });
  Object.defineProperty(globalThis, "sessionStorage", { value: store(), configurable: true });
});

const mocks = vi.hoisted(() => ({
  listMergeConflicts: vi.fn(),
  resolveMergeConflict: vi.fn(),
  getFlowSettings: vi.fn(),
  getMerge: vi.fn(),
  push: vi.fn(),
}));

vi.mock("../../../api/hubFlows.api", () => ({
  listMergeConflicts: mocks.listMergeConflicts,
  resolveMergeConflict: mocks.resolveMergeConflict,
}));
vi.mock("../../../api/flow.api", () => ({ FlowApi: { getFlowSettings: mocks.getFlowSettings } }));
vi.mock("../../../api/project.api", () => ({
  ProjectApi: { getMerge: mocks.getMerge, getActive: vi.fn(async () => ({ project: null })) },
  ProjectFeatureUnavailable: class extends Error {},
}));
vi.mock("vue-router", () => ({ useRouter: () => ({ push: mocks.push }) }));

import MergeConflictsBar from "./MergeConflictsBar.vue";
import { useFlowStore } from "../../../stores/flow-store";
import { useProjectStore } from "../../../stores/project-store";

const row = {
  key: "step:6",
  kind: "both_changed",
  label: "step 6 join",
  message: "Both sides changed the step.",
  step_id: 6,
  ours_label: "main (this copy)",
  theirs_label: "origin/main",
  ours: ["settings.how: 'left' → 'inner'"],
  theirs: ["settings.how: 'left' → 'outer'"],
};

const stubs = { "el-button": element("button") };

const mountBar = async (flowId: number, setup?: () => void) => {
  const pinia = createPinia();
  setActivePinia(pinia);
  useFlowStore().setFlowId(flowId);
  setup?.();
  const host = await mountIn(MergeConflictsBar, { pinia, stubs });
  const byTestId = (id: string) =>
    host.querySelector(`[data-testid="${id}"]`) as HTMLElement | null;
  const click = async (id: string) => {
    byTestId(id)!.click();
    await settle();
  };
  return { host, byTestId, click };
};

beforeEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
  mocks.getFlowSettings.mockResolvedValue({ hub_origin: null });
});

describe("MergeConflictsBar", () => {
  it("stays away for a flow holding no conflicts", async () => {
    mocks.listMergeConflicts.mockResolvedValue([]);
    const { byTestId } = await mountBar(3);
    expect(mocks.listMergeConflicts).toHaveBeenCalledWith(3);
    expect(byTestId("merge-conflicts-bar")).toBeNull();
  });

  it("lists a conflict with both sides, settles it, and says to finish a pull on the project page", async () => {
    mocks.listMergeConflicts.mockResolvedValue([row]);
    mocks.resolveMergeConflict.mockResolvedValue([]);
    mocks.getMerge.mockResolvedValue({ conflicts: [] });
    const { byTestId, click } = await mountBar(42, () => {
      useProjectStore().mergeFlows = { 42: "flows/nightly.flow.yaml" };
    });

    expect(text(byTestId("merge-conflicts-bar"))).toContain(
      "1 merge conflict to settle before this flow can run",
    );
    await click("merge-conflicts-toggle");
    const item = text(byTestId("merge-bar-step:6"));
    for (const piece of [
      "step 6 join",
      "Both sides changed the step.",
      "settings.how: 'left' → 'inner'",
      "settings.how: 'left' → 'outer'",
      "Keep main (this copy)",
      "Keep origin/main",
    ]) {
      expect(item).toContain(piece);
    }

    mocks.listMergeConflicts.mockResolvedValue([]);
    await click("merge-bar-theirs-step:6");
    expect(mocks.resolveMergeConflict).toHaveBeenCalledWith(42, "step:6", "theirs");
    expect(useFlowStore().pendingReloadCounter).toBeGreaterThan(0);
    expect(byTestId("merge-bar-step:6")).toBeNull();
    expect(text(byTestId("merge-conflicts-settled"))).toContain(
      "Go back to the project page to finish the merge.",
    );

    await click("merge-conflicts-to-project");
    expect(mocks.push).toHaveBeenCalledWith({ name: "project" });
    expect(byTestId("merge-conflicts-bar")).toBeNull();
  });
});
