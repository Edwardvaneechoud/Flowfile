// @vitest-environment happy-dom
// The project's branch card: where the branch stands against the remote, and a pull's merge settled item by item.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { element, mountIn, settle, text } from "../../__fixtures__/mount";
import type { ProjectMergeState } from "../../types";

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

const api = vi.hoisted(() => ({
  getBranches: vi.fn(),
  getMerge: vi.fn(),
  settleMerge: vi.fn(),
  pull: vi.fn(),
  push: vi.fn(),
  getActive: vi.fn(),
  getVersions: vi.fn(),
}));

vi.mock("../../api/project.api", () => ({
  ProjectApi: api,
  ProjectFeatureUnavailable: class extends Error {},
}));
vi.mock("vue-router", () => ({ useRouter: () => ({ push: vi.fn() }) }));
vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), warning: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn(), prompt: vi.fn(), alert: vi.fn() },
}));

import ProjectBranches from "./ProjectBranches.vue";

const branches = {
  current: "feature",
  branches: [
    { name: "feature", sha: "a".repeat(40), current: true, ahead: 2, behind: 1 },
    { name: "main", sha: "b".repeat(40), current: false, ahead: 0, behind: 0 },
  ],
  remote: "https://git.example.invalid/team/project.git",
  has_token: true,
};

const merge: ProjectMergeState = {
  branch: "feature",
  ours: "a".repeat(40),
  theirs: "c".repeat(40),
  sides: { base: "base", ours: "feature (this copy)", theirs: "origin/feature" },
  conflicts: [
    {
      path: "flows/nightly_revenue.flow.yaml",
      key: "step:6",
      kind: "both_changed",
      label: "step 6 join",
      message: "Both sides changed the step.",
      step_id: 6,
      ours_label: "feature (this copy)",
      theirs_label: "origin/feature",
      ours: ["settings.how: 'left' → 'inner'"],
      theirs: ["settings.how: 'left' → 'outer'"],
    },
  ],
  flows: ["flows/nightly_revenue.flow.yaml"],
  warnings: [],
};

const stubs = {
  "el-button": element("button"),
  "el-select": element("div"),
  "el-option": element("div"),
  "el-dialog": element("div"),
  "el-input": element("input"),
};

beforeEach(() => {
  vi.clearAllMocks();
  api.getBranches.mockResolvedValue(branches);
  api.getActive.mockResolvedValue({ project: null });
  api.getVersions.mockResolvedValue([]);
});

const mountCard = async () => {
  const pinia = createPinia();
  setActivePinia(pinia);
  return mountIn(ProjectBranches, { pinia, stubs });
};

describe("ProjectBranches", () => {
  it("says how the branch stands against the remote", async () => {
    api.getMerge.mockResolvedValue(null);
    const host = await mountCard();
    expect(text(host.querySelector('[data-testid="project-branch-sync"]'))).toBe(
      "2 versions to push · 1 version to pull",
    );
    expect(host.querySelector('[data-testid="project-merge"]')).toBeNull();
  });

  it("lists a waiting merge's conflict with both sides and settles it on the side picked", async () => {
    api.getMerge.mockResolvedValue(merge);
    api.settleMerge.mockResolvedValue({ merge: { ...merge, conflicts: [] }, pull: null });
    const host = await mountCard();
    const panel = host.querySelector('[data-testid="project-merge"]');
    expect(text(panel)).toContain("nightly_revenue · step 6 join");
    expect(text(panel)).toContain("settings.how: 'left' → 'inner'");
    expect(text(panel)).toContain("settings.how: 'left' → 'outer'");

    const take = Array.from(panel!.querySelectorAll("button")).find(
      (b) => text(b) === "Take origin/feature",
    );
    take!.click();
    await settle();
    expect(api.settleMerge).toHaveBeenCalledWith(
      "flows/nightly_revenue.flow.yaml",
      "step:6",
      "theirs",
      false,
    );
    expect(text(host.querySelector('[data-testid="project-merge"]'))).toContain("0 to settle");
  });
});
