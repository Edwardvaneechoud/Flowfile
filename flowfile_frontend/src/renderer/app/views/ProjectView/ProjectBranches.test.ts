// @vitest-environment happy-dom
// The project's branch card: where the branch stands against the remote, and a pull's merge settled item by item.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useFlowStore } from "../../stores/flow-store";
import { useProjectStore } from "../../stores/project-store";
import { element, mountIn, settle, text } from "../../__fixtures__/mount";
import type { FunctionalComponent } from "vue";
import { h } from "vue";
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
  concludeMerge: vi.fn(),
  openMergeFlow: vi.fn(),
  pull: vi.fn(),
  push: vi.fn(),
  setRemote: vi.fn(),
  switchBranch: vi.fn(),
  startGithubSignIn: vi.fn(),
  pollGithubSignIn: vi.fn(),
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

  const scheduleItem = {
    path: "schedules/nightly.yaml",
    key: "schedule:3",
    kind: "both_changed",
    label: "schedule nightly",
    message: "Both sides changed the schedule.",
    step_id: null,
    ours_label: "feature (this copy)",
    theirs_label: "origin/feature",
    ours: ["cron: '0 6 * * *' → '0 5 * * *'"],
    theirs: ["cron: '0 6 * * *' → '0 7 * * *'"],
  };

  it("offers the editor only for a flow's step, and shows a schedule's two sides", async () => {
    api.getMerge.mockResolvedValue({
      ...merge,
      conflicts: [...merge.conflicts, scheduleItem],
      flows: [...merge.flows, scheduleItem.path],
    });
    const host = await mountCard();
    const items = host.querySelectorAll(".merge-item");
    expect(items).toHaveLength(2);
    expect(items[0].querySelector('[data-testid="project-merge-open"]')).not.toBeNull();
    expect(items[1].querySelector('[data-testid="project-merge-open"]')).toBeNull();
    expect(text(items[1])).toContain("cron: '0 6 * * *' → '0 5 * * *'");
    expect(text(items[1])).toContain("cron: '0 6 * * *' → '0 7 * * *'");
  });

  it("re-reads the merge after a settle fails, so the panel shows what core holds", async () => {
    api.getMerge.mockResolvedValueOnce(merge).mockResolvedValueOnce({ ...merge, conflicts: [] });
    api.settleMerge.mockRejectedValue(new Error("Nothing to settle at flows/nightly_revenue"));
    const host = await mountCard();
    const take = Array.from(host.querySelectorAll("button")).find(
      (b) => text(b) === "Keep feature (this copy)",
    );
    take!.click();
    await settle();
    expect(api.getMerge).toHaveBeenCalledTimes(2);
    const panel = host.querySelector('[data-testid="project-merge"]');
    expect(text(panel)).toContain("0 to settle");
    expect(text(panel)).toContain("Finish the merge");
  });

  it("a pull that applied an import leaves the designer on no flow and keeps the secrets to fill", async () => {
    api.getActive.mockResolvedValue({ project: { id: 1, name: "Demo" }, dirty: false });
    api.getMerge.mockResolvedValue(null);
    api.pull.mockResolvedValue({
      state: "fast_forward",
      sha: "c".repeat(40),
      result: { imported: { flows: 1 }, placeholder_secrets: ["warehouse_pw"] },
      conflicts: [],
      warnings: [],
    });
    const host = await mountCard();
    useFlowStore().setFlowId(5);
    Array.from(host.querySelectorAll("button"))
      .find((b) => text(b) === "Pull")!
      .click();
    await settle();
    expect(api.pull).toHaveBeenCalledWith(false);
    expect(useFlowStore().flowId).toBe(-1);
    expect(useProjectStore().placeholderSecrets).toEqual(["warehouse_pw"]);
  });

  it("a pull stopped on conflicts keeps the designer's flow", async () => {
    api.getMerge.mockResolvedValueOnce(null).mockResolvedValue(merge);
    api.pull.mockResolvedValue({
      state: "conflicted",
      sha: "a".repeat(40),
      result: null,
      conflicts: merge.conflicts,
      warnings: [],
    });
    const host = await mountCard();
    useFlowStore().setFlowId(5);
    Array.from(host.querySelectorAll("button"))
      .find((b) => text(b) === "Pull")!
      .click();
    await settle();
    expect(useFlowStore().flowId).toBe(5);
    expect(host.querySelector('[data-testid="project-merge"]')).not.toBeNull();
  });

  describe("signing in to a GitHub remote", () => {
    const github = { ...branches, remote: "https://github.com/acme/project.git", has_token: false };
    const button = (host: HTMLElement, label: string) =>
      Array.from(host.querySelectorAll("button")).find((b) => text(b) === label)!;

    beforeEach(() => {
      vi.useFakeTimers();
      api.getMerge.mockResolvedValue(null);
    });
    afterEach(() => vi.useRealTimers());

    it("shows the code, waits for GitHub, then keeps the address with the token core stored", async () => {
      api.getBranches.mockResolvedValue(github);
      api.startGithubSignIn.mockResolvedValue({
        device_code: "dev-1",
        user_code: "ABCD-1234",
        verification_uri: "https://github.com/login/device",
        interval: 5,
        expires_in: 900,
      });
      api.pollGithubSignIn
        .mockResolvedValueOnce({ status: "pending", interval: 5 })
        .mockResolvedValueOnce({ status: "connected", login: "ben" });
      api.setRemote.mockResolvedValue({ ...github, has_token: true });
      const host = await mountCard();
      expect(host.querySelector(".no-token")).not.toBeNull();
      button(host, "Change").click();
      await settle();
      (host.querySelector('[data-testid="remote-github-start"]') as HTMLElement).click();
      await settle();
      expect(text(host.querySelector('[data-testid="remote-github-code"]'))).toBe("ABCD-1234");

      await vi.advanceTimersByTimeAsync(5000);
      await settle();
      expect(api.pollGithubSignIn).toHaveBeenCalledTimes(1);
      expect(api.setRemote).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(5000);
      await settle();
      expect(api.pollGithubSignIn).toHaveBeenLastCalledWith("dev-1");
      expect(api.setRemote).toHaveBeenCalledWith(
        "https://github.com/acme/project.git",
        undefined,
        undefined,
      );
      expect(host.querySelector(".no-token")).toBeNull();
    });

    it("says why a sign-in failed and offers it again", async () => {
      api.getBranches.mockResolvedValue(github);
      api.startGithubSignIn.mockResolvedValue({
        device_code: "dev-2",
        user_code: "WXYZ-0000",
        verification_uri: "https://github.com/login/device",
        interval: 5,
        expires_in: 900,
      });
      api.pollGithubSignIn.mockRejectedValue(new Error("The sign-in code expired. Start again."));
      const host = await mountCard();
      button(host, "Change").click();
      await settle();
      (host.querySelector('[data-testid="remote-github-start"]') as HTMLElement).click();
      await settle();
      await vi.advanceTimersByTimeAsync(5000);
      await settle();
      expect(text(host.querySelector('[data-testid="remote-github-error"]'))).toBe(
        "The sign-in code expired. Start again.",
      );
      expect(host.querySelector('[data-testid="remote-github-start"]')).not.toBeNull();
      expect(api.setRemote).not.toHaveBeenCalled();
    });

    it("offers no GitHub sign-in for another host", async () => {
      const host = await mountCard();
      button(host, "Change").click();
      await settle();
      expect(host.querySelector('[data-testid="remote-github-signin"]')).toBeNull();
    });
  });

  it("shows the switch under way until core has rebuilt the catalog", async () => {
    api.getMerge.mockResolvedValue(null);
    let finish: (value: unknown) => void = () => undefined;
    api.switchBranch.mockReturnValue(new Promise((resolve) => (finish = resolve)));
    const pickMain: FunctionalComponent = (_, { attrs }) =>
      h("button", {
        "data-testid": "pick-main",
        onClick: () => (attrs.onChange as (name: string) => void)("main"),
      });
    const pinia = createPinia();
    setActivePinia(pinia);
    const host = await mountIn(ProjectBranches, {
      pinia,
      stubs: { ...stubs, "el-select": pickMain },
    });
    expect(host.querySelector('[data-testid="project-branch-switching"]')).toBeNull();

    (host.querySelector('[data-testid="pick-main"]') as HTMLElement).click();
    await settle();
    expect(api.switchBranch).toHaveBeenCalledWith("main", false);
    expect(text(host.querySelector('[data-testid="project-branch-switching"]'))).toBe(
      "Switching to main and updating the catalog…",
    );
    expect(host.querySelector('[data-testid="project-branch-sync"]')).toBeNull();

    finish({
      imported: { flows: 1, connections: 0, schedules: 0, by_kind: {} },
      placeholder_secrets: [],
      unknown_kinds: [],
      not_applied: [],
      incomplete: [],
      recovery_sha: null,
    });
    await settle();
    expect(host.querySelector('[data-testid="project-branch-switching"]')).toBeNull();
    expect(host.querySelector('[data-testid="project-branch-sync"]')).not.toBeNull();
  });
});
