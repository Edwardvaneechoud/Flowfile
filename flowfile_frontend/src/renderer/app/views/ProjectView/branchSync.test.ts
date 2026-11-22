import { describe, expect, it } from "vitest";
import {
  branchNameError,
  isGithubAddress,
  itemFlowName,
  opensInEditor,
  pullMessage,
  syncText,
  takeLabel,
} from "./branchSync";
import type { ProjectBranch, ProjectMergeItem, ProjectPullResult } from "../../types";

const branch = (ahead: number | null, behind: number | null): ProjectBranch => ({
  name: "feature",
  sha: "a".repeat(40),
  current: true,
  ahead,
  behind,
});

const item: ProjectMergeItem = {
  path: "flows/nightly_revenue.flow.yaml",
  key: "step:6",
  kind: "both_changed",
  label: "step 6 join",
  message: "",
  step_id: 6,
  ours_label: "feature (this copy)",
  theirs_label: "origin/feature",
  ours: [],
  theirs: [],
};

const pull = (state: ProjectPullResult["state"], conflicts: ProjectMergeItem[] = []) => ({
  state,
  sha: null,
  result: null,
  conflicts,
  warnings: [],
});

describe("branchSync", () => {
  it("names how a branch stands against the remote", () => {
    expect(syncText(branch(0, 0), true)).toBe("Up to date with the remote");
    expect(syncText(branch(2, 1), true)).toBe("2 versions to push · 1 version to pull");
    expect(syncText(branch(null, null), true)).toBe("Not on the remote yet");
    expect(syncText(branch(1, 0), false)).toBe("");
  });

  it("refuses the branch names core refuses", () => {
    expect(branchNameError("feature/fx-rates")).toBeNull();
    for (const bad of ["", "-x", "a..b", "a//b", "x.lock", "x/", "a b"]) {
      expect(branchNameError(bad)).not.toBeNull();
    }
  });

  it("reports each pull outcome in one line", () => {
    expect(pullMessage(pull("up_to_date"))).toMatch(/up to date/);
    expect(pullMessage(pull("conflicted", [item, item]))).toBe(
      "2 conflicts to settle before the pull is applied.",
    );
    expect(pullMessage(pull("merged"))).toMatch(/Merged/);
  });

  it("labels both sides and the flow of a conflict", () => {
    expect(takeLabel(item, "ours")).toBe("Keep feature (this copy)");
    expect(takeLabel(item, "theirs")).toBe("Take origin/feature");
    expect(itemFlowName(item)).toBe("nightly_revenue");
  });

  it("opens only a step conflict of a conflicted flow file in the editor", () => {
    const flows = ["flows/nightly_revenue.flow.yaml", "schedules/nightly.yaml"];
    expect(opensInEditor(item, flows)).toBe(true);
    expect(
      opensInEditor({ ...item, path: "schedules/nightly.yaml", key: "schedule:3" }, flows),
    ).toBe(false);
    expect(opensInEditor({ ...item, key: "parameter:region" }, flows)).toBe(false);
    expect(opensInEditor({ ...item, key: null, kind: "file" }, flows)).toBe(false);
    expect(opensInEditor(item, [])).toBe(false);
  });
});

describe("isGithubAddress", () => {
  it("is a repository on github.com over https, and nothing else", () => {
    expect(isGithubAddress("https://github.com/acme/project.git")).toBe(true);
    expect(isGithubAddress("  https://GitHub.com/acme/project  ")).toBe(true);
    expect(isGithubAddress("https://github.com/acme")).toBe(false);
    expect(isGithubAddress("git@github.com:acme/project.git")).toBe(false);
    expect(isGithubAddress("https://github.example.com/acme/project.git")).toBe(false);
    expect(isGithubAddress("https://git.example.com/github.com/acme/project.git")).toBe(false);
  });
});
