import type { ProjectBranch, ProjectMergeItem, ProjectPullResult } from "../../types";

const BRANCH_RE = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$/;

/** Mirrors core's `git_ops.check_branch_name`, so the dialog refuses what core would. */
export const branchNameError = (name: string): string | null => {
  const trimmed = name.trim();
  if (!trimmed) return "Give the branch a name.";
  if (
    !BRANCH_RE.test(trimmed) ||
    trimmed.includes("..") ||
    trimmed.includes("//") ||
    /(\.|\/|\.lock)$/.test(trimmed)
  ) {
    return "Use letters, digits, dots, dashes, underscores and slashes; start with a letter or digit.";
  }
  return null;
};

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** How a branch stands against the remote's copy, as last fetched. */
export const syncText = (branch: ProjectBranch | null, hasRemote: boolean): string => {
  if (!branch || !hasRemote) return "";
  if (branch.ahead === null || branch.behind === null) return "Not on the remote yet";
  if (!branch.ahead && !branch.behind) return "Up to date with the remote";
  const parts = [];
  if (branch.ahead) parts.push(`${plural(branch.ahead, "version")} to push`);
  if (branch.behind) parts.push(`${plural(branch.behind, "version")} to pull`);
  return parts.join(" · ");
};

/** The one line a finished pull is reported with. */
export const pullMessage = (result: ProjectPullResult): string => {
  switch (result.state) {
    case "up_to_date":
      return "Already up to date with the remote.";
    case "ahead":
      return "Nothing to pull; push to share your versions.";
    case "fast_forward":
      return "Pulled the remote's versions into this project.";
    case "merged":
      return "Merged the remote's versions into this project.";
    case "conflicted":
      return `${plural(result.conflicts.length, "conflict")} to settle before the pull is applied.`;
  }
};

/**
 * Only a step conflict of a conflicted flow file opens in the editor (`flows` is the merge's list); a whole file,
 * a schedule or a parameter is settled here.
 */
export const opensInEditor = (item: ProjectMergeItem, flows: string[]): boolean =>
  !!item.key?.startsWith("step:") && flows.includes(item.path);

export const takeLabel = (item: ProjectMergeItem, side: "ours" | "theirs"): string =>
  side === "ours" ? `Keep ${item.ours_label}` : `Take ${item.theirs_label}`;

/** The flow a conflict sits in, by its file name: `flows/nightly_revenue.flow.yaml` -> `nightly_revenue`. */
export const itemFlowName = (item: ProjectMergeItem): string =>
  (item.path.split("/").pop() ?? item.path).replace(/\.flow\.yaml$|\.yaml$/, "");
