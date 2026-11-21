import type { EditorSettleOutcome } from "../../stores/project-store";
import { SETTLED } from "./hubFlows";

const plural = (n: number, word: string): string => `${n} ${word}${n === 1 ? "" : "s"}`;

/** The conflict bar's heading while the open flow holds merge conflicts. */
export const conflictsTitle = (count: number): string =>
  `${plural(count, "merge conflict")} to settle before this flow can run`;

export interface SettledNotice {
  message: string;
  /** The next step is on the project page. */
  toProject: boolean;
}

/** What happens once the last conflict of the open flow is settled, from where the merge came from. */
export const settledNotice = (next: {
  project?: EditorSettleOutcome | null;
  hub?: boolean;
}): SettledNotice => {
  switch (next.project?.state) {
    case "merged":
      return {
        message: "Every conflict is settled and the pull is applied: the project holds the merge.",
        toProject: true,
      };
    case "conclude":
      return {
        message:
          "Every conflict in this flow is settled. Go back to the project page to finish the merge.",
        toProject: true,
      };
    case "settle_rest":
      return {
        message: `Every conflict in this flow is settled. ${plural(next.project.left, "item")} of the pull still to settle on the project page.`,
        toProject: true,
      };
  }
  if (next.hub) return { message: SETTLED, toProject: false };
  return { message: "Every conflict is settled: the flow can run again.", toProject: false };
};

export interface UnresolvedConflicts {
  count: number;
  message: string;
}

/** Core's 409 `UNRESOLVED_CONFLICTS` refusal of a run or a publish, null for any other error. */
export const unresolvedConflictsOf = (error: unknown): UnresolvedConflicts | null => {
  const response = (error as { response?: { status?: number; data?: { detail?: unknown } } })
    ?.response;
  const detail = response?.data?.detail as
    | { code?: unknown; count?: unknown; message?: unknown }
    | undefined;
  if (response?.status !== 409 || detail?.code !== "UNRESOLVED_CONFLICTS") return null;
  const count = typeof detail.count === "number" ? detail.count : 0;
  const message =
    typeof detail.message === "string" ? detail.message : "Settle the merge conflicts first.";
  return { count, message };
};

/** The refusal of a run, pointing at where the conflicts are settled. */
export const runRefusedMessage = (refusal: UnresolvedConflicts): string =>
  `${refusal.message} Pick a side for each in the merge conflicts bar above the canvas.`;
