import { describe, expect, it } from "vitest";
import { conflictSides, keepLabel, type MergeConflictView } from "./mergeConflict";

const item: MergeConflictView = {
  label: "step 6 join",
  message: "Both sides changed the step.",
  ours_label: "feature (this copy)",
  theirs_label: "origin/feature",
  ours: ["settings.how: 'left' → 'inner'"],
  theirs: ["settings.how: 'left' → 'outer'"],
};

describe("conflictSides", () => {
  it("lists ours then theirs, each with its lines and a Keep button", () => {
    expect(conflictSides(item)).toEqual([
      {
        side: "ours",
        name: "feature (this copy)",
        lines: ["settings.how: 'left' → 'inner'"],
        action: "Keep feature (this copy)",
      },
      {
        side: "theirs",
        name: "origin/feature",
        lines: ["settings.how: 'left' → 'outer'"],
        action: "Keep origin/feature",
      },
    ]);
  });

  it("puts theirs first when asked and takes the caller's button labels", () => {
    const sides = conflictSides(item, {
      theirsFirst: true,
      actionLabel: (side, name) => (side === "ours" ? keepLabel(name) : `Take ${name}`),
    });
    expect(sides.map((s) => s.side)).toEqual(["theirs", "ours"]);
    expect(sides.map((s) => s.action)).toEqual(["Take origin/feature", "Keep feature (this copy)"]);
  });

  it("shows the lines of a schedule or parameter item like a step's", () => {
    const schedule = { ...item, ours: ["cron: '0 6 * * *'"], theirs: ["cron: '0 7 * * *'"] };
    expect(conflictSides(schedule).map((s) => s.lines)).toEqual([
      ["cron: '0 6 * * *'"],
      ["cron: '0 7 * * *'"],
    ]);
  });
});
