// @vitest-environment happy-dom
// A version's change list names the steps that changed inside a changed flow, under that flow's row.
import { describe, expect, it } from "vitest";
import { mountIn, text } from "../../__fixtures__/mount";
import ChangeList from "./ChangeList.vue";
import type { ProjectVersionChange } from "../../types";

const flow = (extra: Partial<ProjectVersionChange>): ProjectVersionChange => ({
  change: "modified",
  kind: "flow",
  label: "nightly_revenue",
  path: "flows/nightly_revenue.flow.yaml",
  ...extra,
});

describe("ChangeList", () => {
  it("lists a changed flow's steps with the values that differ", async () => {
    const host = await mountIn(ChangeList, {
      props: {
        changes: [
          flow({
            steps: [
              {
                kind: "changed",
                label: 'step 6 join "left join"',
                details: ["settings.join_input.how: 'left' → 'inner'"],
              },
              { kind: "added", label: "step 9 filter", details: [] },
            ],
          }),
        ],
      },
    });
    const steps = host.querySelector('[data-testid="change-steps"]');
    expect(text(steps)).toContain("changed step 6 join");
    expect(text(steps)).toContain("settings.join_input.how: 'left' → 'inner'");
    expect(text(steps)).toContain("added step 9 filter");
  });

  it("says a flow changed only where it sits when no step changed", async () => {
    const host = await mountIn(ChangeList, { props: { changes: [flow({ steps: [] })] } });
    expect(host.querySelector('[data-testid="change-steps"]')).toBeNull();
    expect(text(host)).toContain("(layout only)");
  });

  it("shows a row without steps as before", async () => {
    const host = await mountIn(ChangeList, { props: { changes: [flow({})] } });
    expect(text(host)).toBe("flow “nightly_revenue” changed");
  });
});
