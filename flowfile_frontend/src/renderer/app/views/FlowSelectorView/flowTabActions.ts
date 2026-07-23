// Pure tab-menu logic for the flow tab strip — no Vue/axios imports so it stays unit-testable.
import type { ContextMenuOption } from "../../components/common/ContextMenu/types";

export type TabCloseAction = "close" | "close-others" | "close-all" | "close-left" | "close-right";

/** Flow ids to close for a tab action, in tab order. Unknown target → no-op. */
export function computeCloseTargets(
  flowIds: number[],
  targetId: number,
  action: TabCloseAction,
): number[] {
  const index = flowIds.indexOf(targetId);
  if (index === -1) return [];
  switch (action) {
    case "close":
      return [targetId];
    case "close-others":
      return flowIds.filter((id) => id !== targetId);
    case "close-all":
      return [...flowIds];
    case "close-left":
      return flowIds.slice(0, index);
    case "close-right":
      return flowIds.slice(index + 1);
  }
}

export function tabMenuOptions(flowIds: number[], targetId: number): ContextMenuOption[] {
  const index = flowIds.indexOf(targetId);
  const count = flowIds.length;
  return [
    { label: "Rename", action: "rename" },
    { label: "Close", action: "close" },
    { label: "Close Others", action: "close-others", disabled: count <= 1 },
    { label: "Close All", action: "close-all" },
    { label: "Close Tabs to the Left", action: "close-left", disabled: index <= 0 },
    { label: "Close Tabs to the Right", action: "close-right", disabled: index >= count - 1 },
  ];
}
