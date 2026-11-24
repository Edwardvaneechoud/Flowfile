/** What one conflict row shows; the hub dialog, the project page and the designer's conflict bar share it. */
export interface MergeConflictView {
  label: string;
  message: string;
  ours_label: string;
  theirs_label: string;
  ours: string[];
  theirs: string[];
}

export type MergeSide = "ours" | "theirs";

export interface ConflictSide {
  side: MergeSide;
  name: string;
  lines: string[];
  action: string;
}

export const keepLabel = (name: string): string => `Keep ${name}`;

export interface ConflictSideOptions {
  /** Show the other side first: the hub merge names the person's own copy "theirs". */
  theirsFirst?: boolean;
  /** The button that takes a side; "Keep <side>" unless given. */
  actionLabel?: (side: MergeSide, name: string) => string;
}

/** Both sides of a conflict in the order shown, each with what it did and the button that takes it. */
export const conflictSides = (
  item: MergeConflictView,
  options: ConflictSideOptions = {},
): ConflictSide[] => {
  const label = options.actionLabel ?? ((_side: MergeSide, name: string) => keepLabel(name));
  const sides: ConflictSide[] = (["ours", "theirs"] as const).map((side) => {
    const name = side === "ours" ? item.ours_label : item.theirs_label;
    return { side, name, lines: item[side] ?? [], action: label(side, name) };
  });
  return options.theirsFirst ? sides.reverse() : sides;
};
