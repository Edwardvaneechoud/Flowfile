// Output handle naming shared between canvas, node settings, and data preview.
// Keep in sync with flowfile_core.flowfile.flow_node.multi_output on the backend.

export const DEFAULT_OUTPUT_HANDLE = "output-0";

export function outputHandle(index: number): string {
  return `output-${index}`;
}

// Compact per-handle label shown on the node body (A, B, C, …) when a node
// has more than one output.
export function outputLabel(index: number): string {
  return String.fromCharCode(65 + index);
}

// Letters for a node's handles on one side. Prefers the initial of each declared
// name, so a join reads L/R and a filter split reads P/F instead of a meaningless
// A/B. Falls back to positional letters for the whole side when any name is
// missing or two initials would collide — a duplicated letter is worse than an
// arbitrary one. Returns undefined per handle when there is nothing to tell apart.
export function handleLetters(count: number, names?: string[]): (string | undefined)[] {
  if (count < 2) return Array.from({ length: count }, () => undefined);
  const positional = Array.from({ length: count }, (_, i) => outputLabel(i));
  if (!names) return positional;
  const initials = Array.from({ length: count }, (_, i) =>
    (names[i] ?? "").trim().charAt(0).toUpperCase(),
  );
  if (initials.some((c) => !/^[A-Z0-9]$/.test(c))) return positional;
  if (new Set(initials).size !== initials.length) return positional;
  return initials;
}
