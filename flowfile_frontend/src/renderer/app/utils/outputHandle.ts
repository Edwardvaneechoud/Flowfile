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
