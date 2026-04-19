// Output handle naming shared between canvas, node settings, and data preview.
// Keep in sync with flowfile_core.flowfile.flow_node.multi_output on the backend.

export const DEFAULT_OUTPUT_HANDLE = "output-0";

export function outputHandle(index: number): string {
  return `output-${index}`;
}

export function outputHandleIndex(handle: string): number {
  const prefix = "output-";
  if (!handle.startsWith(prefix)) {
    throw new Error(`Invalid output handle: ${handle}`);
  }
  return Number(handle.slice(prefix.length));
}
