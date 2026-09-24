/**
 * Minimized state after a header toggle. Minimizing runs `onMinimize` first; when that
 * resolves `false` (e.g. the open node settings could not be saved) the panel stays open.
 */
export async function nextMinimizedState(
  isMinimized: boolean,
  onMinimize?: (() => unknown) | null,
): Promise<boolean> {
  if (isMinimized || !onMinimize) return !isMinimized;
  return (await onMinimize()) !== false;
}
