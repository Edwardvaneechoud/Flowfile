/** Minimized state after a header toggle; when `onMinimize` resolves `false` the panel stays open. */
export async function nextMinimizedState(
  isMinimized: boolean,
  onMinimize?: (() => unknown) | null,
): Promise<boolean> {
  if (isMinimized || !onMinimize) return !isMinimized;
  return (await onMinimize()) !== false;
}
