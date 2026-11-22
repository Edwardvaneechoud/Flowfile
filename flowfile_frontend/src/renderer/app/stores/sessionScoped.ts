/** Per-user resets a store registers for itself, run by the auth store when the user changes. */
const resets = new Set<() => void>();

export function onSessionEnd(reset: () => void): () => void {
  resets.add(reset);
  return () => resets.delete(reset);
}

export function endSession(): void {
  for (const reset of resets) reset();
}
