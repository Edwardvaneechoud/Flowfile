// The note a remote catalog sends about a flow it keeps, shown as a passing message when the flow opens.

const shown = new Set<number>();

/** The catalog's note to show as `settings`' flow opens: once per flow this session, null when there is none or it
 * was shown (a save shows its own and counts as shown). */
export function noteAtOpen(settings: { flow_id: number; save_note?: string | null }): string | null {
  if (!settings.save_note || shown.has(settings.flow_id)) return null;
  shown.add(settings.flow_id);
  return settings.save_note;
}

/** Forgets what was shown, for tests. */
export function resetNotesShown(): void {
  shown.clear();
}
