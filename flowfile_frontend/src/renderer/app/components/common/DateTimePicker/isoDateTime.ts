/**
 * Pure, framework-free ISO-8601 UTC <-> local-`Date` conversion helpers backing
 * `DateTimePicker.vue`. The model value contract is always a UTC instant ending in
 * `Z` (e.g. `"2026-07-31T12:34:56Z"`) or `null`; the picker itself edits in the
 * browser's local timezone.
 */

export function isoToLocalDate(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export function localDateToIso(d: Date | null | undefined): string | null {
  if (!d || Number.isNaN(d.getTime())) return null;
  // Seconds precision — an SCD2 "active at" instant never needs sub-second resolution.
  return d.toISOString().slice(0, 19) + "Z";
}

export function formatUtcHint(iso: string | null | undefined): string {
  const d = isoToLocalDate(iso);
  if (!d) return "";
  const pad = (n: number) => String(n).padStart(2, "0");
  const y = d.getUTCFullYear();
  const mo = pad(d.getUTCMonth() + 1);
  const day = pad(d.getUTCDate());
  const h = pad(d.getUTCHours());
  const mi = pad(d.getUTCMinutes());
  return `${y}-${mo}-${day} ${h}:${mi} UTC`;
}
