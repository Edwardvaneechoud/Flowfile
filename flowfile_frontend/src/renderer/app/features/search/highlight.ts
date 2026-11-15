export interface TextSegment {
  text: string;
  match: boolean;
}

/** Lower case and separators (`-`, `_`, `.`, whitespace) as one, keeping every offset of `text` in place. */
function foldForMatch(text: string): string {
  return Array.from(text, (ch) => {
    if (/[\s_.-]/.test(ch)) return " ";
    const lower = ch.toLowerCase();
    return lower.length === ch.length ? lower : ch;
  }).join("");
}

/** `text` split into runs that do and do not contain one of `tokens` (case-insensitive); overlaps merge. */
export function highlightSegments(text: string, tokens: readonly string[]): TextSegment[] {
  if (!text) return [];
  const lower = foldForMatch(text);
  const ranges: Array<[number, number]> = [];
  for (const token of tokens) {
    const needle = foldForMatch(token);
    if (!needle) continue;
    for (let at = lower.indexOf(needle); at >= 0; at = lower.indexOf(needle, at + 1)) {
      ranges.push([at, at + needle.length]);
    }
  }
  if (!ranges.length) return [{ text, match: false }];
  ranges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const merged: Array<[number, number]> = [];
  for (const [start, end] of ranges) {
    const last = merged[merged.length - 1];
    if (last && start <= last[1]) last[1] = Math.max(last[1], end);
    else merged.push([start, end]);
  }
  const segments: TextSegment[] = [];
  let cursor = 0;
  for (const [start, end] of merged) {
    if (start > cursor) segments.push({ text: text.slice(cursor, start), match: false });
    segments.push({ text: text.slice(start, end), match: true });
    cursor = end;
  }
  if (cursor < text.length) segments.push({ text: text.slice(cursor), match: false });
  return segments;
}
