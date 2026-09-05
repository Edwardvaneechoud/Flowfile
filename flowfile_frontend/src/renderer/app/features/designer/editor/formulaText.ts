/**
 * Whether column `col` of a formula line sits inside a quoted string or after a
 * `//` comment marker — places where function/column autocomplete is noise.
 *
 * Mirrors `findCommentSpans` in FunctionEditor.vue (and the backend
 * `find_comment_spans`): quote state is line-local and a `//` inside quotes is
 * ordinary text.
 */
export function isInsideStringOrComment(lineText: string, col: number): boolean {
  let insideSingle = false;
  let insideDouble = false;
  const end = Math.min(col, lineText.length);
  for (let pos = 0; pos < end; pos++) {
    const ch = lineText[pos];
    if (ch === "'" && !insideDouble) insideSingle = !insideSingle;
    else if (ch === '"' && !insideSingle) insideDouble = !insideDouble;
    else if (ch === "/" && lineText[pos + 1] === "/" && !insideSingle && !insideDouble) return true;
  }
  return insideSingle || insideDouble;
}
