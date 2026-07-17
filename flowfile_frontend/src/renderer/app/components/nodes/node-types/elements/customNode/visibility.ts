import type { SectionComponent } from "./interface";

// A section with `visible_when` shows only while its referenced toggle equals the
// target value (default true). Fail-open: an unresolvable/undefined reference (typo,
// missing toggle, no dot) leaves the section visible so it can never be lost.
export function isSectionVisible(
  section: Pick<SectionComponent, "visible_when">,
  data: Record<string, Record<string, unknown>>,
): boolean {
  const vw = section.visible_when;
  if (!vw?.field) return true;
  const dot = vw.field.indexOf(".");
  if (dot < 0) return true;
  const actual = data[vw.field.slice(0, dot)]?.[vw.field.slice(dot + 1)];
  if (actual === undefined) return true;
  return actual === (vw.equals ?? true);
}
