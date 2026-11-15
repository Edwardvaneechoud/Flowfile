import { ElMessageBox } from "element-plus";
import type { ProjectUnsavedChanges } from "../../api/project.errors";
import type { ProjectVersionChange } from "../../types";

/** One `kind “label”` line per change, for a confirm box. */
export const changeLines = (changes: ProjectVersionChange[]): string =>
  changes.map((c) => `${c.kind} “${c.label}”`).join("\n");

/** Ask before a restore or reload that keeps a recovery point of the changes the core named; `note` is added
 * below them. The wording of the risk comes from the core's message. */
export const confirmRecoveryPoint = async (
  unsaved: ProjectUnsavedChanges,
  note?: string,
): Promise<boolean> => {
  const body = [unsaved.message, changeLines(unsaved.changes), note].filter(Boolean).join("\n\n");
  try {
    await ElMessageBox.confirm(body, "Unsaved changes", {
      confirmButtonText: "Keep a recovery point and continue",
      cancelButtonText: "Cancel",
      type: "warning",
      customStyle: { whiteSpace: "pre-line" },
    });
    return true;
  } catch {
    return false;
  }
};
