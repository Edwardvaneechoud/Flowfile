import { ElMessageBox } from "element-plus";
import type { ImportResult } from "../../types";

/** What an open, restore or reload committed but could not finish, one line per step; empty when nothing. */
export const incompleteLines = (res: Pick<ImportResult, "incomplete">): string[] =>
  res.incomplete?.length
    ? [
        `The database was updated, but ${res.incomplete.length} step(s) did not finish:`,
        ...res.incomplete,
        "Reload the project from its folder to finish them.",
      ]
    : [];

/** Tell the user what the import left unfinished; resolves whether there was anything to tell. */
export const alertIncomplete = async (
  res: Pick<ImportResult, "incomplete">,
  title: string,
): Promise<boolean> => {
  const lines = incompleteLines(res);
  if (!lines.length) return false;
  await ElMessageBox.alert(lines.join("\n"), title, {
    type: "warning",
    customStyle: { whiteSpace: "pre-line" },
  }).catch(() => undefined);
  return true;
};
