import { ElMessage } from "element-plus";
import { desktop, isDesktop } from "../../lib/desktop";
import { formatCount } from "../features/designer/dataPreview/columnQuality";
import { formatCellValue } from "./cellFormat";
import { copyTextEverywhere } from "./clipboardUtils";

export const COPY_MAX_CELLS = 100_000;
export const DOWNLOAD_DEFAULT_ROWS = 10_000;

/** Rows in one clipboard copy: at most DOWNLOAD_DEFAULT_ROWS and COPY_MAX_CELLS cells. */
export const rowsForCellCap = (columnCount: number): number =>
  Math.min(DOWNLOAD_DEFAULT_ROWS, Math.floor(COPY_MAX_CELLS / Math.max(1, columnCount)));

// Excel-style quoting, matching what Excel and Google Sheets emit when copying.
const serializeCell = (value: unknown, separator: string): string => {
  const raw = formatCellValue(value);
  if (raw.includes(separator) || /[\t\n\r"]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
};

/** Header line plus one line per row, cells joined by `separator` (TSV for copy, CSV for files). */
export const buildDelimited = (
  columns: string[],
  rows: Record<string, unknown>[],
  separator: "\t" | ",",
): string => {
  if (!columns.length) return "";
  const line = (values: unknown[]) =>
    values.map((v) => serializeCell(v, separator)).join(separator);
  const dataLines = rows.map((row) => line(columns.map((c) => row[c])));
  return [line(columns), ...dataLines].join("\n");
};

/** Copy rows as TSV and toast the outcome; a `total` above the row count reads "N of M rows". */
export const copyRows = async (
  columns: string[],
  rows: Record<string, unknown>[],
  total = rows.length,
  format: (n: number) => string = formatCount,
): Promise<void> => {
  if (!(await copyTextEverywhere(buildDelimited(columns, rows, "\t")))) {
    ElMessage.error("Couldn't copy to the clipboard. Use Download CSV instead.");
    return;
  }
  const of = total > rows.length ? ` of ${format(total)}` : "";
  ElMessage.success(`Copied ${format(rows.length)}${of} rows`);
};

/**
 * Save CSV through the native Save dialog on desktop or a browser download on web, and toast
 * the outcome. A UTF-8 BOM is prepended so Excel reads non-ASCII.
 */
export const saveCsv = async (csv: Blob | string, fileName: string): Promise<void> => {
  const data = new Blob(["\uFEFF", csv], { type: "text/csv;charset=utf-8" });
  try {
    if (isDesktop) {
      const bytes = new Uint8Array(await data.arrayBuffer());
      if ((await desktop.saveFile(fileName, bytes)) === null) return;
    } else {
      const url = URL.createObjectURL(data);
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }
    ElMessage.success(`Saved ${fileName}`);
  } catch {
    ElMessage.error("Could not save the file.");
  }
};
