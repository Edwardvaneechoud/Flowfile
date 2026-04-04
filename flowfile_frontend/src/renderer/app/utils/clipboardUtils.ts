/**
 * Parses clipboard text to detect tabular data (TSV from Excel/Sheets, or multi-line single-column).
 * Returns a 2D string array (rows × cols) or null if the text is a single value.
 */
export const parseTabularText = (text: string): string[][] | null => {
  const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const trimmed = normalized.endsWith("\n") ? normalized.slice(0, -1) : normalized;
  if (trimmed.length === 0) return null;

  const lines = trimmed.split("\n");
  const hasTabs = lines.some((line) => line.includes("\t"));
  const isMultiLine = lines.length > 1;

  if (!hasTabs && !isMultiLine) return null;

  if (hasTabs) {
    return lines.map((line) => line.split("\t"));
  }
  // Multi-line, single column
  return lines.map((line) => [line]);
};

/**
 * Infers a data type from an array of string values (as found in clipboard data).
 * Returns one of: "Boolean", "Int64", "Float64", or "String".
 */
export const inferColumnDataType = (values: unknown[]): string => {
  const valid = values.filter((v) => v !== null && v !== undefined && v !== "");
  if (valid.length === 0) return "String";

  if (valid.every((v) => v === "true" || v === "false")) return "Boolean";

  if (
    valid.every((v) => typeof v === "string" && !isNaN(Number(v)) && (v as string).trim() !== "")
  ) {
    return valid.every((v) => Number.isInteger(Number(v))) ? "Int64" : "Float64";
  }

  return "String";
};

/**
 * Parses CSV/TSV text with a specified or auto-detected delimiter.
 * Handles basic double-quote escaping (RFC 4180 style).
 */
export const parseCsvText = (
  text: string,
  delimiter: "tab" | "comma" | "auto" = "auto",
): string[][] => {
  const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const trimmed = normalized.endsWith("\n") ? normalized.slice(0, -1) : normalized;
  if (trimmed.length === 0) return [];

  let sep: string;
  if (delimiter === "tab") {
    sep = "\t";
  } else if (delimiter === "comma") {
    sep = ",";
  } else {
    const tabCount = (trimmed.match(/\t/g) || []).length;
    const commaCount = (trimmed.match(/,/g) || []).length;
    sep = tabCount >= commaCount ? "\t" : ",";
  }

  const rows: string[][] = [];
  let current = "";
  let inQuotes = false;
  let row: string[] = [];

  for (let i = 0; i < trimmed.length; i++) {
    const ch = trimmed[i];

    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < trimmed.length && trimmed[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === sep) {
      row.push(current);
      current = "";
    } else if (ch === "\n") {
      row.push(current);
      current = "";
      rows.push(row);
      row = [];
    } else {
      current += ch;
    }
  }

  row.push(current);
  rows.push(row);

  return rows;
};
