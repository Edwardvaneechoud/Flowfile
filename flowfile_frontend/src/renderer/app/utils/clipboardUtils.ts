import { desktop } from "../../lib/desktop";

/**
 * Writes text to the clipboard, with a fallback for insecure contexts.
 *
 * `navigator.clipboard` only exists in a secure context (https or localhost),
 * so over a plain-http LAN IP (e.g. http://192.168.x.x:8080) it's undefined and
 * throws. There we fall back to a hidden-textarea + execCommand("copy"), which
 * still works. Returns whether the copy succeeded.
 */
export const copyToClipboard = async (text: string): Promise<boolean> => {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // fall through to the legacy path
    }
  }
  try {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.top = "-9999px";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(textarea);
    return ok;
  } catch {
    return false;
  }
};

/**
 * Snapshots the current OS clipboard text into localStorage so paste handlers
 * can detect whether the user copied something externally after copying a node.
 *
 * Reads via desktop.readClipboardText() so the desktop shell uses the native
 * clipboard plugin — the WebKit async read API would pop macOS's "Paste" pill.
 */
export const snapshotClipboard = async (): Promise<void> => {
  try {
    const text = await desktop.readClipboardText();
    localStorage.setItem("clipboardAtNodeCopy", text);
  } catch {
    localStorage.setItem("clipboardAtNodeCopy", "");
  }
};

/**
 * Parses clipboard text to detect tabular data (TSV from Excel/Sheets, or multi-line single-column).
 * Returns a 2D string array (rows × cols) or null if the text is a single value.
 */
export const parseTabularText = (text: string): string[][] | null => {
  const trimmed = normalizeLineEndings(text);
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

export type CsvDelimiter = "tab" | "comma" | "auto";

const DELIMITER_MAP: Record<Exclude<CsvDelimiter, "auto">, string> = {
  tab: "\t",
  comma: ",",
};

/** Normalize Windows/Mac line endings to Unix-style \n and strip trailing newline. */
const normalizeLineEndings = (text: string): string => {
  const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  return normalized.endsWith("\n") ? normalized.slice(0, -1) : normalized;
};

/** Count occurrences of a character in a string. */
const countChar = (text: string, char: string): number =>
  (text.match(new RegExp(char === "\t" ? "\\t" : char, "g")) || []).length;

/** Pick the separator: use the explicit choice, or auto-detect by comparing tab vs comma frequency. */
const resolveSeparator = (text: string, delimiter: CsvDelimiter): string => {
  if (delimiter !== "auto") return DELIMITER_MAP[delimiter];
  return countChar(text, "\t") >= countChar(text, ",") ? "\t" : ",";
};

/**
 * Parses CSV/TSV text into a 2D string array.
 *
 * Supports:
 * - Explicit or auto-detected delimiter (tab vs comma)
 * - RFC 4180 double-quote escaping: `"hello ""world"""` → `hello "world"`
 * - Mixed line endings (CRLF, CR, LF)
 *
 * @example
 * parseCsvText("name,age\nAlice,30\nBob,25")
 * // → [["name","age"], ["Alice","30"], ["Bob","25"]]
 *
 * parseCsvText("name\tage\nAlice\t30", "tab")
 * // → [["name","age"], ["Alice","30"]]
 */
export const parseCsvText = (text: string, delimiter: CsvDelimiter = "auto"): string[][] => {
  const content = normalizeLineEndings(text);
  if (content.length === 0) return [];

  const sep = resolveSeparator(content, delimiter);
  const rows: string[][] = [];
  let field = "";
  let inQuotes = false;
  let row: string[] = [];

  for (let i = 0; i < content.length; i++) {
    const ch = content[i];

    if (inQuotes) {
      if (ch === '"') {
        // Escaped quote ("") → literal quote character
        if (i + 1 < content.length && content[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          // Closing quote
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    // Outside quotes
    if (ch === '"') {
      inQuotes = true;
    } else if (ch === sep) {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      field = "";
      rows.push(row);
      row = [];
    } else {
      field += ch;
    }
  }

  // Push the final field and row
  row.push(field);
  rows.push(row);

  return rows;
};
