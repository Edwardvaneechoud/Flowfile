// MIME types + payload shape for notebook DataFrame display outputs.
// MUST match kernel_runtime/flowfile_client.py (_GW_TABLE_MIME / _GW_EXPLORE_MIME).
import type { IMutField, IRow } from "@kanaries/graphic-walker/interfaces";

export const TABLE_MIME = "application/vnd.flowfile.table+json";
export const EXPLORE_MIME = "application/vnd.flowfile.gwalker+json";

export interface TablePayload {
  columns: string[];
  fields: IMutField[];
  data: IRow[];
  total_rows: number;
  loaded_rows: number;
  truncated: boolean;
  max_rows: number;
}

export function isTableMime(mime: string): boolean {
  return mime === TABLE_MIME || mime === EXPLORE_MIME;
}

export function parseTablePayload(data: string): TablePayload | null {
  try {
    return JSON.parse(data) as TablePayload;
  } catch {
    return null;
  }
}
