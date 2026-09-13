import { formatCellValue } from "./cellFormat";

// WKT is plain text under a user-chosen name: detect by value, stricter than a real parser.

const KEYWORDS = [
  "POINT",
  "LINESTRING",
  "POLYGON",
  "MULTIPOINT",
  "MULTILINESTRING",
  "MULTIPOLYGON",
  "GEOMETRYCOLLECTION",
] as const;

// Optional SRID prefix, keyword, optional Z/M/ZM tag, then EMPTY or "(" (spacing varies).
const HEAD = new RegExp(
  `^\\s*(?:SRID=\\d{1,7}\\s*;\\s*)?(${KEYWORDS.join("|")})\\s*(ZM|Z|M)?\\s*(?:(EMPTY)\\s*$|\\()`,
  "i",
);

// Anything outside this alphabet is prose that merely opens like geometry.
const BODY_ALPHABET = /^[-+0-9.eE\s,()]*$/;

// Bare ints, ".5", both exponent spellings, and the inf/nan a bad CRS transform emits.
const NUMBER = "[-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?|[-+]?(?:inf|nan)";
const COORD_PAIR = new RegExp(`(?:${NUMBER})\\s+(?:${NUMBER})`, "i");

// Cap pathological values so one cell cannot stall the grid.
const MAX_LEN = 1 << 20;

export interface GeometryInfo {
  /** Canonical geometry keyword, title-cased for display (e.g. "Polygon"). */
  type: string;
  /** Dimension tag when present ("Z", "M", "ZM"). */
  dimension: string;
  /** Number of coordinate tuples, or 0 for an EMPTY geometry. */
  points: number;
  /** The sole coordinate, for a single-point geometry. */
  coordinate: [number, number] | null;
}

const titleCase = (keyword: string): string => {
  const upper = keyword.toUpperCase();
  const pretty: Record<string, string> = {
    POINT: "Point",
    LINESTRING: "LineString",
    POLYGON: "Polygon",
    MULTIPOINT: "MultiPoint",
    MULTILINESTRING: "MultiLineString",
    MULTIPOLYGON: "MultiPolygon",
    GEOMETRYCOLLECTION: "GeometryCollection",
  };
  return pretty[upper] ?? upper;
};

/** Parens must balance and close exactly at the end — this is what rejects trailing prose. */
const bodyIsBalanced = (body: string): boolean => {
  let depth = 0;
  for (let i = 0; i < body.length; i++) {
    const ch = body[i];
    if (ch === "(") depth++;
    else if (ch === ")") {
      depth--;
      if (depth < 0) return false;
      if (depth === 0 && body.slice(i + 1).trim() !== "") return false;
    }
  }
  return depth === 0;
};

/** Each comma-separated chunk carrying a digit is one coordinate tuple. */
const countPoints = (body: string): number =>
  body.split(",").filter((chunk) => /\d/.test(chunk)).length;

// A failed CRS transform emits "POINT (inf inf)"; fold to a digit to keep the alphabet strict.
const normalizeBody = (body: string): string => body.replace(/[-+]?\b(?:inf|nan)\b/gi, "0");

// A GEOMETRYCOLLECTION body holds child keywords; strip only those, prose still rejected.
const CHILD_KEYWORD = new RegExp(`\\b(?:${KEYWORDS.join("|")})\\s*(?:ZM|Z|M)?\\s*(?:EMPTY)?`, "gi");

const toNumber = (token: string): number => {
  const lowered = token.toLowerCase();
  if (lowered.endsWith("inf")) return lowered.startsWith("-") ? -Infinity : Infinity;
  if (lowered.endsWith("nan")) return NaN;
  return Number(token);
};

/** Strictly parse a WKT string, returning null for anything that is not geometry. */
export const parseWkt = (value: unknown): GeometryInfo | null => {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_LEN) return null;
  const head = HEAD.exec(value);
  if (!head) return null;

  const type = titleCase(head[1]);
  const dimension = (head[2] ?? "").toUpperCase();
  if (head[3]) return { type, dimension, points: 0, coordinate: null };

  const body = value.slice(head[0].length - 1);
  const isCollection = type === "GeometryCollection";
  const normalized = isCollection
    ? normalizeBody(body).replace(CHILD_KEYWORD, "")
    : normalizeBody(body);
  if (!BODY_ALPHABET.test(normalized) || !bodyIsBalanced(body) || !COORD_PAIR.test(normalized)) {
    return null;
  }

  const points = countPoints(normalized);
  let coordinate: [number, number] | null = null;
  if (points === 1) {
    const nums = body.match(new RegExp(NUMBER, "gi"));
    if (nums && nums.length >= 2) coordinate = [toNumber(nums[0]), toNumber(nums[1])];
  }
  return { type, dimension, points, coordinate };
};

export const isWktValue = (value: unknown): boolean => parseWkt(value) !== null;

/** Values that carry no evidence either way and must not decide a column. */
const isBlank = (value: unknown): boolean =>
  value === null ||
  value === undefined ||
  (typeof value === "string" && (value.trim() === "" || /^(n\/a|null|-)$/i.test(value.trim())));

/** Every non-blank value must parse, so a notes column with one WKT-looking row stays out. */
export const isGeometryColumn = (values: unknown[]): boolean => {
  let hits = 0;
  for (const value of values) {
    if (isBlank(value)) continue;
    if (!isWktValue(value)) return false;
    hits++;
  }
  return hits > 0;
};

/** True for a dtype polars surfaces from a GeoArrow extension field. */
export const isGeoArrowDataType = (dataType: string | undefined): boolean =>
  typeof dataType === "string" && /geoarrow/i.test(dataType);

interface GeometryColumnCandidate {
  name: string;
  data_type?: string;
  data_type_group?: string;
}

/** Sniff only string columns; a GeoArrow dtype is declared, so trust it without sniffing. */
export const detectGeometryColumns = (
  schema: GeometryColumnCandidate[] | null | undefined,
  rows: Record<string, unknown>[] | null | undefined,
): Set<string> => {
  const detected = new Set<string>();
  if (!schema?.length) return detected;
  for (const column of schema) {
    if (isGeoArrowDataType(column.data_type)) {
      detected.add(column.name);
      continue;
    }
    if (column.data_type_group !== "String") continue;
    if (!rows?.length) continue;
    if (isGeometryColumn(rows.map((row) => row[column.name]))) detected.add(column.name);
  }
  return detected;
};

const formatOrdinate = (n: number): string => {
  if (Number.isNaN(n)) return "nan";
  if (!Number.isFinite(n)) return n > 0 ? "inf" : "-inf";
  return String(Math.round(n * 1e4) / 1e4);
};

/** Compact cell summary — raw WKT is unreadable at 200px; full text stays in the tooltip. */
export const describeGeometry = (value: unknown): string | null => {
  const info = parseWkt(value);
  if (!info) return null;
  const tag = info.dimension ? ` ${info.dimension}` : "";
  if (info.points === 0) return `◆ ${info.type}${tag} (empty)`;
  if (info.coordinate) {
    const [x, y] = info.coordinate;
    return `◆ ${info.type}${tag} (${formatOrdinate(x)}, ${formatOrdinate(y)})`;
  }
  return `◆ ${info.type}${tag} · ${info.points.toLocaleString()} pts`;
};

/** AG Grid valueFormatter; display-only, so Cmd+C and the cell editor still see raw WKT. */
export const geometryCellFormatter = (params: { value: unknown }): string =>
  describeGeometry(params.value) ?? formatCellValue(params.value);

/** AG Grid `tooltipValueGetter` exposing the untruncated WKT behind the summary. */
export const geometryTooltipGetter = (params: { value: unknown }): string =>
  formatCellValue(params.value);
