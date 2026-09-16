import { formatCellValue } from "./cellFormat";
import type { SemanticType } from "../types/node.types";

// A column is geometry only when core declares it (FileColumn.semantic_type, derived from a
// GeoArrow dtype). The WKT parser below is display-only: it summarises a cell, never labels a column.

export const isGeometryColumn = (column: { semantic_type?: SemanticType | null }): boolean =>
  column.semantic_type === "geometry";

// Mirrors core's _EXTENSION_DTYPE: Extension('<name>', <storage>, ...) → name + storage base token.
const EXTENSION_DTYPE = /^Extension\('([^']*)',\s*([A-Za-z0-9_]+)/;

export const extensionParts = (dataType?: string): { name: string; storage: string } | null => {
  const match = dataType ? EXTENSION_DTYPE.exec(dataType) : null;
  return match ? { name: match[1], storage: match[2] } : null;
};

/** Pill/badge text: an extension dtype shows the storage type it is castable as. */
export const displayDataType = (dataType: string): string =>
  extensionParts(dataType)?.storage ?? dataType;

/** The one phrase every geometry surface shows: header pill, stats panel, select node, formula list. */
export const geometryTitle = (dataType?: string): string => {
  const parts = extensionParts(dataType);
  if (parts) return `Geometry (${parts.name}), stored as ${parts.storage}`;
  return dataType ? `Geometry (GeoArrow), stored as ${dataType}` : "Geometry (GeoArrow)";
};

/** Material icon for a column-level geometry mark. */
export const GEOMETRY_ICON = "pentagon";

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
// The tag owns its trailing whitespace so two \s* never compete for one run (quadratic otherwise).
const HEAD = new RegExp(
  `^\\s*(?:SRID=\\d{1,7}\\s*;\\s*)?(${KEYWORDS.join("|")})\\s*(?:(ZM|Z|M)\\s*)?(?:(EMPTY)\\s*$|\\()`,
  "i",
);

// Anything outside this alphabet is prose that merely opens like geometry.
const BODY_ALPHABET = /^[-+0-9.eE\s,()]*$/;

// Bare ints, ".5", both exponent spellings, and the inf/nan a bad CRS transform emits.
const NUMBER = "[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?|[-+]?(?:inf|nan)";
const COORD_PAIR = new RegExp(`(?:${NUMBER})\\s+(?:${NUMBER})`, "i");
// A real geometry's first pair sits within a few dozen chars of "("; probing a prefix keeps the
// unanchored search bounded whatever the cell length.
const COORD_PROBE = 1024;

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
  if (
    !BODY_ALPHABET.test(normalized) ||
    !bodyIsBalanced(body) ||
    !COORD_PAIR.test(normalized.slice(0, COORD_PROBE))
  ) {
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

/** Material icon for one cell by shape; WKB and native encodings get the generic mark. */
export const geometryIcon = (value: unknown): string => {
  switch (parseWkt(value)?.type) {
    case "Point":
    case "MultiPoint":
      return "place";
    case "LineString":
    case "MultiLineString":
      return "polyline";
    case "GeometryCollection":
      return "layers";
    default:
      return GEOMETRY_ICON;
  }
};

const formatOrdinate = (n: number): string => {
  if (Number.isNaN(n)) return "nan";
  if (!Number.isFinite(n)) return n > 0 ? "inf" : "-inf";
  return String(Math.round(n * 1e4) / 1e4);
};

/** Compact WKT summary — raw text is unreadable at 200px; null for anything that is not WKT. */
export const describeGeometry = (value: unknown): string | null => {
  const info = parseWkt(value);
  if (!info) return null;
  const tag = info.dimension ? ` ${info.dimension}` : "";
  if (info.points === 0) return `${info.type}${tag} (empty)`;
  if (info.coordinate) {
    const [x, y] = info.coordinate;
    return `${info.type}${tag} (${formatOrdinate(x)}, ${formatOrdinate(y)})`;
  }
  return `${info.type}${tag} · ${info.points.toLocaleString()} pts`;
};

/** AG Grid valueFormatter; display-only, so Cmd+C and the cell editor still see the raw value. */
export const geometryCellFormatter = (params: { value: unknown }): string =>
  describeGeometry(params.value) ?? formatCellValue(params.value);
