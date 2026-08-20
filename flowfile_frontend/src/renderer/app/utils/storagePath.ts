// Path arithmetic that works for both local filesystem paths and object-storage URIs.
// path-browserify collapses "s3://bucket" to "s3:/bucket", so URIs need their own rules.

import path from "path-browserify";

export const CLOUD_URI_SCHEMES = [
  "s3://",
  "s3a://",
  "az://",
  "abfs://",
  "abfss://",
  "adl://",
  "gs://",
  "gcs://",
] as const;

export type CloudStorageKind = "s3" | "adls" | "gcs";

const SCHEME_TO_STORAGE_TYPE: Record<string, CloudStorageKind> = {
  "s3://": "s3",
  "s3a://": "s3",
  "az://": "adls",
  "abfs://": "adls",
  "abfss://": "adls",
  "adl://": "adls",
  "gs://": "gcs",
  "gcs://": "gcs",
};

/** The scheme Flowfile reads and writes for each storage type. */
export const CANONICAL_SCHEME: Record<CloudStorageKind, string> = {
  s3: "s3://",
  adls: "az://",
  gcs: "gs://",
};

export interface ParsedUri {
  scheme: string;
  container: string;
  key: string;
}

/** The `scheme://` prefix of a URI, or "" when it is a local path. */
export function schemeOf(value: string): string {
  const lowered = value.toLowerCase();
  return CLOUD_URI_SCHEMES.find((scheme) => lowered.startsWith(scheme)) ?? "";
}

export function isCloudUri(value: string): boolean {
  return schemeOf(value) !== "";
}

export function storageTypeForUri(value: string): CloudStorageKind | null {
  const scheme = schemeOf(value);
  return scheme ? SCHEME_TO_STORAGE_TYPE[scheme] : null;
}

export function parseUri(uri: string): ParsedUri {
  const scheme = schemeOf(uri);
  if (!scheme) throw new Error(`Not a cloud storage URI: ${uri}`);
  const remainder = uri.slice(scheme.length).replace(/^\/+|\/+$/g, "");
  if (!remainder) return { scheme, container: "", key: "" };
  const separator = remainder.indexOf("/");
  if (separator === -1) return { scheme, container: remainder, key: "" };
  return {
    scheme,
    container: remainder.slice(0, separator),
    key: remainder.slice(separator + 1).replace(/^\/+|\/+$/g, ""),
  };
}

export function buildUri(scheme: string, container = "", key = ""): string {
  if (!container) return scheme;
  const trimmed = key.replace(/^\/+|\/+$/g, "");
  return trimmed ? `${scheme}${container}/${trimmed}` : `${scheme}${container}`;
}

const isWindowsPath = (value: string): boolean =>
  /^[A-Za-z]:[\\/]/.test(value) || (value.includes("\\") && !value.includes("/"));

/** Last separator of a Windows path, ignoring any trailing one. */
const windowsCut = (value: string): [string, number] => {
  const trimmed = value.replace(/[\\/]+$/, "");
  return [trimmed, Math.max(trimmed.lastIndexOf("\\"), trimmed.lastIndexOf("/"))];
};

/** `C:\a\b` -> `C:\a`; a drive root or UNC host is its own parent. */
const windowsParent = (value: string): string => {
  const [trimmed, cut] = windowsCut(value);
  if (cut <= 1) return value;
  const parent = trimmed.slice(0, cut);
  return /^[A-Za-z]:$/.test(parent) ? `${parent}\\` : parent;
};

const windowsBasename = (value: string): string => {
  const [trimmed, cut] = windowsCut(value);
  return cut === -1 ? trimmed : trimmed.slice(cut + 1);
};

/** True at a filesystem root, a home shortcut, or an object-store's container list. */
export function isRootPath(value: string): boolean {
  if (!value) return true;
  if (isCloudUri(value)) return parseUri(value).container === "";
  if (value === "/" || value === "~") return true;
  return /^[A-Za-z]:[/\\]?$/.test(value);
}

/**
 * One level up. A bucket's parent is the container list (`s3://`), never `s3:/`,
 * and the root of any source is its own parent so callers can't walk off the top.
 */
export function parentPath(value: string): string {
  if (!value) return "";
  if (isCloudUri(value)) {
    const { scheme, container, key } = parseUri(value);
    if (!container || !key) return scheme;
    const cut = key.lastIndexOf("/");
    return buildUri(scheme, container, cut === -1 ? "" : key.slice(0, cut));
  }
  if (isRootPath(value)) return value;
  if (isWindowsPath(value)) return windowsParent(value);
  const parent = path.dirname(value);
  return parent === value || parent === "." ? value : parent;
}

export function joinPath(base: string, child: string): string {
  if (!base) return child;
  if (isCloudUri(base)) {
    const { scheme, container, key } = parseUri(base);
    const segment = child.replace(/^\/+|\/+$/g, "");
    if (!segment) return buildUri(scheme, container, key);
    if (!container) return buildUri(scheme, segment);
    return buildUri(scheme, container, key ? `${key}/${segment}` : segment);
  }
  if (isWindowsPath(base)) {
    return `${base.replace(/[\\/]+$/, "")}\\${child.replace(/^[\\/]+/, "")}`;
  }
  return path.join(base, child);
}

export function basename(value: string): string {
  if (isWindowsPath(value)) return windowsBasename(value);
  if (!isCloudUri(value)) return path.basename(value);
  const { scheme, container, key } = parseUri(value);
  if (key) return key.slice(key.lastIndexOf("/") + 1);
  return container || scheme;
}

export function dirname(value: string): string {
  if (isWindowsPath(value)) return windowsParent(value);
  return isCloudUri(value) ? parentPath(value) : path.dirname(value);
}

/** Normalize without ever collapsing a `scheme://` separator. */
export function normalizePath(value: string): string {
  // Comparison form only: Windows separators fold to "/" so prefix checks work.
  if (isWindowsPath(value)) return path.normalize(value.replace(/\\/g, "/"));
  if (!isCloudUri(value)) return path.normalize(value);
  const { scheme, container, key } = parseUri(value);
  if (!container) return scheme;
  const normalizedKey = key ? path.normalize(key).replace(/^\/+|\/+$/g, "") : "";
  return buildUri(scheme, container, normalizedKey === "." ? "" : normalizedKey);
}
