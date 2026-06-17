export const CATALOG_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

// Empty is treated as valid here; callers still enforce required via their own !trim() disable.
export function validateCatalogName(value: string, kind = "Name"): string | null {
  const v = value ?? "";
  if (!v.trim()) return null;
  if (/\s/.test(v)) return `${kind} cannot contain spaces`;
  if (!CATALOG_NAME_PATTERN.test(v)) {
    return `${kind} can only contain letters, numbers, hyphens and underscores`;
  }
  return null;
}

export function sanitizeCatalogName(value: string): string {
  return (value ?? "").replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "");
}
