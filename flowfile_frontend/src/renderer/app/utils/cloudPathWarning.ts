import { CLOUD_URI_SCHEMES, schemeOf } from "./storagePath";

export type CloudPathRole = "reader" | "writer";

const EMPTY_PATH_WARNING: Record<CloudPathRole, string> = {
  reader:
    "No source path set, so this node cannot run. Enter an object-storage URI such as s3://bucket/folder/file.parquet.",
  writer:
    "No target path set, so this node cannot run. Enter an object-storage URI such as s3://bucket/folder/table.",
};

// A leading ${param} resolves at run time and may supply the scheme itself.
const LEADING_PARAMETER = /^\$\{[A-Za-z_][A-Za-z0-9_]*\}/;

// Case-sensitive like the backend's is_cloud_uri: S3:// fails at run time.
const hasCloudScheme = (value: string): boolean =>
  CLOUD_URI_SCHEMES.some((scheme) => value.startsWith(scheme));

/** Non-blocking hint for a cloud reader/writer path, or null when it looks like an object-storage URI. */
export function cloudPathWarning(
  path: string | null | undefined,
  role: CloudPathRole,
): string | null {
  const value = path ?? "";
  if (!value.trim()) return EMPTY_PATH_WARNING[role];
  if (hasCloudScheme(value) || LEADING_PARAMETER.test(value)) return null;
  const scheme = schemeOf(value);
  if (scheme) {
    return (
      `'${value}' starts with ${value.slice(0, scheme.length)}, but the scheme is case-sensitive. ` +
      `Use lower-case ${scheme} instead.`
    );
  }
  return `'${value}' is not a cloud storage URI. Use a path starting with s3://, az://, abfss:// or gs://.`;
}
