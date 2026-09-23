const ANSI_ESCAPE = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g");
const MAX_HINT_LENGTH = 240;

/** delta-rs errors nest ANSI-coloured causes and raw S3 XML; keep the part a user can act on. */
function summarize(message: string): string {
  const text = message.replace(ANSI_ESCAPE, "");
  const s3Message = /<Message>([^<]+)<\/Message>/.exec(text)?.[1];
  if (s3Message) {
    const s3Code = /<Code>([^<]+)<\/Code>/.exec(text)?.[1];
    return s3Code ? `${s3Message} (${s3Code})` : s3Message;
  }
  const flat = text.replace(/\s+/g, " ").trim();
  return flat.length > MAX_HINT_LENGTH ? `${flat.slice(0, MAX_HINT_LENGTH - 1)}…` : flat;
}

/**
 * Non-blocking hint for a failed Delta table probe in the cloud storage drawers, or null to
 * stay quiet. A table that does not exist yet is not a problem (a writer creates it), so the
 * route's NOT_A_DELTA_TABLE stays quiet; everything else (no credentials, missing connection,
 * access denied, unreachable endpoint) is what the run would hit too.
 */
export function deltaProbeHint(error: unknown): string | null {
  const err = error as { message?: unknown; response?: { data?: { detail?: unknown } } };
  const detail = err?.response?.data?.detail as
    | { error_code?: unknown; message?: unknown }
    | string
    | undefined;
  if (typeof detail === "object" && detail?.error_code === "NOT_A_DELTA_TABLE") return null;
  let message: unknown = err?.message;
  if (typeof detail === "string") message = detail;
  else if (typeof detail?.message === "string") message = detail.message;
  const text = typeof message === "string" ? summarize(message) : "";
  return `Could not check the table at this path: ${text || "unknown error"}`;
}
