// Pure helpers for the Delta Sharing panel: names, the profile file and the reader snippets.
import type { DeltaShareProfile, DeltaShareEligibleTable } from "../../api/deltaSharing.api";

export const SHARE_NAME_PATTERN = /^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$/;

export function shareNameError(name: string): string | null {
  const trimmed = name.trim();
  if (!trimmed) return "Give the share a name.";
  if (!SHARE_NAME_PATTERN.test(trimmed)) {
    return "Use letters, digits, _ and - only (it becomes part of the recipient's URLs).";
  }
  return null;
}

/** A name as core's default for a share table would make it: letters, digits, _ and -. */
export function safeShareName(text: string): string {
  const cleaned = text
    .trim()
    .replace(/[^A-Za-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!cleaned) return "table";
  return cleaned.startsWith("-") ? `_${cleaned}` : cleaned;
}

/** The API base core is reached at from this window, which the profile's endpoint is built on. */
export function apiBase(coreBaseUrl: string): string {
  return coreBaseUrl.replace(/\/+$/, "");
}

/** The `.share` file's text, as Delta Sharing clients read it. */
export function profileFileText(profile: DeltaShareProfile): string {
  return JSON.stringify(profile.profile, null, 2) + "\n";
}

/** Python lines that read the first table of a share with the delta-sharing client. */
export function pandasSnippet(profile: DeltaShareProfile): string {
  const table = profile.table_urls[0] ?? "<share>.<schema>.<table>";
  return [
    "# pip install delta-sharing",
    "import delta_sharing",
    "",
    `profile = "${profile.file_name}"`,
    "print(delta_sharing.SharingClient(profile).list_all_tables())",
    `df = delta_sharing.load_as_pandas(f"{profile}#${table}")`,
  ].join("\n");
}

/** Whether an endpoint is reachable only from this machine, which recipients elsewhere cannot use. */
export function isLoopbackEndpoint(endpoint: string): boolean {
  try {
    const host = new URL(endpoint).hostname;
    return host === "localhost" || host === "127.0.0.1" || host === "[::1]" || host === "::1";
  } catch {
    return false;
  }
}

/** Eligible tables not yet in the share, shareable ones first, each group by name. */
export function tablesToOffer(
  eligible: DeltaShareEligibleTable[],
  sharedTableIds: number[],
): DeltaShareEligibleTable[] {
  const taken = new Set(sharedTableIds);
  return eligible
    .filter((t) => !taken.has(t.table_id))
    .sort((a, b) => {
      if (!a.refusal !== !b.refusal) return a.refusal ? 1 : -1;
      return a.catalog_name.localeCompare(b.catalog_name);
    });
}
