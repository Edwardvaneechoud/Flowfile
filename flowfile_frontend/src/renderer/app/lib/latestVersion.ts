// "Is there a newer Flowfile?" for browser-served installs (pip, Docker, the
// web dev server), which have no updater plugin. Same source as the desktop
// updater — the latest GitHub release — but read through the REST API: the
// updater's latest.json asset carries no CORS headers, the API allows any origin.

export const LATEST_RELEASE_URL =
  "https://api.github.com/repos/Edwardvaneechoud/Flowfile/releases/latest";

const numericParts = (version: string): number[] =>
  version
    .split(/[^0-9]+/)
    .filter(Boolean)
    .map(Number);

/** Plain numeric compare of the dotted parts: enough for the x.y.z tags Flowfile ships. */
export function isNewerVersion(candidate: string, current: string): boolean {
  const a = numericParts(candidate);
  const b = numericParts(current);
  if (!a.length || !b.length) return false;
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const diff = (a[i] ?? 0) - (b[i] ?? 0);
    if (diff !== 0) return diff > 0;
  }
  return false;
}

/** The latest release's version, without the `v` tag prefix. */
export async function fetchLatestVersion(): Promise<string> {
  const response = await fetch(LATEST_RELEASE_URL, {
    headers: { Accept: "application/vnd.github+json" },
  });
  if (!response.ok) throw new Error(`GitHub responded ${response.status}`);
  const tag = (await response.json())?.tag_name;
  if (typeof tag !== "string" || !tag) throw new Error("release has no tag_name");
  return tag.replace(/^v/, "");
}
