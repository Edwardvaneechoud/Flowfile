// Pure, framework-free helpers for the in-app "Publish on GitHub" flow: typed
// error-code copy, device-poll cadence, fork-retry budget, and the Create-PR
// gate. No Vue/axios imports so it unit-tests as a plain module.

export const GITHUB_ERROR_MESSAGES: Record<string, string> = {
  GITHUB_NOT_CONFIGURED:
    "One-click publishing isn't configured in this build yet — paste a personal access token below or download the bundle instead.",
  GITHUB_NOT_CONNECTED: "Connect a GitHub account before publishing.",
  GITHUB_TOKEN_INVALID:
    "That GitHub token was rejected. Reconnect your account, or paste a valid personal access token.",
  DEVICE_EXPIRED:
    "The device code expired before it was authorized. Start over to get a fresh code.",
  DEVICE_DENIED: "Authorization was denied on GitHub. Start over to try again.",
  VERSION_NOT_INCREMENTED:
    "This version is already published. Bump the version in the Publishing panel, then publish again.",
  GITHUB_RATE_LIMITED: "GitHub's rate limit was reached. Wait a few minutes, then try again.",
  GITHUB_API_ERROR: "GitHub returned an unexpected error. Try again in a moment.",
  NODE_NOT_FOUND: "This node could not be found on disk. Save it, then try again.",
};

export function describeGithubError(code: string, fallback = ""): string {
  return GITHUB_ERROR_MESSAGES[code] ?? (fallback || "Something went wrong talking to GitHub.");
}

export const DEVICE_POLL_MIN_INTERVAL_SEC = 5;
export const SLOW_DOWN_BUMP_SEC = 5;

// Next device-flow poll delay. The backend already returns the bumped interval on
// slow_down; honor it, but floor slow_down at the protocol's minimum back-off.
export function nextPollDelayMs(currentIntervalSec: number, status: string): number {
  const interval = currentIntervalSec > 0 ? currentIntervalSec : DEVICE_POLL_MIN_INTERVAL_SEC;
  if (status === "slow_down") {
    return Math.max(interval, DEVICE_POLL_MIN_INTERVAL_SEC + SLOW_DOWN_BUMP_SEC) * 1000;
  }
  return interval * 1000;
}

export const FORK_RETRY_DELAY_MS = 3000;
export const FORK_RETRY_MAX = 20;

export interface CanCreatePrState {
  hasErrors: boolean;
  checking: boolean;
  connected: boolean;
  confirmed: boolean;
  publishing: boolean;
}

export function canCreatePr(state: CanCreatePrState): boolean {
  return (
    state.connected && state.confirmed && !state.hasErrors && !state.checking && !state.publishing
  );
}

export const PR_SUCCESS_MESSAGES: Record<string, string> = {
  created: "Pull request created!",
  updated: "Pull request updated with your latest files.",
  // Old-core skew: pre-"updated" backends still report already_open.
  already_open: "A pull request for this version is already open.",
};

export function prSuccessMessage(status: string): string {
  return PR_SUCCESS_MESSAGES[status] ?? PR_SUCCESS_MESSAGES.created;
}

// Strict semver only (the Publishing panel enforces X.Y.Z); non-semver input → null.
function parseSemver(version: string): [number, number, number] | null {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version.trim());
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

export function semverGt(a: string, b: string): boolean {
  const pa = parseSemver(a);
  const pb = parseSemver(b);
  if (!pa || !pb) return false;
  for (let i = 0; i < 3; i++) {
    if (pa[i] !== pb[i]) return pa[i] > pb[i];
  }
  return false;
}

export type BumpLevel = "patch" | "minor" | "major";

export function bumpSemver(version: string, level: BumpLevel): string | null {
  const parsed = parseSemver(version);
  if (!parsed) return null;
  const [major, minor, patch] = parsed;
  if (level === "major") return `${major + 1}.0.0`;
  if (level === "minor") return `${major}.${minor + 1}.0`;
  return `${major}.${minor}.${patch + 1}`;
}
