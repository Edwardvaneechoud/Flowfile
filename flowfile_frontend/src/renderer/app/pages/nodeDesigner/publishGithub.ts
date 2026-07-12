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
