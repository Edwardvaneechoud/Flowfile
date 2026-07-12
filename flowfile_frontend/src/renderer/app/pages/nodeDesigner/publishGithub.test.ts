import { describe, expect, it } from "vitest";

import {
  bumpSemver,
  canCreatePr,
  DEVICE_POLL_MIN_INTERVAL_SEC,
  describeGithubError,
  FORK_RETRY_DELAY_MS,
  FORK_RETRY_MAX,
  GITHUB_ERROR_MESSAGES,
  nextPollDelayMs,
  openPrNotice,
  PR_SUCCESS_MESSAGES,
  prSuccessMessage,
  README_TEMPLATE,
  semverGt,
  SLOW_DOWN_BUMP_SEC,
  withReadmeTemplate,
} from "./publishGithub";

// Every typed error_code the publish-pr / github routes can surface.
const ALL_CODES = [
  "GITHUB_NOT_CONFIGURED",
  "GITHUB_NOT_CONNECTED",
  "GITHUB_TOKEN_INVALID",
  "DEVICE_EXPIRED",
  "DEVICE_DENIED",
  "VERSION_NOT_INCREMENTED",
  "GITHUB_RATE_LIMITED",
  "GITHUB_API_ERROR",
  "NODE_NOT_FOUND",
];

describe("GITHUB_ERROR_MESSAGES", () => {
  it("covers every code with a non-empty message", () => {
    for (const code of ALL_CODES) {
      expect(GITHUB_ERROR_MESSAGES[code], code).toBeTruthy();
      expect(GITHUB_ERROR_MESSAGES[code].length, code).toBeGreaterThan(0);
    }
  });

  it("gives every code a distinct message", () => {
    const messages = ALL_CODES.map((c) => GITHUB_ERROR_MESSAGES[c]);
    expect(new Set(messages).size).toBe(ALL_CODES.length);
  });

  it("NOT_CONFIGURED copy points at the PAT / bundle fallbacks", () => {
    const msg = GITHUB_ERROR_MESSAGES.GITHUB_NOT_CONFIGURED.toLowerCase();
    expect(msg).toContain("personal access token");
    expect(msg).toContain("bundle");
  });

  it("VERSION_NOT_INCREMENTED copy mentions bumping the version", () => {
    expect(GITHUB_ERROR_MESSAGES.VERSION_NOT_INCREMENTED.toLowerCase()).toContain("version");
  });
});

describe("describeGithubError", () => {
  it("returns the mapped message for a known code", () => {
    expect(describeGithubError("DEVICE_EXPIRED")).toBe(GITHUB_ERROR_MESSAGES.DEVICE_EXPIRED);
  });

  it("uses the fallback for an unknown code", () => {
    expect(describeGithubError("MYSTERY", "server said no")).toBe("server said no");
  });

  it("returns a generic message when the code is unknown and no fallback given", () => {
    expect(describeGithubError("MYSTERY")).toBeTruthy();
    expect(describeGithubError("MYSTERY").length).toBeGreaterThan(0);
  });
});

describe("nextPollDelayMs", () => {
  it("polls at the given interval while pending", () => {
    expect(nextPollDelayMs(5, "pending")).toBe(5000);
    expect(nextPollDelayMs(7, "pending")).toBe(7000);
  });

  it("honors the server-bumped interval on slow_down", () => {
    // Backend returns the already-bumped interval (default 5 + 5 = 10).
    expect(nextPollDelayMs(10, "slow_down")).toBe(10000);
    // A larger server interval is honored as-is.
    expect(nextPollDelayMs(15, "slow_down")).toBe(15000);
  });

  it("floors slow_down at the protocol minimum back-off", () => {
    expect(nextPollDelayMs(5, "slow_down")).toBe(
      (DEVICE_POLL_MIN_INTERVAL_SEC + SLOW_DOWN_BUMP_SEC) * 1000,
    );
  });

  it("falls back to the minimum interval when none is given", () => {
    expect(nextPollDelayMs(0, "pending")).toBe(DEVICE_POLL_MIN_INTERVAL_SEC * 1000);
  });
});

describe("canCreatePr", () => {
  const ready = {
    hasErrors: false,
    checking: false,
    connected: true,
    confirmed: true,
    publishing: false,
  };

  it("allows PR creation only when connected, confirmed, and idle with no errors", () => {
    expect(canCreatePr(ready)).toBe(true);
  });

  it("blocks when any gate fails", () => {
    expect(canCreatePr({ ...ready, connected: false })).toBe(false);
    expect(canCreatePr({ ...ready, confirmed: false })).toBe(false);
    expect(canCreatePr({ ...ready, hasErrors: true })).toBe(false);
    expect(canCreatePr({ ...ready, checking: true })).toBe(false);
    expect(canCreatePr({ ...ready, publishing: true })).toBe(false);
  });
});

describe("fork-retry budget", () => {
  it("uses sane retry constants", () => {
    expect(FORK_RETRY_DELAY_MS).toBeGreaterThanOrEqual(1000);
    expect(FORK_RETRY_MAX).toBeGreaterThan(1);
    // The bounded retry window must comfortably exceed a typical fork spin-up.
    expect(FORK_RETRY_DELAY_MS * FORK_RETRY_MAX).toBeGreaterThanOrEqual(30000);
  });
});

describe("prSuccessMessage", () => {
  it("maps each status to distinct copy", () => {
    const statuses = ["created", "updated", "already_open"];
    const messages = statuses.map((s) => prSuccessMessage(s));
    expect(new Set(messages).size).toBe(statuses.length);
    for (const msg of messages) expect(msg.length).toBeGreaterThan(0);
  });

  it("says updated when the open PR was refreshed", () => {
    expect(prSuccessMessage("updated").toLowerCase()).toContain("updated");
  });

  it("falls back to the created copy for unknown statuses", () => {
    expect(prSuccessMessage("mystery")).toBe(PR_SUCCESS_MESSAGES.created);
  });
});

describe("semverGt", () => {
  it("compares numerically per segment", () => {
    expect(semverGt("1.1.0", "1.0.9")).toBe(true);
    expect(semverGt("2.0.0", "1.9.9")).toBe(true);
    expect(semverGt("1.0.10", "1.0.9")).toBe(true);
    expect(semverGt("1.0.0", "1.0.0")).toBe(false);
    expect(semverGt("1.0.0", "1.0.1")).toBe(false);
  });

  it("returns false for non-semver input", () => {
    expect(semverGt("1.0", "0.9.0")).toBe(false);
    expect(semverGt("abc", "1.0.0")).toBe(false);
  });
});

describe("withReadmeTemplate", () => {
  it("template carries the standard sections", () => {
    for (const heading of ["## What it does", "## Inputs", "## Settings"]) {
      expect(README_TEMPLATE).toContain(heading);
    }
  });

  it("fills an empty README with the template", () => {
    expect(withReadmeTemplate("")).toBe(README_TEMPLATE);
    expect(withReadmeTemplate("   \n")).toBe(README_TEMPLATE);
  });

  it("appends the template after existing content", () => {
    const result = withReadmeTemplate("# My node\n");
    expect(result.startsWith("# My node")).toBe(true);
    expect(result).toContain("## What it does");
  });
});

describe("openPrNotice", () => {
  it("returns null with no open PRs", () => {
    expect(openPrNotice([], "1.0.0")).toBeNull();
  });

  it("flags an open PR for the current version as updatable in place", () => {
    const notice = openPrNotice([{ number: 6, version: "1.0.0" }], "1.0.0");
    expect(notice?.matchesCurrent).toBe(true);
    expect(notice?.text).toContain("#6");
    expect(notice?.text.toLowerCase()).toContain("updates it in place");
  });

  it("warns about a stale open PR for another version", () => {
    const notice = openPrNotice([{ number: 6, version: "1.0.0" }], "1.1.0");
    expect(notice?.matchesCurrent).toBe(false);
    expect(notice?.text).toContain("v1.0.0");
    expect(notice?.text).toContain("v1.1.0");
    expect(notice?.text.toLowerCase()).toContain("close the old");
  });

  it("prefers the current-version PR when several are open", () => {
    const notice = openPrNotice(
      [
        { number: 4, version: "0.9.0" },
        { number: 6, version: "1.0.0" },
      ],
      "1.0.0",
    );
    expect(notice?.matchesCurrent).toBe(true);
    expect(notice?.text).toContain("#6");
  });
});

describe("bumpSemver", () => {
  it("bumps each level and resets the lower segments", () => {
    expect(bumpSemver("1.2.3", "patch")).toBe("1.2.4");
    expect(bumpSemver("1.2.3", "minor")).toBe("1.3.0");
    expect(bumpSemver("1.2.3", "major")).toBe("2.0.0");
  });

  it("returns null for non-semver input", () => {
    expect(bumpSemver("1.2", "patch")).toBeNull();
    expect(bumpSemver("", "major")).toBeNull();
  });
});
