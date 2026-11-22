import { describe, it, expect } from "vitest";
import { DOCS_BASE_URL, NEW_ISSUE_URL, bugReportUrl, docsUrl } from "./docsLinks";

describe("docsUrl", () => {
  it("keeps a trailing slash on the base so concatenation can't fuse segments", () => {
    expect(DOCS_BASE_URL.endsWith("/")).toBe(true);
  });

  it("builds an absolute URL from a site-relative path with a fragment", () => {
    expect(docsUrl("users/visual-editor/nodes/input.html#read-data")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/input.html#read-data",
    );
  });

  it("does not double-slash when the path has a leading slash", () => {
    expect(docsUrl("/users/visual-editor/nodes/input.html")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/input.html",
    );
  });

  it("returns the bare base with no argument", () => {
    expect(docsUrl()).toBe(DOCS_BASE_URL);
  });
});

describe("bugReportUrl", () => {
  const bodyOf = (url: string) => new URL(url).searchParams.get("body") ?? "";

  it("opens a new issue on the Flowfile repo", () => {
    const url = new URL(bugReportUrl());
    expect(`${url.origin}${url.pathname}`).toBe(NEW_ISSUE_URL);
  });

  it("prefills the version and environment", () => {
    const body = bodyOf(bugReportUrl({ version: "0.9.1", environment: "Desktop app (mac)" }));
    expect(body).toContain("- Flowfile version: 0.9.1");
    expect(body).toContain("- Environment: Desktop app (mac)");
    expect(body).not.toContain("- Browser:");
  });

  it("adds the user agent only when given and says unknown for missing fields", () => {
    const body = bodyOf(bugReportUrl({ userAgent: "Mozilla/5.0 Test" }));
    expect(body).toContain("- Flowfile version: unknown");
    expect(body).toContain("- Browser: Mozilla/5.0 Test");
  });
});
