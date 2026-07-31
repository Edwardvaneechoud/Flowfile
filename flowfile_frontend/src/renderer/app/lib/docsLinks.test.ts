import { describe, it, expect } from "vitest";
import { DOCS_BASE_URL, docsUrl } from "./docsLinks";

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
