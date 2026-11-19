// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";

import "../../__fixtures__/domPurifyNodeName";
import { marked } from "marked";
import { renderTileMarkdown } from "./tileMarkdown";

describe("renderTileMarkdown", () => {
  it("renders headings, emphasis, lists and links", () => {
    const html = renderTileMarkdown(
      "# Revenue\n\n**Net** of refunds\n\n- one\n- two\n\n[Def](https://example.com)",
    );
    expect(html).toContain("<h1>Revenue</h1>");
    expect(html).toContain("<strong>Net</strong>");
    expect(html).toContain("<li>one</li>");
    expect(html).toContain('<a href="https://example.com">Def</a>');
  });

  it("is empty for blank or missing text", () => {
    expect(renderTileMarkdown("")).toBe("");
    expect(renderTileMarkdown("   \n ")).toBe("");
    expect(renderTileMarkdown(null)).toBe("");
    expect(renderTileMarkdown(undefined)).toBe("");
  });

  it("drops a script tag", () => {
    const html = renderTileMarkdown("Hello <script>alert(1)</script> world");
    expect(html).not.toContain("<script");
    expect(html).not.toContain("alert(1)");
    expect(html).toContain("Hello");
  });

  it("drops an onerror attribute but keeps the image", () => {
    const html = renderTileMarkdown('<img src="x.png" onerror="alert(1)">');
    expect(html).toContain("<img");
    expect(html).not.toContain("onerror");
    expect(html).not.toContain("alert(1)");
  });

  it("drops a javascript: link target", () => {
    const html = renderTileMarkdown("[click](javascript:alert(1))");
    expect(html).not.toContain("javascript:");
  });
});

describe("renderTileMarkdown and the global marked", () => {
  it("is not changed by options another module sets on the global instance", () => {
    const before = renderTileMarkdown("one\ntwo\n\n![bar](https://example.com/bar.png)");
    marked.setOptions({ breaks: true });
    marked.use({ renderer: { image: () => "", html: () => "" } });
    try {
      const after = renderTileMarkdown("one\ntwo\n\n![bar](https://example.com/bar.png)");
      expect(after).toBe(before);
      expect(after).not.toContain("<br>");
      expect(after).toContain('<img src="https://example.com/bar.png" alt="bar">');
      expect(marked.parse("one\ntwo", { async: false })).toContain("<br>");
    } finally {
      marked.setOptions(marked.getDefaults());
    }
  });
});
