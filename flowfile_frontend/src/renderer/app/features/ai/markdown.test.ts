// W66 — unit tests for the constrained markdown helper.
//
// `renderSafeMarkdown` is the renderer behind the W35 diff preview's
// rationale paragraph. The model emits markdown (bold, code, lists,
// links, horizontal rules); the helper turns that into sanitised HTML
// constrained to a small allow-list. Tests here cover both the happy
// formatting path (AC #6 / #8) and the script-stripping safety guarantee
// (AC #7).
//
// AiDiffPreview.vue's "renderer" surface is just `v-html` over this
// helper's output — the meaningful logic lives here. Same posture as
// W61's `aiDiffTypes.test.ts` extracts (vitest is node-only, no jsdom,
// so component-level rendering tests would need a different runner).
//
// dompurify reaches for `window.trustedTypes` in node and falls back
// gracefully — no jsdom polyfill needed for these assertions.

import { describe, expect, it } from "vitest";

import { renderSafeMarkdown } from "./markdown";

describe("renderSafeMarkdown — formatting (AC #6, #8)", () => {
  it("renders bold, inline code, and links", () => {
    const html = renderSafeMarkdown("**bold** and `code` and [link](https://example.com)");
    expect(html).toContain("<strong>bold</strong>");
    expect(html).toContain("<code>code</code>");
    expect(html).toContain('href="https://example.com"');
    expect(html).toContain('target="_blank"');
    expect(html).toContain('rel="noopener noreferrer"');
    // No literal markdown markers leak through.
    expect(html).not.toMatch(/\*\*bold\*\*/);
    expect(html).not.toMatch(/`code`/);
    expect(html).not.toMatch(/\[link\]/);
  });

  it("renders ordered lists", () => {
    const html = renderSafeMarkdown("1. one\n2. two");
    expect(html).toContain("<ol>");
    expect(html).toContain("<li>one</li>");
    expect(html).toContain("<li>two</li>");
  });

  it("renders bullet lists", () => {
    const html = renderSafeMarkdown("- alpha\n- beta");
    expect(html).toContain("<ul>");
    expect(html).toContain("<li>alpha</li>");
    expect(html).toContain("<li>beta</li>");
  });

  it("renders horizontal rules + paragraph breaks", () => {
    const html = renderSafeMarkdown("1. one\n2. two\n\n---\n\nfooter");
    expect(html).toContain("<ol>");
    expect(html).toContain("<hr>");
    expect(html).toContain("footer");
  });

  it("renders italic emphasis", () => {
    const html = renderSafeMarkdown("This *matters* a lot.");
    expect(html).toContain("<em>matters</em>");
  });

  it("renders fenced code blocks", () => {
    const html = renderSafeMarkdown("```\nfoo()\n```");
    expect(html).toContain("<pre>");
    expect(html).toContain("<code>");
    expect(html).toContain("foo()");
  });

  it("returns empty string for empty / null-ish input", () => {
    expect(renderSafeMarkdown("")).toBe("");
    expect(renderSafeMarkdown(undefined as unknown as string)).toBe("");
    expect(renderSafeMarkdown(null as unknown as string)).toBe("");
  });
});

describe("renderSafeMarkdown — safety (AC #7)", () => {
  it("strips <script> tags wholesale", () => {
    const html = renderSafeMarkdown("<script>alert(1)</script><img src=x onerror=alert(1)>");
    expect(html).not.toContain("<script>");
    expect(html).not.toContain("alert(1)");
    expect(html).not.toContain("onerror");
    // <img> isn't on the allow-list either; should be removed.
    expect(html).not.toContain("<img");
  });

  it("strips disallowed HTML tags so they cannot execute", () => {
    // marked's `html` renderer returns empty string for any raw HTML
    // token, so disallowed tags never make it into the output. In
    // production DOMPurify additionally preserves text content for
    // disallowed elements; the safety guarantee verified here is that
    // the tag itself never survives.
    const html = renderSafeMarkdown('<iframe src="evil"></iframe>');
    expect(html).not.toContain("<iframe");
    expect(html).not.toContain("evil");
  });

  it("does not allow javascript: URIs to survive", () => {
    const html = renderSafeMarkdown("[click](javascript:alert(1))");
    // dompurify drops the unsafe href; the surviving anchor (if any)
    // must not carry a javascript-scheme target.
    expect(html.toLowerCase()).not.toContain("javascript:");
  });

  it("does not produce raw HTML images even via markdown image syntax", () => {
    const html = renderSafeMarkdown("![alt](https://example.com/x.png)");
    expect(html).not.toContain("<img");
  });
});
