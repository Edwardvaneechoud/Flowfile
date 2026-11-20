// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ openExternal: vi.fn(async () => undefined) }));
vi.mock("../../../lib/desktop", () => ({ desktop: { openExternal: mocks.openExternal } }));

import { externalLinkHref, openTileLink } from "./tileLinks";

describe("externalLinkHref", () => {
  it("opens http, https and mailto links", () => {
    expect(externalLinkHref("https://example.com/a?b=1")).toBe("https://example.com/a?b=1");
    expect(externalLinkHref("http://example.com")).toBe("http://example.com/");
    expect(externalLinkHref("mailto:data@example.com")).toBe("mailto:data@example.com");
    expect(externalLinkHref("  HTTPS://Example.com/x  ")).toBe("https://example.com/x");
  });

  it("ignores every other scheme", () => {
    expect(externalLinkHref("javascript:alert(1)")).toBeNull();
    expect(externalLinkHref("data:text/html,<b>x</b>")).toBeNull();
    expect(externalLinkHref("file:///etc/passwd")).toBeNull();
    expect(externalLinkHref("tauri://localhost/x")).toBeNull();
    expect(externalLinkHref("ftp://example.com")).toBeNull();
  });

  it("ignores relative links, in-page anchors and missing hrefs", () => {
    expect(externalLinkHref("#/main/designer")).toBeNull();
    expect(externalLinkHref("/catalog")).toBeNull();
    expect(externalLinkHref("docs.html")).toBeNull();
    expect(externalLinkHref("")).toBeNull();
    expect(externalLinkHref(null)).toBeNull();
    expect(externalLinkHref(undefined)).toBeNull();
  });
});

describe("openTileLink", () => {
  beforeEach(() => mocks.openExternal.mockClear());

  const clickIn = (html: string, selector: string) => {
    const root = document.createElement("div");
    root.innerHTML = html;
    root.addEventListener("click", openTileLink);
    document.body.appendChild(root);
    const event = new MouseEvent("click", { bubbles: true, cancelable: true });
    root.querySelector(selector)!.dispatchEvent(event);
    root.remove();
    return event;
  };

  it("opens an external link outside the app instead of following it", () => {
    const event = clickIn('<p><a href="https://example.com/x"><b>go</b></a></p>', "b");
    expect(event.defaultPrevented).toBe(true);
    expect(mocks.openExternal).toHaveBeenCalledWith("https://example.com/x");
  });

  it("follows no link of another scheme and opens nothing", () => {
    const event = clickIn('<a href="#/main/designer">here</a>', "a");
    expect(event.defaultPrevented).toBe(true);
    expect(mocks.openExternal).not.toHaveBeenCalled();
  });

  it("leaves a click outside a link alone", () => {
    const event = clickIn("<p>plain <em>text</em></p>", "em");
    expect(event.defaultPrevented).toBe(false);
    expect(mocks.openExternal).not.toHaveBeenCalled();
  });
});
