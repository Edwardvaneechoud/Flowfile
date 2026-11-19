// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import { BROWSER_DEFAULT_VIEWPORT, DEVICE_WIDTH_VIEWPORT, setViewportMeta } from "./viewportMeta";

const viewportTags = () => document.head.querySelectorAll('meta[name="viewport"]');

describe("setViewportMeta", () => {
  afterEach(() => {
    viewportTags().forEach((el) => el.remove());
  });

  it("adds the tag where there was none and removes it again", () => {
    const restore = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
    expect(viewportTags()).toHaveLength(1);
    expect(viewportTags()[0].getAttribute("content")).toBe(DEVICE_WIDTH_VIEWPORT);
    restore();
    expect(viewportTags()).toHaveLength(0);
  });

  it("writes the browser's default width before removing the tag it added", () => {
    const restore = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
    const meta = viewportTags()[0] as HTMLMetaElement;
    const contents: (string | null)[] = [];
    const setAttribute = vi.spyOn(meta, "setAttribute");
    restore();
    setAttribute.mock.calls.forEach(([name, value]) => name === "content" && contents.push(value));
    expect(contents).toEqual([BROWSER_DEFAULT_VIEWPORT]);
    expect(meta.isConnected).toBe(false);
  });

  it("puts back the content a tag had", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", "viewport");
    meta.setAttribute("content", "width=1024");
    document.head.appendChild(meta);

    const restore = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
    expect(viewportTags()).toHaveLength(1);
    expect(meta.getAttribute("content")).toBe(DEVICE_WIDTH_VIEWPORT);
    restore();
    expect(viewportTags()).toHaveLength(1);
    expect(meta.getAttribute("content")).toBe("width=1024");
  });

  it("leaves a tag without content without content", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", "viewport");
    document.head.appendChild(meta);
    const restore = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
    restore();
    expect(meta.hasAttribute("content")).toBe(false);
  });

  it("restores once, so a second call cannot undo a later page's tag", () => {
    const restore = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
    restore();
    const later = setViewportMeta("width=800");
    restore();
    expect(viewportTags()).toHaveLength(1);
    expect(viewportTags()[0].getAttribute("content")).toBe("width=800");
    later();
  });

  it("does nothing without a document", () => {
    expect(() => setViewportMeta(DEVICE_WIDTH_VIEWPORT, undefined)()).not.toThrow();
  });
});
