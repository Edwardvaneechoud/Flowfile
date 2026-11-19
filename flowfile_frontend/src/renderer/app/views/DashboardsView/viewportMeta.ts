/** What the dashboard viewer asks of a phone: lay the page out at the device's own width. */
export const DEVICE_WIDTH_VIEWPORT = "width=device-width, initial-scale=1";

/** A mobile browser's layout width when a page names none (Chrome and Safari use 980px). */
export const BROWSER_DEFAULT_VIEWPORT = "width=980";

/**
 * Sets the page's `<meta name="viewport">` to `content` and returns a function that puts back
 * what was there: the previous content, or no tag at all. Scoped to a page on purpose, since the
 * app as a whole keeps the browser's default layout width on a phone.
 *
 * Chromium applies a changed `content` at once but keeps the last one it applied when the tag or
 * its `content` is removed, so where there was none the default width is written first.
 */
export function setViewportMeta(
  content: string,
  doc: Document | undefined = typeof document !== "undefined" ? document : undefined,
): () => void {
  if (!doc?.head) return () => undefined;
  const existing = doc.head.querySelector<HTMLMetaElement>('meta[name="viewport"]');
  const previous = existing?.getAttribute("content") ?? null;
  const meta = existing ?? doc.createElement("meta");
  if (!existing) {
    meta.setAttribute("name", "viewport");
    doc.head.appendChild(meta);
  }
  meta.setAttribute("content", content);
  let restored = false;
  return () => {
    if (restored) return;
    restored = true;
    if (previous != null) {
      meta.setAttribute("content", previous);
      return;
    }
    meta.setAttribute("content", BROWSER_DEFAULT_VIEWPORT);
    if (existing) meta.removeAttribute("content");
    else meta.remove();
  };
}
