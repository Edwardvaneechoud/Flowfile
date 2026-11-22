import { desktop } from "../../../lib/desktop";

const EXTERNAL_SCHEMES = new Set(["http:", "https:", "mailto:"]);

/** The URL a link in dashboard Markdown may open outside the app, or null: http, https and mailto only. */
export const externalLinkHref = (href: string | null | undefined): string | null => {
  const text = href?.trim();
  if (!text) return null;
  try {
    const url = new URL(text);
    return EXTERNAL_SCHEMES.has(url.protocol) ? url.href : null;
  } catch {
    return null;
  }
};

/**
 * Click handler for rendered dashboard Markdown (text blocks and notes): a link never navigates the
 * app itself; an http, https or mailto link opens outside it (the system browser on desktop, a new
 * tab on the web) and any other link does nothing.
 */
export const openTileLink = (event: MouseEvent): void => {
  const target = event.target instanceof Element ? event.target : null;
  const anchor = target?.closest("a");
  if (!anchor || !(event.currentTarget as Element | null)?.contains(anchor)) return;
  event.preventDefault();
  const url = externalLinkHref(anchor.getAttribute("href"));
  if (url) void desktop.openExternal(url);
};
