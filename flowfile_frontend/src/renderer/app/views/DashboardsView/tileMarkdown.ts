import DOMPurify from "dompurify";
import { Marked } from "marked";

// Its own instance: `lib/markdown.ts` reconfigures the global `marked` for the whole session.
const tileMarked = new Marked();

/**
 * A dashboard's Markdown as sanitised HTML: a text block's body and a tile's note both render
 * through here, so a note is never sanitised more weakly than a text block, and both read the
 * same whichever pages were opened before. Blank text is "".
 */
export const renderTileMarkdown = (md: string | null | undefined): string => {
  const text = md ?? "";
  if (!text.trim()) return "";
  const raw = tileMarked.parse(text, { async: false }) as string;
  return DOMPurify.sanitize(raw);
};
