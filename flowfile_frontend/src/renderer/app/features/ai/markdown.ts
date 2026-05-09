// Shared markdown rendering helper for the AI surface.
//
// Both `AiMessage.vue` (regular chat) and `AiAgentEvent.vue` (planner
// thinking events) parse LLM-produced text as markdown. Rather than
// duplicating the marked + DOMPurify pipeline, route both through here.
//
// LLM output is untrusted — DOMPurify strips <script>, on* handlers, and
// javascript: URIs by default. `marked.parse` returns a string under the
// configuration below.
//
// `marked.setOptions` is module-singleton: calling it once at module load
// applies for every subsequent `marked.parse(...)` call across the app.
// AiMessage.vue used to call this itself; centralised here so the two
// callers can't drift on options.
//
// GFM = GitHub-flavoured markdown (tables, fenced code, task lists).
// breaks = treat single newlines as <br> so streamed paragraph chunks
// render naturally without the user remembering double-newline semantics.

import DOMPurify from "dompurify";
import { marked, type Tokens } from "marked";

marked.setOptions({
  gfm: true,
  breaks: true,
});

export const sanitiseMarkdown = (text: string): string => {
  if (!text) return "";
  const raw = marked.parse(text) as string;
  return DOMPurify.sanitize(raw);
};

// Constrained markdown renderer for surfaces where the LLM emits
// short prose (Cmd+K rationale; reusable for the chat / agent
// rationale surfaces).
//
// Differs from `sanitiseMarkdown`:
// - Allow-list is enforced at marked's renderer layer — `html` and
//   `image` tokens render as empty strings, so raw HTML ( `<script>`,
//   `<img>`, `<iframe>`, …) and markdown image syntax never make it
//   into the output even before DOMPurify runs.
// - Anchor tags get `target="_blank"` + `rel="noopener noreferrer"`
//   injected directly by marked's `link` renderer; URLs with unsafe
//   schemes (`javascript:`, `data:`, `vbscript:`) are rendered as plain
//   text rather than as anchors.
// - DOMPurify still runs as defense-in-depth where a window is
//   available (production). In Node without a DOM, DOMPurify's
//   `sanitize` is unavailable — `_safeDOMSanitize` no-ops, which is
//   safe because the marked-renderer pass already enforces the
//   allow-list.
//
// Returns a string of sanitised HTML safe to drop into `v-html`.

const SAFE_LINK_SCHEME = /^(?:https?:|mailto:|\.|\/|#)/i;

let _renderSafeMarkedConfigured = false;

const configureRenderSafeMarked = (): void => {
  if (_renderSafeMarkedConfigured) return;
  marked.use({
    renderer: {
      // Block any raw inline / block HTML embedded in the markdown
      // source. Without this, marked passes <script> / <iframe> / <img>
      // through verbatim.
      html(): string {
        return "";
      },
      // Markdown image syntax (`![alt](url)`) is not in the
      // allow-list — render as nothing so the LLM can't smuggle an
      // image (or an `onerror`-bearing one) via markdown.
      image(): string {
        return "";
      },
      link(token: Tokens.Link): string {
        // `this` is the marked Renderer instance at runtime; we reach
        // into its parser to resolve the inline children.
        const text = (
          this as unknown as { parser: { parseInline: (t: Tokens.Link["tokens"]) => string } }
        ).parser.parseInline(token.tokens);
        const href = (token.href ?? "").trim();
        if (!href || !SAFE_LINK_SCHEME.test(href)) {
          // Unsafe / empty scheme — render the link text only, no anchor.
          return text;
        }
        const escapedHref = href.replace(/"/g, "&quot;");
        return `<a href="${escapedHref}" target="_blank" rel="noopener noreferrer">${text}</a>`;
      },
    },
  });
  _renderSafeMarkedConfigured = true;
};

interface DOMPurifyRuntime {
  sanitize?: (input: string, config?: Record<string, unknown>) => string;
}

const ALLOWED_TAGS = ["p", "br", "strong", "em", "code", "pre", "ul", "ol", "li", "a", "hr"];

const ALLOWED_ATTR = ["href", "target", "rel"];

const _safeDOMSanitize = (raw: string): string => {
  // DOMPurify's runtime API is only fully populated when a window is
  // present (browser / electron renderer). In a pure Node test env the
  // module exposes only `version` / `removed` / `isSupported` — call
  // the marked-stripped output through unchanged.
  const purifier = DOMPurify as unknown as DOMPurifyRuntime;
  if (typeof purifier.sanitize !== "function") return raw;
  return purifier.sanitize(raw, {
    ALLOWED_TAGS,
    ALLOWED_ATTR,
    ALLOW_DATA_ATTR: false,
  });
};

export const renderSafeMarkdown = (text: string): string => {
  if (!text) return "";
  configureRenderSafeMarked();
  const raw = marked.parse(text) as string;
  return _safeDOMSanitize(raw);
};
