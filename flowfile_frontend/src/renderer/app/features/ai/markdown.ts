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
import { marked } from "marked";

marked.setOptions({
  gfm: true,
  breaks: true,
});

export const sanitiseMarkdown = (text: string): string => {
  if (!text) return "";
  const raw = marked.parse(text) as string;
  return DOMPurify.sanitize(raw);
};
