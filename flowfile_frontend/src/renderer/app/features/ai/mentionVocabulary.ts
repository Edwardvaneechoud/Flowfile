// W24 — `@`-mention vocabulary helpers for the chat composer.
//
// Mirrors the four mention kinds the W22 backend parser
// (`flowfile_core/flowfile_core/ai/context/mentions.py`) understands:
//   @flow            — bare; the whole graph
//   @selection       — bare; the canvas selection
//   @node:<ref>      — pin a node's settings + IO schema
//   @schema:<ref>    — pin a node's column list/types
//
// The backend is the source of truth for parsing once a message is
// sent; this file exists only to power the UI dropdown so the user
// can compose well-formed mentions without memorising the syntax. We
// keep the helpers pure (no Vue / VueFlow imports) so they're easy to
// reason about by reading.

export type MentionKind = "node" | "schema" | "flow" | "selection";

export interface MentionCandidate {
  kind: MentionKind;
  /** Undefined for bare kinds (`@flow`, `@selection`). */
  ref?: string;
  /** User-facing primary line. */
  label: string;
  /** Optional secondary line shown beneath the label. */
  hint?: string;
}

export interface ActiveTrigger {
  /** Resolved kind once `@kind:` has been committed; undefined while typing the kind. */
  kind?: MentionKind;
  /** Characters after the trigger anchor — either the kind chars or the post-`:` ref chars. */
  refPrefix: string;
  /** Half-open `[start, end)` slice of the source text the dropdown will replace on pick. */
  span: [number, number];
}

const REF_KINDS = new Set<MentionKind>(["node", "schema"]);
const BARE_KINDS = new Set<MentionKind>(["flow", "selection"]);

const KIND_LABELS: Record<MentionKind, string> = {
  flow: "@flow",
  selection: "@selection",
  node: "@node:…",
  schema: "@schema:…",
};

const KIND_HINTS: Record<MentionKind, string> = {
  flow: "Pin the entire flow graph",
  selection: "Pin the current canvas selection",
  node: "Pin a node by name (settings + schema)",
  schema: "Pin a node's columns + types only",
};

const KIND_NAMES: MentionKind[] = ["flow", "selection", "node", "schema"];

/**
 * Detect whether the caret is inside an `@…` token.
 *
 * The trigger anchor (`@`) only fires when preceded by start-of-string
 * or a non-word character — same posture as the W22 backend regex's
 * `(?<!\w)` lookbehind, so `email@node:foo` does NOT trigger autocomplete.
 *
 * Whitespace terminates the trigger unless the caret is sitting inside
 * an unclosed quoted ref (`@node:"my fi…`) so the user can still type
 * names with spaces. After a committed `@node:filter_3 hello`, the
 * trailing space rules out the dropdown.
 *
 * Returns `null` when the caret is not on a trigger. Otherwise returns
 * `{kind?, refPrefix, span}` where `span` is the half-open `[start, end)`
 * the dropdown will replace once the user picks a candidate.
 */
export function detectActiveTrigger(text: string, caret: number): ActiveTrigger | null {
  if (caret < 1 || caret > text.length) return null;

  // Find the rightmost `@` at-or-before `caret` whose predecessor is
  // start-of-string or a non-word character. We walk right-to-left
  // and accept any `@` that satisfies the boundary; the rest of the
  // function decides if the slice from there to caret is a still-active
  // trigger (the alternative — bailing on whitespace mid-walk — gets
  // confused inside quoted refs, where spaces are legitimate).
  let anchor = -1;
  for (let i = caret - 1; i >= 0; i -= 1) {
    if (text[i] !== "@") continue;
    if (i === 0 || !isWordChar(text[i - 1])) {
      anchor = i;
      break;
    }
  }
  if (anchor < 0) return null;

  const after = text.slice(anchor + 1, caret);

  // `after` is empty → user just typed `@`; show all kinds.
  if (after.length === 0) {
    return { kind: undefined, refPrefix: "", span: [anchor, caret] };
  }

  const colonIdx = after.indexOf(":");

  // No `:` yet → still typing the kind. Reject if any whitespace or
  // disallowed char appears (kinds are pure word chars).
  if (colonIdx < 0) {
    for (let i = 0; i < after.length; i += 1) {
      if (!isMentionChar(after[i])) return null;
    }
    return { kind: undefined, refPrefix: after, span: [anchor, caret] };
  }

  // `kind:` committed. Extract kind + post-colon raw ref.
  const kindRaw = after.slice(0, colonIdx).toLowerCase();
  const rawRef = after.slice(colonIdx + 1);

  if (!isKnownKind(kindRaw)) return null;
  // Bare kinds with a `:` payload are invalid (W22 skips them too).
  if (!REF_KINDS.has(kindRaw)) return null;

  // Validate the rawRef. Three legal shapes:
  //   1. unquoted: only \w / `-` / `.`. Whitespace ends the trigger.
  //   2. open quoted: leading `"` or `'`, no matching closer yet — any
  //      content allowed (including whitespace).
  //   3. fully quoted (already closed): not a live trigger any more
  //      from a UX POV; the user has committed a ref and likely moved on.
  const refValidation = validateRawRef(rawRef);
  if (refValidation === null) return null;

  return { kind: kindRaw, refPrefix: refValidation, span: [anchor, caret] };
}

/**
 * Validate the raw post-`:` chars and return the prefix the dropdown
 * should filter on. Returns `null` when the trigger has ended (e.g.
 * unquoted whitespace).
 */
function validateRawRef(rawRef: string): string | null {
  if (rawRef.length === 0) return "";

  const first = rawRef[0];
  if (first === '"' || first === "'") {
    // Quoted ref. Look for a matching closer.
    const closerIdx = rawRef.indexOf(first, 1);
    if (closerIdx === -1) {
      // Open quoted ref still being typed — accept everything inside.
      return rawRef.slice(1);
    }
    // Already closed. Anything past the closer means the trigger is
    // over; if the caret sits exactly at the closer, the user has
    // committed the ref and we don't want to keep popping the dropdown.
    if (closerIdx === rawRef.length - 1) return null;
    return null;
  }

  // Unquoted: each char must be a mention char (no whitespace).
  for (let i = 0; i < rawRef.length; i += 1) {
    if (!isMentionChar(rawRef[i])) return null;
  }
  return rawRef;
}

/**
 * Build the kind-picker candidates for a given query (the chars typed
 * after `@` so far). Empty query → all four kinds.
 */
export function buildKindCandidates(query: string): MentionCandidate[] {
  const q = query.toLowerCase();
  const matches = KIND_NAMES.filter((k) => q === "" || k.includes(q));
  return matches.map((k) => ({
    kind: k,
    ref: REF_KINDS.has(k) ? "" : undefined,
    label: KIND_LABELS[k],
    hint: KIND_HINTS[k],
  }));
}

/**
 * Build the ref-picker candidates for a `node:` or `schema:` trigger.
 *
 * Sort: case-insensitive name *prefix* match first, then *substring*,
 * then stringified-id prefix. Stable within each group so the keyboard
 * navigation order matches the visual order.
 */
export function buildRefCandidates(
  kind: "node" | "schema",
  query: string,
  nodes: ReadonlyArray<{ id: number | string; name?: string | null }>,
): MentionCandidate[] {
  const q = query.toLowerCase();
  const prefixHits: MentionCandidate[] = [];
  const substringHits: MentionCandidate[] = [];
  const idHits: MentionCandidate[] = [];

  for (const node of nodes) {
    const name = (node.name ?? "").trim();
    const idStr = String(node.id);
    const ref = name.length > 0 ? name : idStr;
    const candidate: MentionCandidate = {
      kind,
      ref,
      label: ref,
      hint: name.length > 0 ? `id ${idStr}` : `node ${idStr}`,
    };
    if (q === "") {
      prefixHits.push(candidate);
      continue;
    }
    const nameLower = name.toLowerCase();
    if (nameLower.startsWith(q)) {
      prefixHits.push(candidate);
    } else if (nameLower.includes(q)) {
      substringHits.push(candidate);
    } else if (idStr.startsWith(q)) {
      idHits.push(candidate);
    }
  }

  return [...prefixHits, ...substringHits, ...idHits];
}

/**
 * Render the literal text that should replace the trigger span when
 * the user picks `candidate`. Names containing whitespace are
 * quote-wrapped — matches W22's quoted-ref support.
 */
export function formatMentionInsert(candidate: MentionCandidate): string {
  if (BARE_KINDS.has(candidate.kind)) {
    return `@${candidate.kind}`;
  }
  const ref = candidate.ref ?? "";
  if (ref === "") {
    // The user picked the kind row but a ref hasn't been chosen yet.
    // We still commit the kind + colon so the next keystroke goes into
    // ref-prefix mode.
    return `@${candidate.kind}:`;
  }
  const needsQuotes = /\s/.test(ref) || ref.includes(":") || ref.includes(",");
  const refOut = needsQuotes ? `"${ref.replace(/"/g, '\\"')}"` : ref;
  return `@${candidate.kind}:${refOut}`;
}

const MENTION_CHAR_RE = /[\w\-.]/;
const WORD_CHAR_RE = /\w/;

function isMentionChar(ch: string): boolean {
  // Unquoted refs and kind chars: letters, digits, underscore,
  // hyphen, dot. Colon is handled positionally; whitespace and quotes
  // are handled explicitly by `detectActiveTrigger` / `validateRawRef`.
  return MENTION_CHAR_RE.test(ch);
}

function isWordChar(ch: string): boolean {
  return WORD_CHAR_RE.test(ch);
}

function isKnownKind(s: string): s is MentionKind {
  return s === "node" || s === "schema" || s === "flow" || s === "selection";
}
