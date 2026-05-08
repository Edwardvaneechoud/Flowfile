<script setup lang="ts">
// Single-event renderer for the W40 planner agent event stream.
//
// Most event kinds are mechanical summaries — "Picking category…",
// "Staged filter (node 2)", "Rejected X: <detail>" — and render as plain
// text. The exception is `thinking`, which carries the LLM's free-form
// natural-language prose (markdown). Render that through the same
// sanitised marked + DOMPurify pipeline that AiMessage.vue uses for
// regular assistant chat.
//
// W38 — for ``tool_call_*`` events the renderer prefers the model's
// ``rationale`` (the assistant's plain-English preamble) as the primary
// headline, with the raw tool name + arg JSON tucked into a collapsible
// ``<details>`` block. When the model didn't emit a preamble we fall back
// to the server-generated ``arg_summary``. Events whose ``op_kind`` is
// ``"meta"`` (D002 internal routing — pick_category and "category
// narrowed" info notes) are hidden entirely so the user-visible chat
// trail only shows real graph mutations and schema reads.

import { computed, ref } from "vue";

import type { AgentEvent } from "../../stores/ai-agent-store";
import { isAgentEventHidden } from "./argSummary";
import { sanitiseMarkdown } from "./markdown";

const props = defineProps<{ event: AgentEvent }>();

// W40 — agent event payload accessors. Templates can't use TS type
// assertions, so unwrap with helpers that return safe defaults.
const _str = (payload: Record<string, unknown>, key: string): string => {
  const v = payload?.[key];
  return typeof v === "string" ? v : "";
};
const _num = (payload: Record<string, unknown>, key: string): number | null => {
  const v = payload?.[key];
  return typeof v === "number" ? v : null;
};

// Strip the dotted MCP prefix so summaries read "filter" not
// "flowfile.graph.add_filter", and "pick_upstream" not
// "flowfile.meta.pick_upstream" (W71 v1.3 — meta-op rejections are now
// surfaced in the chat trail).
const _shortToolName = (name: string): string => {
  if (name.startsWith("flowfile.graph.add_")) return name.slice("flowfile.graph.add_".length);
  if (name.startsWith("flowfile.meta.")) return name.slice("flowfile.meta.".length);
  if (name.startsWith("flowfile.graph.")) return name.slice("flowfile.graph.".length);
  if (name.startsWith("flowfile.schema.")) return name.slice("flowfile.schema.".length);
  if (name.startsWith("flowfile.codegen.")) return name.slice("flowfile.codegen.".length);
  return name;
};

// W38 — payload-level accessors for the rationale-primary rendering.
const rationale = computed<string>(() => _str(props.event.payload ?? {}, "rationale"));
const argSummary = computed<string>(() => _str(props.event.payload ?? {}, "arg_summary"));

// True for tool_call_* events that are user-facing graph / schema / codegen
// operations. We render those with the rationale-primary layout. Other
// events (thinking, drift, paused, retry, abort, complete, info) keep
// their existing one-line summary.
const isToolStep = computed<boolean>(() => {
  const kind = props.event.kind;
  return (
    kind === "tool_call_proposed" ||
    kind === "tool_call_staged" ||
    kind === "tool_call_warned" ||
    kind === "tool_call_rejected"
  );
});

// W38 — hide D002 internal routing + housekeeping info events from the
// user-facing chat trail. Shared helper keeps this in lockstep with the
// timeline-grouping filter in AiAssistant.vue (so meta-only agent runs
// don't produce empty bubbles either).
const isHidden = computed<boolean>(() => isAgentEventHidden(props.event.kind, props.event.payload));

// W38 — rationale-primary headline for tool_call_* events.
//
// The planner attaches the same `rationale` (the model's natural-language
// preamble) to every tool_call_* event in a round — proposed AND the
// follow-up staged/warned/rejected. Rendering the rationale on all of them
// duplicates the same prose in the chat trail (W53 symptom: a proposed +
// rejected pair shows the same paragraph twice). Anchor rationale to the
// `proposed` event only — that's where the model "speaks" — and let the
// outcome events render their kind-specific status line (`Staged X`,
// `Rejected X: <reason>`). Falls back to `arg_summary` (server-generated
// from settings) when the model skipped the preamble, then to a generic
// short-tool-name label when neither is populated.
const toolStepHeadline = computed<string>(() => {
  const name = _shortToolName(_str(props.event.payload ?? {}, "name"));
  if (props.event.kind === "tool_call_proposed") {
    if (rationale.value.trim()) return rationale.value.trim();
    if (argSummary.value.trim()) return argSummary.value.trim();
    return name ? `Planning: ${name}` : "Planning a step…";
  }
  if (props.event.kind === "tool_call_staged") return name ? `Staged ${name}` : "Staged a step";
  if (props.event.kind === "tool_call_warned")
    return `${name ? `Staged ${name}` : "Staged a step"} — with warnings`;
  if (props.event.kind === "tool_call_rejected") {
    const detail = _str(props.event.payload ?? {}, "detail");
    return detail ? `Rejected ${name}: ${detail}` : `Rejected ${name}`;
  }
  return name;
});

// Secondary metadata line: short tool name + the args summary or node id.
// Read by the collapsed "Show details" caption so power users / debugging
// can still see what was actually called.
const toolStepCaption = computed<string>(() => {
  const name = _shortToolName(_str(props.event.payload ?? {}, "name"));
  const nid = _num(props.event.payload ?? {}, "node_id");
  const summary = argSummary.value.trim();

  // ``arg_summary`` already encodes the gist (e.g. "Filter on `[region]=='EU'`").
  // When it's the same string as the rationale, fall back to the raw tool name
  // so the secondary line adds new information.
  const showSummaryAsCaption = summary && summary !== rationale.value.trim();

  const parts: string[] = [];
  if (name) parts.push(name);
  if (showSummaryAsCaption) parts.push(summary);
  if (nid !== null) parts.push(`node ${nid}`);
  return parts.join(" · ");
});

// Raw arguments JSON for the expanded details panel. Pretty-printed so the
// user / debugger can read it; fallback to "{}" when no args. Kept as a
// computed so we don't re-stringify on every render.
const toolStepArgsJson = computed<string>(() => {
  const args = (props.event.payload ?? {}).arguments;
  if (!args || typeof args !== "object") return "";
  try {
    return JSON.stringify(args, null, 2);
  } catch {
    return "";
  }
});

// W38 rejected events keep the refusal detail visible inside the expanded
// panel — useful when debugging why the model's call didn't validate.
const toolStepRejectionDetail = computed<string>(() => {
  if (props.event.kind !== "tool_call_rejected") return "";
  const detail = _str(props.event.payload ?? {}, "detail");
  const reason = _str(props.event.payload ?? {}, "reason");
  if (detail && reason) return `${reason}: ${detail}`;
  return detail || reason;
});

const showRejectionDetail = computed(() => toolStepRejectionDetail.value.length > 0);

// Whether the details disclosure starts open. Closed by default; user can
// toggle. ``ref`` so the user's toggle state persists across renders within
// the same component instance.
const detailsOpen = ref(false);

// W71 v1.2 — humanised stage-transition labels. The planner's
// agent_staged surface emits one ``stage_advanced`` event per state-
// machine transition (see ``planner._log_stage_transition``); without
// rendering them the chat trail goes silent between rounds. We pick the
// most-informative payload field per transition and fall back to a
// generic "stage: X → Y" label for transitions we don't enumerate.
const _renderStageAdvanced = (p: Record<string, unknown>): string => {
  const to = _str(p, "to");
  const from = _str(p, "from");
  const opKind = _str(p, "op_kind");
  const pickedNodeType = _str(p, "picked_node_type");
  const completedOp = _str(p, "completed_op");
  const upstreamRaw = (p as { picked_upstream_ids?: unknown }).picked_upstream_ids;
  const upstream = Array.isArray(upstreamRaw)
    ? upstreamRaw.filter((v): v is number => typeof v === "number")
    : [];

  // Reset transitions (after a successful add or single-stage op):
  // ``to=classify`` with completed_op populated.
  if (to === "classify" && completedOp) {
    const shortName = completedOp.startsWith("flowfile.graph.add_")
      ? completedOp.slice("flowfile.graph.add_".length)
      : completedOp.replace(/^flowfile\.graph\./, "");
    return `Staged ${shortName} — ready for the next step.`;
  }

  // Forward transitions:
  if (to === "pick_type" && opKind) return `Classified as ${opKind}.`;
  if (to === "pick_upstream" && pickedNodeType) return `Picked node type: ${pickedNodeType}.`;
  if (to === "fill_settings") {
    if (upstream.length === 1) return `Attaching to node ${upstream[0]}.`;
    if (upstream.length > 1) return `Attaching to nodes ${upstream.join(", ")}.`;
    return "Filling settings…";
  }
  if (to === "single_stage_op" && opKind) return `Routing to ${opKind} operation.`;

  // Fallback — muted "stage: X → Y" for any transition we didn't enumerate.
  if (from && to) return `Stage: ${from} → ${to}.`;
  return "";
};

// One-line summary used for non-tool-step kinds (thinking / drift / paused
// / retry / abort / complete / info / stage_advanced).
const summary = computed<string>(() => {
  const p = props.event.payload ?? {};
  switch (props.event.kind) {
    case "thinking":
      return _str(p, "text") || "Thinking…";
    case "drift_detected":
      return "Graph changed since the agent started — pausing.";
    case "paused":
      return "Paused, waiting for your decision.";
    case "retry": {
      const attempt = _num(p, "attempt") ?? 1;
      const max = _num(p, "max") ?? 3;
      return `Retrying step (attempt ${attempt} of ${max}).`;
    }
    case "abort":
      return "Agent aborted.";
    case "complete": {
      const ops = _num(p, "op_count") ?? 0;
      return ops === 0
        ? "Agent finished — nothing to stage."
        : `Agent finished — ${ops} ${ops === 1 ? "op" : "ops"} staged for review.`;
    }
    case "info": {
      const msg = _str(p, "message");
      return msg || "";
    }
    case "stage_advanced":
      return _renderStageAdvanced(p);
    default:
      return "";
  }
});

const isThinking = computed(() => props.event.kind === "thinking");

// Markdown for thinking events only. Other kinds are mechanical summaries
// where markdown chars in tool args (e.g. a `*` in a filter expression)
// shouldn't be silently transformed.
const thinkingHtml = computed<string>(() => {
  if (!isThinking.value) return "";
  const text = _str(props.event.payload ?? {}, "text");
  return sanitiseMarkdown(text);
});
</script>

<template>
  <div v-if="!isHidden" :class="`ai-agent-event ai-agent-event--${event.kind}`">
    <!-- W38 — rationale-primary tool_step rendering. -->
    <template v-if="isToolStep">
      <div class="ai-agent-event__rationale">{{ toolStepHeadline }}</div>
      <details
        v-if="toolStepCaption || toolStepArgsJson || showRejectionDetail"
        class="ai-agent-event__details"
        :open="detailsOpen"
      >
        <summary class="ai-agent-event__details-summary">
          <span class="ai-agent-event__caption">{{ toolStepCaption || "Show details" }}</span>
        </summary>
        <pre v-if="toolStepArgsJson" class="ai-agent-event__args">{{ toolStepArgsJson }}</pre>
        <p v-if="showRejectionDetail" class="ai-agent-event__refusal">
          {{ toolStepRejectionDetail }}
        </p>
      </details>
    </template>
    <!-- Thinking events → rendered markdown. eslint-disable: v-html is sanitised. -->
    <!-- eslint-disable-next-line vue/no-v-html -->
    <div
      v-else-if="isThinking && thinkingHtml"
      class="ai-agent-event__markdown"
      v-html="thinkingHtml"
    />
    <span v-else class="ai-agent-event__summary">{{ summary }}</span>
  </div>
</template>

<style scoped>
.ai-agent-event {
  font-size: 12px;
  color: var(--color-text-muted, #6a737d);
  padding: 4px 12px;
  border-left: 2px solid var(--color-border-primary, #d0d7de);
  margin: 2px 0;
  background-color: rgba(175, 184, 193, 0.08);
}

.ai-agent-event--thinking {
  /* Promote thinking events visually — they carry the LLM's actual prose,
     not just step metadata. Match assistant chat bubble styling. */
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
  background-color: var(--color-background-secondary, #f6f8fa);
  border-left-color: var(--color-accent, #6f42c1);
  padding: 8px 12px;
}

.ai-agent-event--tool_call_proposed,
.ai-agent-event--tool_call_staged,
.ai-agent-event--tool_call_warned {
  /* W38 — rationale rendering needs more breathing room than mechanical summaries. */
  padding: 8px 12px;
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
  background-color: var(--color-background-secondary, #f6f8fa);
}

.ai-agent-event--tool_call_staged {
  border-left-color: var(--color-success, #2ea043);
}

.ai-agent-event--tool_call_warned {
  border-left-color: var(--color-warning, #d4a72c);
}

.ai-agent-event--drift_detected,
.ai-agent-event--paused {
  color: var(--color-danger, #c53030);
  background-color: var(--color-danger-light, #ffe5e5);
  border-left-color: var(--color-danger, #c53030);
}

.ai-agent-event--complete {
  color: var(--color-success, #1a7f37);
  border-left-color: var(--color-success, #1a7f37);
}

/* W71 v1.2 — stage_advanced events render as faint trail markers between
   the more salient tool_call_* events. Smaller font, lighter color, no
   secondary background so they read as "the agent is moving through its
   pipeline" without competing with the staged-node summaries. */
.ai-agent-event--stage_advanced {
  font-size: 11px;
  color: var(--color-text-muted, #6a737d);
  background-color: transparent;
  border-left-color: var(--color-border-primary, #d0d7de);
  padding: 2px 12px;
  font-style: italic;
}

.ai-agent-event--tool_call_rejected {
  padding: 8px 12px;
  font-size: 13px;
  color: var(--color-warning, #b08800);
  border-left-color: var(--color-warning, #b08800);
}

.ai-agent-event__summary {
  white-space: pre-wrap;
  word-break: break-word;
}

/* W38 — rationale-primary tool_step rendering. */

.ai-agent-event__rationale {
  font-weight: 500;
  line-height: 1.4;
  white-space: pre-wrap;
  word-break: break-word;
}

.ai-agent-event__details {
  margin-top: 4px;
  font-size: 11px;
  color: var(--color-text-muted, #6a737d);
}

.ai-agent-event__details-summary {
  cursor: pointer;
  list-style: none;
  user-select: none;
}

.ai-agent-event__details-summary::-webkit-details-marker {
  display: none;
}

.ai-agent-event__details-summary::before {
  content: "▸ ";
  color: var(--color-text-muted, #6a737d);
  display: inline-block;
  width: 14px;
}

.ai-agent-event__details[open] > .ai-agent-event__details-summary::before {
  content: "▾ ";
}

.ai-agent-event__caption {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 11px;
  word-break: break-all;
}

.ai-agent-event__args {
  margin: 6px 0 0;
  padding: 6px 8px;
  border-radius: 4px;
  background-color: var(--color-background-tertiary, rgba(175, 184, 193, 0.15));
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 11px;
  line-height: 1.4;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-x: auto;
  max-height: 240px;
}

.ai-agent-event__refusal {
  margin: 6px 0 0;
  font-style: italic;
  color: var(--color-warning, #b08800);
}

/* ------------------------------------------------------------------ */
/* Markdown styling for thinking events. Mirrors AiMessage.vue's       */
/* `.ai-message__markdown` rules so thinking text renders consistently */
/* with regular assistant content.                                     */
/* ------------------------------------------------------------------ */

.ai-agent-event__markdown {
  white-space: normal;
  font-size: 13px;
  line-height: 1.5;
}

.ai-agent-event__markdown :deep(p) {
  margin: 0 0 8px;
}
.ai-agent-event__markdown :deep(p:last-child) {
  margin-bottom: 0;
}

.ai-agent-event__markdown :deep(h1),
.ai-agent-event__markdown :deep(h2),
.ai-agent-event__markdown :deep(h3),
.ai-agent-event__markdown :deep(h4),
.ai-agent-event__markdown :deep(h5),
.ai-agent-event__markdown :deep(h6) {
  margin: 12px 0 6px;
  font-weight: 600;
  line-height: 1.25;
}
.ai-agent-event__markdown :deep(h1) {
  font-size: 16px;
}
.ai-agent-event__markdown :deep(h2) {
  font-size: 15px;
}
.ai-agent-event__markdown :deep(h3) {
  font-size: 14px;
}
.ai-agent-event__markdown :deep(h4),
.ai-agent-event__markdown :deep(h5),
.ai-agent-event__markdown :deep(h6) {
  font-size: 13px;
}

.ai-agent-event__markdown :deep(ul),
.ai-agent-event__markdown :deep(ol) {
  margin: 4px 0 8px;
  padding-left: 20px;
}
.ai-agent-event__markdown :deep(li) {
  margin: 2px 0;
}
.ai-agent-event__markdown :deep(li > p) {
  margin: 0;
}

.ai-agent-event__markdown :deep(code) {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 12px;
  padding: 1px 4px;
  border-radius: 3px;
  background-color: rgba(175, 184, 193, 0.2);
}

.ai-agent-event__markdown :deep(pre) {
  margin: 8px 0;
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-background-tertiary, #f0f1f3);
  overflow-x: auto;
}
.ai-agent-event__markdown :deep(pre code) {
  padding: 0;
  background: transparent;
  font-size: 12px;
  line-height: 1.45;
  white-space: pre;
}

.ai-agent-event__markdown :deep(strong) {
  font-weight: 600;
}
.ai-agent-event__markdown :deep(em) {
  font-style: italic;
}

.ai-agent-event__markdown :deep(a) {
  color: var(--color-link, #0366d6);
  text-decoration: underline;
}

.ai-agent-event__markdown :deep(blockquote) {
  margin: 8px 0;
  padding: 4px 12px;
  border-left: 3px solid var(--color-border-primary, #d0d7de);
  color: var(--color-text-muted, #6a737d);
}

.ai-agent-event__markdown :deep(table) {
  border-collapse: collapse;
  margin: 8px 0;
  font-size: 12px;
}
.ai-agent-event__markdown :deep(th),
.ai-agent-event__markdown :deep(td) {
  border: 1px solid var(--color-border-primary, #d0d7de);
  padding: 4px 8px;
  text-align: left;
}
.ai-agent-event__markdown :deep(th) {
  background-color: var(--color-background-tertiary, rgba(175, 184, 193, 0.15));
  font-weight: 600;
}
</style>
