<script setup lang="ts">
// AI Assistant chat drawer (W20). Read-only — no tool calls, no graph
// mutation. Mounted as an independent <draggable-item> from Canvas.vue
// (D005), so it coexists with the node-settings drawer.
//
// Scope of this component: provider/model picker, message list, composer,
// abort. Anything more (context pinning, @-mentions, diff preview) lands
// in W22 / W24 / W31+ and extends the store rather than this view.

import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useAiAgentStore, type AgentEvent } from "../../stores/ai-agent-store";
import { useAiStore, type ChatMessage } from "../../stores/ai-store";
import { useFlowStore } from "../../stores/flow-store";
import { AiDisabledError } from "../../views/AiProvidersView/api";
import AiAgentRun from "./AiAgentRun.vue";
import AiDiffPanel from "./AiDiffPanel.vue";
import AiMessage from "./AiMessage.vue";
import AiMentionAutocomplete from "./AiMentionAutocomplete.vue";
import { isAgentEventHidden } from "./argSummary";
import { useMentionAutocomplete } from "./useMentionAutocomplete";

const emit = defineEmits<{ close: [] }>();

const aiStore = useAiStore();
const flowStore = useFlowStore();
const agentStore = useAiAgentStore();

// W40 — agent-mode toggle. When true, the composer's Send dispatches the
// multi-turn planner instead of single-shot chat. Defaults off so the read-
// only chat surface stays the path of least surprise; experienced users opt
// in for the "build me a flow that…" workflow.
const agentMode = ref(false);

const composerText = ref("");
const composerTextarea = ref<HTMLTextAreaElement | null>(null);
const messageContainerRef = ref<HTMLElement | null>(null);
const isDisabledByFlag = ref(false);

// W24 — `@`-mention autocomplete. Source the candidate node list from
// the live VueFlow graph (the chat drawer is mounted from Canvas.vue
// per D005, so `vueFlowInstance` is populated whenever the drawer is
// open). When the instance isn't ready yet, surface only the bare
// kinds (`@flow`, `@selection`).
const mention = useMentionAutocomplete(composerTextarea, composerText, () => {
  const instance = flowStore.vueFlowInstance;
  if (!instance) return [];
  const liveNodes = instance.getNodes?.value ?? [];
  type LiveNode = { id: string; data?: { id?: number | string; label?: string | null } };
  return (liveNodes as LiveNode[]).map((node) => ({
    id: node.data?.id ?? node.id,
    name: node.data?.label ?? null,
  }));
});

const placeholder = computed(() =>
  aiStore.hasConfiguredProvider
    ? "Ask anything about Flowfile or your data..."
    : "Configure a provider in Settings → AI Providers to start chatting.",
);

// W29 — model picker source. Prefer the credential's curated `models` list
// (multiple free models behind one OpenRouter / Groq key); fall back to the
// singleton `defaultModel`; finally the class-level default. Picker only
// renders when there are multiple options.
const selectedProviderMeta = computed(() => {
  const name = aiStore.selectedProvider;
  if (!name) return null;
  return aiStore.providers.find((p) => p.provider === name) ?? null;
});

const availableModels = computed<string[]>(() => {
  const meta = selectedProviderMeta.value;
  if (!meta) return [];
  const curated = meta.credential?.models;
  if (curated && curated.length > 0) return curated;
  const singleton = meta.credential?.defaultModel ?? meta.defaultModel ?? null;
  return singleton ? [singleton] : [];
});

const showModelPicker = computed(() => availableModels.value.length >= 1);

const isAgentRunning = computed(
  () => agentStore.status === "running" || agentStore.status === "paused_drift",
);

const canSend = computed(
  () =>
    !aiStore.isStreaming &&
    !isAgentRunning.value &&
    aiStore.hasConfiguredProvider &&
    composerText.value.trim().length > 0 &&
    aiStore.selectedProvider !== null,
);

const scrollToBottom = (): void => {
  void nextTick(() => {
    requestAnimationFrame(() => {
      const el = messageContainerRef.value;
      if (el) el.scrollTop = el.scrollHeight;
    });
  });
};

watch(
  () => aiStore.messages.length,
  () => scrollToBottom(),
);

watch(
  () => aiStore.messages.map((m) => m.content).join("\n"),
  () => scrollToBottom(),
);

onMounted(async () => {
  try {
    await aiStore.loadProviders();
  } catch (err) {
    if (err instanceof AiDisabledError) {
      isDisabledByFlag.value = true;
    }
  }
});

const handleProviderChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedProvider(target.value);
};

const handleModelChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedModel(target.value || null);
};

const handleSend = async (): Promise<void> => {
  if (!canSend.value) return;
  const text = composerText.value;
  composerText.value = "";

  if (agentMode.value) {
    if (flowStore.flowId === null) {
      // Hard-edge case: agent mode without a flow loaded — surface the gap as
      // a chat-side error rather than silently no-op'ing.
      aiStore.streamError = "Open a flow before starting the agent.";
      return;
    }
    // Capture the user's typed prompt in the visible chat trail. The wire-
    // level prompt is built server-side via W22's render_prompt_context;
    // this synthetic message is purely cosmetic so the user sees what they
    // asked alongside the streamed agent events.
    aiStore.messages.push({
      id: Date.now(),
      createdAt: Date.now(),
      role: "user",
      content: `[Agent] ${text}`,
    });

    // W49 — when the active session is followup-resumable (``completed``
    // or ``awaiting_user_input``), route the user's typed message through
    // the followup endpoint rather than allocating a fresh session via
    // ``start()``. This keeps the conversation history and avoids the
    // ``── new agent run ──`` boundary insertion the chat trail uses to
    // mark genuine new sessions.
    const sid = agentStore.currentSessionId;
    if (
      sid !== null &&
      (agentStore.status === "completed" || agentStore.status === "awaiting_user_input")
    ) {
      await agentStore.resumeAfterMessage(sid, text);
      return;
    }

    // Forward the model the user picked verbatim. If the W29 picker has a
    // value, that's the user's choice and the backend honours it. If it's
    // null (no picker rendered, no manual selection), the backend's
    // surface-aware routing picks a tool-capable model from
    // ``Provider.surface_models[surface]``.
    await agentStore.start({
      flow_id: flowStore.flowId,
      prompt: text,
      // ``agent`` is the planner default — two-stage with ``pick_category``,
      // routes to a sonnet-class model. ``agent_complex`` exposes the full
      // catalog and routes to opus-class; opt in via a future toggle if
      // dogfood shows the two-stage flow under-performs.
      surface: "agent",
      provider: aiStore.selectedProvider ?? "anthropic",
      model: aiStore.selectedModel ?? null,
    });
    return;
  }

  await aiStore.sendMessage(text);
};

const handleAbort = (): void => {
  if (agentStore.status === "running" || agentStore.status === "paused_drift") {
    void agentStore.abort();
    return;
  }
  aiStore.abortStream();
};

const handleAgentResumeContinue = async (): Promise<void> => {
  const sid = agentStore.currentSessionId;
  if (!sid) return;
  await agentStore.resumeContinue(sid);
};

// W58 — banner "Keep this as chat instead" affordance: aborts the in-flight
// agent run, flips ``autoPromote`` off for the session, and re-dispatches
// the saved message as a regular chat. The store wraps all of that.
const handleUndoPromotion = async (): Promise<void> => {
  await aiStore.undoPromotion();
};

// W58 round 7 — banner "Continue as agent" affordance: keeps the current
// run going AND short-circuits classification on subsequent sends so every
// future message dispatches straight to the agent. The store handles the
// session-flag flip; we just clear the banner via the store.
const handleAcceptPromotion = (): void => {
  aiStore.acceptPromotion();
};

// W58 — drawer-side autoPromote toggle (the AI Settings tab carries a
// secondary entry point; the drawer is where the behavior is most
// discoverable since it's right next to where the user is typing). The
// toggle is grayed out while the manual Agent toggle is on or a stream
// is in flight — those states make autoPromote a no-op.
const handleAutoPromoteChange = (event: Event): void => {
  const target = event.target as HTMLInputElement;
  aiStore.setAutoPromote(target.checked);
};

const autoPromoteTitle = computed<string>(() =>
  agentMode.value
    ? "Manual Agent toggle is on — auto-promotion is overridden until you turn it off."
    : aiStore.autoPromote
      ? "Auto-detect build requests and switch to Agent mode for this send. Read-only questions stay in chat."
      : "Off — every message stays in chat unless you flip the manual Agent toggle.",
);

const handleAgentResumeDiscard = async (): Promise<void> => {
  const sid = agentStore.currentSessionId;
  if (!sid) return;
  await agentStore.resumeDiscard(sid);
};

const handleClear = (): void => {
  aiStore.clearMessages();
  // Also wipe the agent event trail + persisted state so the chat is
  // genuinely empty after clicking Clear. ``agentStore.clear()`` aborts
  // any in-flight stream and removes the sessionStorage entry too.
  agentStore.clear();
};

const handleComposerKeydown = (event: KeyboardEvent): void => {
  // W24 — let the mention autocomplete intercept Up/Down/Enter/Tab/Escape
  // before the composer's own Enter-to-send guard fires.
  if (mention.onKeyDown(event)) return;

  // Enter sends, Shift+Enter inserts a newline. Mirrors most chat UIs.
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    void handleSend();
  }
};

const handleComposerInput = (): void => {
  mention.onInput();
};

const handleMentionPick = (candidate: Parameters<typeof mention.pick>[0]): void => {
  mention.pick(candidate);
};

const handleMentionDismiss = (): void => {
  mention.close();
};

const handleMentionHover = (index: number): void => {
  mention.setActiveIndex(index);
};

// W45 Q3 — render the drift banner's per-id rows with typed labels:
// "Filter node 6 was deleted" / "Manual-input node 3 was added externally".
// Falls back to "Node {id}" when the snapshot didn't capture a type.
const _formatNodeType = (nodeType: string): string => {
  if (!nodeType) return "Node";
  const pretty = nodeType.replace(/_/g, "-");
  return pretty.charAt(0).toUpperCase() + pretty.slice(1) + " node";
};
const formatDriftRemoval = (id: number): string => {
  const nt = agentStore.driftDetail?.nodeTypes?.[id];
  return `${_formatNodeType(nt ?? "")} ${id} was deleted`;
};
const formatDriftExternalAdd = (id: number): string => {
  const nt = agentStore.driftDetail?.nodeTypes?.[id];
  return `${_formatNodeType(nt ?? "")} ${id} was added externally`;
};

// Merged chronological timeline of chat messages and agent events.
//
// `ChatMessage.createdAt` is `Date.now()` at push time; `AgentEvent.at`
// is `Date.now()` captured at append. Both directly comparable.
// `ChatMessage.id` is a small monotonic counter — historically used here,
// but it can't interleave with event timestamps (1, 2, 3 always sort
// before 1.7e12), which is why earlier layouts pinned all events to the
// bottom even when they were newer than the messages above them.
//
// Walk the sorted list and **group consecutive agent events into a single
// "agent run" bubble**, breaking only when a chat message appears between
// events. The result reads chronologically: user q → assistant a → [Agent]
// q → agent run (one bubble) → next user q → next assistant a.
type ChatTimelineItem = { kind: "message"; at: number; data: ChatMessage };
type AgentRunTimelineItem = { kind: "agent_run"; at: number; events: AgentEvent[] };
type TimelineItem = ChatTimelineItem | AgentRunTimelineItem;

const timelineItems = computed<TimelineItem[]>(() => {
  type RawItem =
    | { kind: "message"; at: number; data: ChatMessage }
    | { kind: "event"; at: number; data: AgentEvent };
  const raw: RawItem[] = [];
  for (const msg of aiStore.messages) {
    raw.push({ kind: "message", at: msg.createdAt, data: msg });
  }
  for (const evt of agentStore.events) {
    // W38 — filter D002 meta routing + housekeeping info events out of the
    // chat trail before grouping, so a streaming run that produces nothing
    // but pick_category + "category narrowed" doesn't render as an empty
    // agent bubble between user/assistant messages.
    if (isAgentEventHidden(evt.kind, evt.payload)) continue;
    raw.push({ kind: "event", at: evt.at, data: evt });
  }
  raw.sort((a, b) => a.at - b.at);

  const grouped: TimelineItem[] = [];
  let currentRun: AgentRunTimelineItem | null = null;
  for (const item of raw) {
    if (item.kind === "event") {
      if (currentRun === null) {
        currentRun = { kind: "agent_run", at: item.at, events: [] };
        grouped.push(currentRun);
      }
      currentRun.events.push(item.data);
    } else {
      currentRun = null;
      grouped.push({ kind: "message", at: item.at, data: item.data });
    }
  }
  return grouped;
});
</script>

<template>
  <div class="ai-assistant">
    <header class="ai-assistant__header">
      <select
        v-if="aiStore.providers.length > 0 && !isDisabledByFlag"
        class="ai-assistant__select"
        :value="aiStore.selectedProvider ?? ''"
        :disabled="aiStore.isStreaming || isAgentRunning"
        @change="handleProviderChange"
      >
        <option value="" disabled>Pick a provider</option>
        <option
          v-for="provider in aiStore.providers"
          :key="provider.provider"
          :value="provider.provider"
          :disabled="provider.status === 'unconfigured'"
        >
          {{ provider.provider }}
          <template v-if="provider.status === 'env_fallback'"> (env)</template>
          <template v-else-if="provider.status === 'unconfigured'"> (not configured)</template>
        </option>
      </select>
      <select
        v-if="showModelPicker && !isDisabledByFlag"
        class="ai-assistant__select ai-assistant__select--model"
        :value="aiStore.selectedModel ?? ''"
        :disabled="aiStore.isStreaming || isAgentRunning"
        :title="aiStore.selectedModel ?? 'Pick a model'"
        @change="handleModelChange"
      >
        <option v-for="model in availableModels" :key="model" :value="model">
          {{ model }}
        </option>
      </select>
      <label
        v-if="!isDisabledByFlag"
        class="ai-assistant__agent-toggle"
        :title="
          agentMode
            ? 'Agent mode is on — Send opens a multi-turn planner that stages a GraphDiff for review.'
            : 'Agent mode is off — Send starts a read-only chat.'
        "
      >
        <input
          v-model="agentMode"
          type="checkbox"
          :disabled="isAgentRunning || aiStore.isStreaming"
        />
        Agent
      </label>
      <button
        v-if="aiStore.messages.length > 0 || agentStore.events.length > 0"
        type="button"
        class="ai-assistant__clear"
        :disabled="aiStore.isStreaming || isAgentRunning"
        @click="handleClear"
      >
        Clear
      </button>
      <button
        type="button"
        class="ai-assistant__close"
        aria-label="Close AI assistant"
        @click="emit('close')"
      >
        ×
      </button>
    </header>

    <div v-if="isDisabledByFlag" class="ai-assistant__notice">
      <p>AI features are disabled.</p>
      <p class="ai-assistant__notice-hint">
        Set <code>FEATURE_FLAG_AI=true</code> on the backend and restart <code>flowfile_core</code>.
      </p>
    </div>

    <div v-else-if="aiStore.providersLoading" class="ai-assistant__notice">Loading providers…</div>

    <div v-else-if="!aiStore.hasConfiguredProvider" class="ai-assistant__notice">
      <p>No providers configured yet.</p>
      <p class="ai-assistant__notice-hint">
        Open <strong>Connections → AI Providers</strong> and add an API key (or run a local Ollama
        server) to start chatting.
      </p>
    </div>

    <div v-else class="ai-assistant__chat">
      <AiDiffPanel />
      <div v-if="aiStore.promotionBanner !== null" class="ai-assistant__promo-banner">
        <p class="ai-assistant__promo-text">
          <span class="ai-assistant__promo-icon">✨</span>
          <span> Switched to Agent mode — {{ aiStore.promotionBanner.reason }}. </span>
        </p>
        <div class="ai-assistant__promo-actions">
          <button
            type="button"
            class="ai-assistant__promo-accept"
            title="Skip the classifier on subsequent sends; every message goes to the agent until you Clear or undo."
            @click="handleAcceptPromotion"
          >
            Continue as agent
          </button>
          <button type="button" class="ai-assistant__promo-undo" @click="handleUndoPromotion">
            Keep this as chat instead
          </button>
        </div>
      </div>
      <div
        v-if="agentStore.status === 'awaiting_user_input'"
        class="ai-assistant__awaiting-banner"
        role="status"
      >
        <span class="ai-assistant__awaiting-icon">💬</span>
        <span class="ai-assistant__awaiting-text">
          Agent is waiting for your reply — type below and Send to continue the same session.
        </span>
      </div>
      <div
        v-if="agentStore.status === 'paused_drift' && agentStore.driftDetail"
        class="ai-assistant__drift-banner"
      >
        <p class="ai-assistant__drift-title">⚠ Graph changed since the agent started</p>
        <ul class="ai-assistant__drift-list">
          <li v-for="id in agentStore.driftDetail.missingNodeIds" :key="`missing-${id}`">
            {{ formatDriftRemoval(id) }}
          </li>
          <li v-for="id in agentStore.driftDetail.externalAddedNodeIds" :key="`added-${id}`">
            {{ formatDriftExternalAdd(id) }}
          </li>
        </ul>
        <div class="ai-assistant__drift-actions">
          <button
            type="button"
            class="ai-assistant__btn ai-assistant__btn--danger"
            @click="handleAgentResumeDiscard"
          >
            Discard
          </button>
          <button
            type="button"
            class="ai-assistant__btn ai-assistant__btn--primary"
            @click="handleAgentResumeContinue"
          >
            Continue against new state
          </button>
        </div>
      </div>
      <div ref="messageContainerRef" class="ai-assistant__messages">
        <div
          v-if="aiStore.messages.length === 0 && timelineItems.length === 0"
          class="ai-assistant__empty"
        >
          <p v-if="agentMode">
            Agent mode is on. Type a goal — e.g. "filter to last 30 days, then sort by region".
          </p>
          <p v-else>Ask a question, or turn on Agent mode to make changes.</p>
        </div>
        <template v-for="(item, idx) in timelineItems" :key="`${item.kind}-${item.at}-${idx}`">
          <AiMessage v-if="item.kind === 'message'" :message="item.data" />
          <AiAgentRun v-else :events="item.events" />
        </template>
      </div>
    </div>

    <div v-if="aiStore.streamError" class="ai-assistant__error">
      {{ aiStore.streamError }}
    </div>

    <footer v-if="!isDisabledByFlag" class="ai-assistant__composer">
      <div class="ai-assistant__textarea-wrapper">
        <textarea
          ref="composerTextarea"
          v-model="composerText"
          class="ai-assistant__textarea"
          rows="3"
          :placeholder="placeholder"
          :disabled="!aiStore.hasConfiguredProvider"
          @keydown="handleComposerKeydown"
          @input="handleComposerInput"
          @click="handleComposerInput"
          @keyup="handleComposerInput"
        ></textarea>
        <AiMentionAutocomplete
          :candidates="mention.candidates.value"
          :active-index="mention.activeIndex.value"
          :position="mention.caretPosition.value"
          @pick="handleMentionPick"
          @dismiss="handleMentionDismiss"
          @hover="handleMentionHover"
        />
      </div>
      <div class="ai-assistant__actions">
        <label class="ai-assistant__autopromote-toggle" :title="autoPromoteTitle">
          <input
            type="checkbox"
            :checked="aiStore.autoPromote"
            :disabled="agentMode || isAgentRunning || aiStore.isStreaming"
            @change="handleAutoPromoteChange"
          />
          Auto-agent
        </label>
        <button
          v-if="aiStore.isStreaming || isAgentRunning"
          type="button"
          class="ai-assistant__btn ai-assistant__btn--danger"
          @click="handleAbort"
        >
          Stop
        </button>
        <button
          v-else
          type="button"
          class="ai-assistant__btn ai-assistant__btn--primary"
          :disabled="!canSend"
          @click="handleSend"
        >
          Send
        </button>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.ai-assistant {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 8px;
  padding: 10px;
  font-family: var(--font-family-base, system-ui);
  background-color: var(--color-background-primary, #ffffff);
}

.ai-assistant__header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--color-border-primary, #e1e4e8);
  min-width: 0;
}

.ai-assistant__select {
  flex: 1 1 0;
  min-width: 0;
  padding: 6px 8px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 12px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__select:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.ai-assistant__clear {
  flex-shrink: 0;
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-secondary, #f6f8fa);
  font-size: 12px;
  cursor: pointer;
}

.ai-assistant__clear:hover:enabled {
  background-color: var(--color-background-hover, #ececec);
}

.ai-assistant__close {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  padding: 0;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-secondary, #f6f8fa);
  font-size: 18px;
  line-height: 1;
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
}

.ai-assistant__close:hover {
  background-color: var(--color-background-hover, #ececec);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__chat {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.ai-assistant__messages {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 4px 2px;
}

.ai-assistant__empty {
  padding: 20px 8px;
  text-align: center;
  color: var(--color-text-muted, #6a737d);
  font-size: 13px;
}

.ai-assistant__notice {
  padding: 16px;
  border-radius: 8px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__notice-hint {
  margin-top: 6px;
  color: var(--color-text-muted, #6a737d);
  font-size: 12px;
}

.ai-assistant__error {
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-danger-light, #ffe5e5);
  color: var(--color-danger, #c53030);
  font-size: 12px;
}

.ai-assistant__composer {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding-top: 8px;
  border-top: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-assistant__textarea-wrapper {
  position: relative;
}

.ai-assistant__textarea {
  resize: vertical;
  min-height: 60px;
  width: 100%;
  padding: 8px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-family: inherit;
  font-size: 13px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
  box-sizing: border-box;
}

.ai-assistant__textarea:disabled {
  background-color: var(--color-background-secondary, #f6f8fa);
  cursor: not-allowed;
}

.ai-assistant__actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}

/* W58 — drawer-side auto-promotion toggle. Sits left of Send so the user
   can see whether auto-promotion is armed without leaving the drawer. */
.ai-assistant__autopromote-toggle {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  color: var(--color-text-secondary, #586069);
  cursor: pointer;
  user-select: none;
}

.ai-assistant__autopromote-toggle input[type="checkbox"] {
  cursor: pointer;
}

.ai-assistant__autopromote-toggle:has(input:disabled) {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__autopromote-toggle:has(input:disabled) input[type="checkbox"] {
  cursor: not-allowed;
}

.ai-assistant__btn {
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
}

.ai-assistant__btn--primary {
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #6f42c1) 0%,
    var(--color-gradient-purple-end, #5933a8) 100%
  );
  color: var(--color-text-inverse, #ffffff);
}

.ai-assistant__btn--primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__btn--danger {
  background-color: var(--color-danger, #c53030);
  color: var(--color-text-inverse, #ffffff);
}

/* W40 — agent-mode toggle + drift banner + event list */

.ai-assistant__agent-toggle {
  display: inline-flex;
  flex-shrink: 0;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  color: var(--color-text-secondary, #586069);
  cursor: pointer;
  user-select: none;
}

.ai-assistant__agent-toggle input[type="checkbox"] {
  cursor: pointer;
}

.ai-assistant__awaiting-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  margin: 0 0 8px 0;
  background-color: var(--color-info-soft, #e3f2fd);
  border: 1px solid var(--color-info, #2196f3);
  border-radius: 4px;
  font-size: 12px;
  color: var(--color-info-text, #0d47a1);
}

.ai-assistant__awaiting-icon {
  font-size: 14px;
}

.ai-assistant__awaiting-text {
  flex: 1;
}

.ai-assistant__drift-banner {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 12px;
  margin: 0 0 8px 0;
  background-color: var(--color-warning-soft, #fff3cd);
  border: 1px solid var(--color-warning, #d4a72c);
  border-radius: 4px;
  font-size: 12px;
}

.ai-assistant__drift-title {
  margin: 0;
  font-weight: 600;
  color: var(--color-warning-text, #5c4400);
}

.ai-assistant__drift-list {
  margin: 0;
  padding-left: 18px;
  list-style: disc;
}

.ai-assistant__drift-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}

/* Old `.ai-assistant__agent-events` / `.ai-assistant__agent-event` styles
   moved into the `AiAgentEvent` component's scoped block — the timeline
   now renders each item as its own component, not a `<li>` in a `<ul>`. */

/* W58 — chat → agent auto-promotion banner. Sits above the messages and
   below the diff panel; visually distinct from the W40 drift banner so
   the two don't read as the same alert pattern. */
.ai-assistant__promo-banner {
  display: flex;
  flex-direction: column;
  padding: 8px 12px;
  margin: 0 0 8px 0;
  background-color: var(--color-info-soft, #eef2ff);
  border: 1px solid var(--color-info, #6366f1);
  border-radius: 4px;
  font-size: 12px;
  color: var(--color-info-text, #312e81);
}

.ai-assistant__promo-text {
  margin: 0;
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: baseline;
}

.ai-assistant__promo-icon {
  flex-shrink: 0;
}

.ai-assistant__promo-actions {
  display: flex;
  gap: 12px;
  align-items: center;
  margin-top: 6px;
  flex-wrap: wrap;
}

.ai-assistant__promo-accept {
  padding: 4px 12px;
  border: 1px solid var(--color-info, #6366f1);
  background-color: var(--color-info, #6366f1);
  color: var(--color-text-inverse, #ffffff);
  border-radius: 4px;
  font: inherit;
  font-weight: 500;
  cursor: pointer;
}

.ai-assistant__promo-accept:hover {
  background-color: var(--color-info-hover, #4f46e5);
  border-color: var(--color-info-hover, #4f46e5);
}

.ai-assistant__promo-undo {
  padding: 0;
  border: none;
  background: transparent;
  color: var(--color-info, #6366f1);
  font: inherit;
  text-decoration: underline;
  cursor: pointer;
}

.ai-assistant__promo-undo:hover {
  text-decoration: none;
}
</style>
