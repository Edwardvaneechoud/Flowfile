<script setup lang="ts">
// AI Assistant chat drawer. Read-only — no tool calls, no graph
// mutation. Mounted as an independent <draggable-item> from
// Canvas.vue, so it coexists with the node-settings drawer.
//
// Scope of this component: provider/model picker, message list,
// composer, abort. Anything more (context pinning, @-mentions, diff
// preview) lives in the store and other modules; this view stays
// composer-only.

import { computed, nextTick, onMounted, onBeforeUnmount, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useAiAgentStore, type AgentEvent } from "../../stores/ai-agent-store";
import { useAiStore, type AiMode, type ChatMessage } from "../../stores/ai-store";
import { useFlowStore } from "../../stores/flow-store";
import { AiDisabledError } from "../../views/AiProvidersView/api";
import { LOCAL_PROVIDER_ID, LOCAL_PROVIDER_LABEL } from "../../views/AiProvidersView/localModelApi";
import AiAgentRun from "./AiAgentRun.vue";
import AiAvatar from "./AiAvatar.vue";
import AiDiffPanel from "./AiDiffPanel.vue";
import AiMessage from "./AiMessage.vue";
import AiMentionAutocomplete from "./AiMentionAutocomplete.vue";
import AiThinkingDots from "./AiThinkingDots.vue";
import { isAgentEventHidden } from "./argSummary";
import { useMentionAutocomplete } from "./useMentionAutocomplete";

const aiStore = useAiStore();
const flowStore = useFlowStore();
const agentStore = useAiAgentStore();
const router = useRouter();

// Settings popover anchored to the gear button in the header. Holds
// provider / model / agent-variant pickers plus inline explanations
// of what each variant does. Closed by default; opens on gear click,
// closes on outside click or Esc.
const settingsOpen = ref(false);
const settingsAnchorRef = ref<HTMLElement | null>(null);

// Info popover — shows AI use cases / tips. Mutually exclusive with
// the settings popover (opening one closes the other). Same dismiss
// rules: outside-click + Esc.
const infoOpen = ref(false);
const infoAnchorRef = ref<HTMLElement | null>(null);

const composerText = ref("");
const composerTextarea = ref<HTMLTextAreaElement | null>(null);
const messageContainerRef = ref<HTMLElement | null>(null);
const isDisabledByFlag = ref(false);

// `@`-mention autocomplete. Source the candidate node list from the
// live VueFlow graph (the chat drawer is mounted from Canvas.vue, so
// `vueFlowInstance` is populated whenever the drawer is open). When
// the instance isn't ready yet, surface only the bare kinds
// (`@flow`, `@selection`).
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

// Model picker source. Prefer the credential's curated `models` list
// (multiple free models behind one OpenRouter / Groq key); fall back
// to the singleton `defaultModel`; finally the class-level default.
// Picker only renders when there are multiple options.
const selectedProviderMeta = computed(() => {
  const name = aiStore.selectedProvider;
  if (!name) return null;
  return aiStore.providers.find((p) => p.provider === name) ?? null;
});

// User-facing provider label: the wire id stays "local"; users see "On-device AI".
const providerLabel = (id: string): string =>
  id === LOCAL_PROVIDER_ID ? LOCAL_PROVIDER_LABEL : id;

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

// Agent_staged stage badge labels. The state machine has 4 stages
// for an "add" intent (classify → pick_type → pick_upstream →
// fill_settings) and 2 stages for a non-add intent (classify →
// single_stage_op). The numerator counts up through stages so the
// user sees concrete progress; the denominator reflects the path:
// 4 if op_kind is "add" (or unknown), 2 otherwise.
const STAGE_ORDER_ADD: Record<string, number> = {
  classify: 1,
  pick_type: 2,
  pick_upstream: 3,
  fill_settings: 4,
};
const STAGE_ORDER_OTHER: Record<string, number> = {
  classify: 1,
  single_stage_op: 2,
};

const STAGE_HUMAN_LABELS: Record<string, string> = {
  classify: "classifying intent",
  pick_type: "picking node type",
  pick_upstream: "picking upstream",
  fill_settings: "filling settings",
  single_stage_op: "applying operation",
};

const stageStepLabel = computed<string>(() => {
  const isAdd = agentStore.pickedOpKind === "add" || agentStore.pickedOpKind === null;
  const total = isAdd ? 4 : 2;
  const order = isAdd ? STAGE_ORDER_ADD : STAGE_ORDER_OTHER;
  const idx = order[agentStore.stage] ?? 1;
  return `Step ${idx}/${total}`;
});

const stageReadableLabel = computed<string>(
  () => STAGE_HUMAN_LABELS[agentStore.stage] ?? agentStore.stage,
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

// Agent-side scroll triggers. Agent events stream into agentStore.events
// (separate from aiStore.messages), so the watchers above never fire
// during an agent run. Track count + status here, plus a deep watch for
// streaming payload updates within an event (e.g., a thinking event's
// text accumulating tokens).
watch(
  () => agentStore.events.length,
  () => scrollToBottom(),
);

watch(
  () => agentStore.events,
  () => scrollToBottom(),
  { deep: true },
);

// Status flips drive the thinking placeholder bubble (visible while
// running). Both its appearance and disappearance change scrollHeight,
// so re-anchor to bottom on either edge.
watch(
  () => isAgentRunning.value,
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
  // Outside-click + Esc dismiss for the settings popover. The handlers
  // early-out when ``settingsOpen`` is false so they're cheap when idle.
  document.addEventListener("click", handleDocumentClick);
  document.addEventListener("keydown", handleDocumentKeydown);
});

onBeforeUnmount(() => {
  document.removeEventListener("click", handleDocumentClick);
  document.removeEventListener("keydown", handleDocumentKeydown);
});

const handleAgentSurfaceChange = (event: Event): void => {
  // Handler bound to radio inputs in the settings popover; cast
  // accepts either HTMLInputElement (radios) or HTMLSelectElement
  // (legacy callers, if any) so the read of ``.value`` is type-safe.
  const target = event.target as HTMLInputElement;
  const value = target.value;
  // Three surfaces are user-selectable:
  //   - agent_live (default) — REPL-style; applies each step live.
  //   - agent_staged — multi-stage planner; reviews steps before staging.
  //   - agent_complex — single-shot full catalog.
  if (value === "agent_complex" || value === "agent_staged" || value === "agent_live") {
    aiStore.setSelectedAgentSurface(value);
  }
};

const handleVerifyPlanCompletionChange = (event: Event): void => {
  // Opt-in verify-completion gate. Toggled via a checkbox in the
  // agent settings popover; the value flows into
  // ``AgentStartRequest.verify_plan_completion`` on every dispatch
  // (both the manual Agent toggle in this component and the
  // auto-promote-from-chat path in ai-store.ts).
  const target = event.target as HTMLInputElement;
  aiStore.setVerifyPlanCompletion(target.checked);
};

const handleProviderChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedProvider(target.value);
};

const handleModelChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedModel(target.value || null);
};

// --- Local model (offline) UI ---
// The local 1.5B model can't drive the tool-calling agent, so when it's the
// selected provider we replace the Chat/Auto-agent/Agent dropdown with a
// simpler Chat / Generate toggle. "Generate" runs one-shot flow generation
// (→ diff-review panel); "Chat" is plain offline Q&A.
const isLocalSelected = computed(() => aiStore.isLocalSelected);

// Apply a Simple-build result onto the canvas (inline "Add to canvas" button).
const handleAddBuild = (messageId: number): void => {
  void aiStore.addBuiltFlow(messageId);
};

const handleSend = async (): Promise<void> => {
  if (!canSend.value) return;
  const text = composerText.value;
  composerText.value = "";
  exitRecallMode();

  // Simple build is a universal mode (local or cloud): generate a whole flow
  // in one response, surfaced with an inline "Add to canvas" button.
  if (aiStore.mode === "simple") {
    await aiStore.generateFlowFromComposer(text);
    return;
  }

  // The local model can't drive the tool-calling agent, so its only other mode
  // is plain chat (the mode dropdown hides auto/agent for local).
  if (isLocalSelected.value) {
    await aiStore.sendMessage(text);
    return;
  }

  if (aiStore.mode === "agent") {
    if (flowStore.flowId === null) {
      // Hard-edge case: agent mode without a flow loaded — surface the gap as
      // a chat-side error rather than silently no-op'ing.
      aiStore.streamError = "Open a flow before starting the agent.";
      return;
    }
    // Capture the user's typed prompt in the visible chat trail. The
    // wire-level prompt is built server-side via
    // ``render_prompt_context``; this synthetic message is purely
    // cosmetic so the user sees what they asked alongside the
    // streamed agent events.
    aiStore.messages.push({
      id: Date.now(),
      createdAt: Date.now(),
      role: "user",
      content: `[Agent] ${text}`,
    });

    // When the active session is followup-resumable (``completed`` or
    // ``awaiting_user_input``), route the user's typed message
    // through the followup endpoint rather than allocating a fresh
    // session via ``start()``. This keeps the conversation history
    // and avoids the ``── new agent run ──`` boundary insertion the
    // chat trail uses to mark genuine new sessions.
    const sid = agentStore.currentSessionId;
    if (
      sid !== null &&
      (agentStore.status === "completed" || agentStore.status === "awaiting_user_input")
    ) {
      await agentStore.resumeAfterMessage(sid, text);
      return;
    }

    // Fold prior chat history into the agent prompt the same way the
    // auto-promotion path does. Without this, toggling the manual
    // Agent checkbox after a chat assistant has laid out a plan and
    // typing *"Implement the plan"* would forward a context-less
    // message and the agent would reply *"I'm not sure which specific
    // plan you'd like me to implement"*. ``buildAgentPromptWithHistory``
    // returns ``text`` verbatim when no prior chat exists (no harm on
    // the first-message-of-session case).
    const promptWithHistory = aiStore.buildAgentPromptWithHistory(text);

    // Forward the model the user picked verbatim. If the picker has
    // a value, that's the user's choice and the backend honours it.
    // If it's null (no picker rendered, no manual selection), the
    // backend's surface-aware routing picks a tool-capable model from
    // ``Provider.surface_models[surface]``.
    await agentStore.start({
      flow_id: flowStore.flowId,
      prompt: promptWithHistory,
      // Surface is user-selectable via the settings popover. Default
      // is ``agent_live`` (REPL-style; applies each step live).
      // ``agent_staged`` (multi-stage planner with batched diff) and
      // ``agent_complex`` (single-shot full catalog) are opt-ins.
      surface: aiStore.selectedAgentSurface,
      provider: aiStore.selectedProvider ?? "anthropic",
      model: aiStore.selectedModel ?? null,
      // Opt-in verify-completion gate. Default off; toggled via the
      // agent settings panel checkbox so a multi-step plan that ends
      // prematurely gets one auto-correction round.
      verify_plan_completion: aiStore.verifyPlanCompletion,
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

// Banner "Keep this as chat instead" affordance: aborts the
// in-flight agent run, flips the session mode to chat, and
// re-dispatches the saved message as a regular chat. The store
// wraps all of that.
const handleUndoPromotion = async (): Promise<void> => {
  await aiStore.undoPromotion();
};

// Banner "Continue as agent" affordance: keeps the current run going
// AND short-circuits classification on subsequent sends so every
// future message dispatches straight to the agent. The store handles
// the session-flag flip; we just clear the banner via the store.
const handleAcceptPromotion = (): void => {
  aiStore.acceptPromotion();
};

// Unified mode dropdown next to Send. Persisted via the store's
// sessionStorage `mode` key so refresh keeps the user's choice
// within the same tab.
const handleModeChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setMode(target.value as AiMode);
};

const modeTooltip = computed<string>(() => {
  switch (aiStore.mode) {
    case "chat":
      return "Chat mode — every message stays in chat. The AI answers but never proposes graph changes.";
    case "agent":
      return "Agent mode — every Send runs the agent and proposes a GraphDiff for review.";
    case "simple":
      return "Simple build — generate a whole flow in one shot (no validation); add it to the canvas with one click.";
    case "auto":
    default:
      return "Auto-agent — chat by default; the classifier auto-promotes to agent when a build intent is detected.";
  }
});

// Local can't run auto/agent (no tool-calling). If those modes were active
// when the user switches to local, fall back to chat so the dropdown shows a
// valid selection. Watcher (not computed) so it can call the store setter.
watch(
  isLocalSelected,
  (local) => {
    if (local && (aiStore.mode === "auto" || aiStore.mode === "agent")) {
      aiStore.setMode("chat");
    }
  },
  { immediate: true },
);

const toggleSettings = (): void => {
  settingsOpen.value = !settingsOpen.value;
  if (settingsOpen.value) infoOpen.value = false;
};

const toggleInfo = (): void => {
  infoOpen.value = !infoOpen.value;
  if (infoOpen.value) settingsOpen.value = false;
};

// Outside-click + Esc dismiss for both popovers. Each branch checks
// containment against its own anchor wrapper so a click on the gear
// button doesn't immediately close the settings popover it just opened.
const handleDocumentClick = (event: MouseEvent): void => {
  const target = event.target as Node | null;
  if (target === null) return;
  if (settingsOpen.value) {
    const wrapper = settingsAnchorRef.value;
    if (!wrapper || !wrapper.contains(target)) {
      settingsOpen.value = false;
    }
  }
  if (infoOpen.value) {
    const wrapper = infoAnchorRef.value;
    if (!wrapper || !wrapper.contains(target)) {
      infoOpen.value = false;
    }
  }
};

const handleDocumentKeydown = (event: KeyboardEvent): void => {
  if (event.key !== "Escape") return;
  if (settingsOpen.value) settingsOpen.value = false;
  if (infoOpen.value) infoOpen.value = false;
};

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

// Empty-state suggestion chips. Click fills the composer (no auto-send)
// so the user can edit / confirm before dispatching.
const suggestionChips: ReadonlyArray<string> = [
  "Show columns with nulls in the current dataset",
  "Group by city and count unique customers",
  "Join two CSVs on email and dedupe",
];

const handleSuggestionPick = (text: string): void => {
  composerText.value = text;
  void nextTick(() => {
    composerTextarea.value?.focus();
  });
};

// Navigate to the AI Providers tab in Connections so users can add /
// edit credentials. Closes the popover first to avoid a flash of the
// open menu during the route transition.
const handleManageProviders = (): void => {
  settingsOpen.value = false;
  void router.push({ name: "connections", query: { tab: "ai" } });
};

// --- One-click local-model onboarding (shown when no provider is configured) ---
// Size of the recommended (selected default) model, e.g. "2.0 GB", for the CTA.
const localSetupSizeLabel = computed<string>(() => {
  const st = aiStore.localModelStatus;
  const rec = st?.models.find((m) => m.id === st?.selectedModelId) ?? st?.models[0];
  const mb = rec?.approxDownloadMb;
  if (!mb || mb <= 0) return "small download";
  return mb >= 1000 ? `${(mb / 1000).toFixed(1)} GB` : `${mb} MB`;
});

const localSetupProgressLabel = computed<string>(() => {
  switch (aiStore.localSetupPhase) {
    case "downloading_binary":
      return "Downloading runtime…";
    case "extracting":
      return "Extracting…";
    case "downloading_model":
      return `Downloading model (~${localSetupSizeLabel.value})…`;
    case "verifying":
      return "Verifying…";
    case "done":
      return "Finishing…";
    default:
      return "Setting up…";
  }
});

const handleSetupLocal = (): void => {
  void aiStore.setupLocalModel();
};

// Terminal-style ↑/↓ prompt-history recall. Reads from the persisted
// chat (already capped at 200 messages per flow in localStorage); no
// new persistence layer. Recall mode is entered when the composer is
// empty and the user presses ↑; any real keystroke exits it.
const recallIndex = ref<number | null>(null);
const recallAnchor = ref<string>("");

const userPromptHistory = computed<string[]>(() => {
  const out: string[] = [];
  for (let i = aiStore.messages.length - 1; i >= 0; i--) {
    const m = aiStore.messages[i];
    if (m.role !== "user") continue;
    // Strip [Agent] / [Lineage] prefixes — handleSend re-adds the
    // appropriate one based on the current mode.
    out.push(m.content.replace(/^\[(Agent|Lineage)\]\s*/, ""));
  }
  return out;
});

const hasHistory = computed(() => userPromptHistory.value.length > 0);

const moveCursorToEnd = (): void => {
  void nextTick(() => {
    const el = composerTextarea.value;
    if (!el) return;
    const len = el.value.length;
    el.setSelectionRange(len, len);
  });
};

const exitRecallMode = (): void => {
  recallIndex.value = null;
  recallAnchor.value = "";
};

// direction: +1 = older, -1 = newer. Returns true if the keystroke was
// handled (caller should preventDefault).
const tryRecall = (direction: 1 | -1): boolean => {
  const history = userPromptHistory.value;
  if (history.length === 0) return false;

  if (recallIndex.value === null) {
    // Entering recall mode — only when going back (↑) and composer is empty.
    if (direction !== 1 || composerText.value !== "") return false;
    recallAnchor.value = composerText.value;
    recallIndex.value = 0;
    composerText.value = history[0];
    moveCursorToEnd();
    return true;
  }

  const next = recallIndex.value + direction;
  if (next < 0) {
    // ↓ past the most recent — exit recall mode, restore anchor.
    recallIndex.value = null;
    composerText.value = recallAnchor.value;
    moveCursorToEnd();
    return true;
  }
  if (next >= history.length) {
    // ↑ past the oldest — stay put, but consume the keystroke.
    return true;
  }
  recallIndex.value = next;
  composerText.value = history[next];
  moveCursorToEnd();
  return true;
};

// Cycle order matches the dropdown. On-device AI can't tool-call, so it only
// toggles Chat ↔ Simple build; cloud providers cycle Chat → Auto-agent → Agent.
const _MODE_CYCLE: ReadonlyArray<AiMode> = ["chat", "auto", "agent"];
const _LOCAL_MODE_CYCLE: ReadonlyArray<AiMode> = ["chat", "simple"];

const cycleMode = (): void => {
  if (aiStore.isStreaming || isAgentRunning.value) return;
  const cycle = isLocalSelected.value ? _LOCAL_MODE_CYCLE : _MODE_CYCLE;
  const idx = cycle.indexOf(aiStore.mode);
  const next = cycle[(idx + 1) % cycle.length];
  aiStore.setMode(next);
};

// Platform-aware modifier label for the keyboard hints. ⌘ on Mac, Ctrl
// elsewhere. `event.metaKey` covers ⌘ on Mac and the Windows key on
// other platforms; we also accept `ctrlKey` so the binding works for
// the user community that habitually reaches for Ctrl regardless.
const isMacPlatform =
  typeof navigator !== "undefined" && /Mac|iPhone|iPad|iPod/.test(navigator.platform);
const modKeyLabel = isMacPlatform ? "⌘" : "Ctrl";

const handleComposerKeydown = (event: KeyboardEvent): void => {
  // Let the mention autocomplete intercept Up/Down/Enter/Tab/Escape
  // before the composer's own Enter-to-send guard fires.
  if (mention.onKeyDown(event)) return;

  // ⌘/Ctrl + Shift + M — cycle AI mode (Chat → Auto-agent → Agent → Chat).
  // `event.code === "KeyM"` is layout-independent so the binding works on
  // non-QWERTY keyboards too.
  if ((event.metaKey || event.ctrlKey) && event.shiftKey && event.code === "KeyM") {
    event.preventDefault();
    cycleMode();
    return;
  }

  // ↑/↓ — terminal-style prompt history. Skip when modifier keys are held
  // so OS / text-selection shortcuts (Shift+↑, Cmd+↑, Alt+↑) still work.
  if (event.key === "ArrowUp" && !event.shiftKey && !event.altKey && !event.metaKey) {
    if (tryRecall(1)) {
      event.preventDefault();
      return;
    }
  }
  if (event.key === "ArrowDown" && !event.shiftKey && !event.altKey && !event.metaKey) {
    if (tryRecall(-1)) {
      event.preventDefault();
      return;
    }
  }

  // Enter sends, Shift+Enter inserts a newline. Mirrors most chat UIs.
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    void handleSend();
  }
};

const handleComposerInput = (): void => {
  mention.onInput();
  // Real user input exits recall mode. Programmatic updates from
  // tryRecall() don't fire input events, so this only flips on actual
  // typing/editing.
  if (recallIndex.value !== null) {
    exitRecallMode();
  }
};

const kbdHint = computed<string>(() => {
  if (aiStore.isStreaming || isAgentRunning.value) return "";
  if (composerText.value.length > 0) return "↵ to send · ⇧↵ for newline";
  if (hasHistory.value) return "↑ to recall · ↵ to send";
  return "";
});

const handleMentionPick = (candidate: Parameters<typeof mention.pick>[0]): void => {
  mention.pick(candidate);
};

const handleMentionDismiss = (): void => {
  mention.close();
};

const handleMentionHover = (index: number): void => {
  mention.setActiveIndex(index);
};

// Render the drift banner's per-id rows with typed labels:
// "Filter node 6 was deleted" / "Manual-input node 3 was added
// externally". Falls back to "Node {id}" when the snapshot didn't
// capture a type.
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
    // Filter meta routing + housekeeping info events out of the chat
    // trail before grouping, so a streaming run that produces nothing
    // but pick_category + "category narrowed" doesn't render as an
    // empty agent bubble between user/assistant messages.
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

// Show the standalone thinking placeholder only when the agent is
// running AND the most recent timeline item is not yet an agent_run —
// i.e. the user just sent the prompt but no events have arrived. Once
// the first event lands, AiAgentRun gets `is-running` and renders the
// dots inside its own card, so we hide this one to avoid two indicators.
const showStandaloneThinking = computed<boolean>(() => {
  if (!isAgentRunning.value) return false;
  const items = timelineItems.value;
  const last = items[items.length - 1];
  return !last || last.kind !== "agent_run";
});
</script>

<template>
  <div class="ai-assistant">
    <header class="ai-assistant__header">
      <!-- Info popover — surfaces use-case examples and keyboard tips.
           Anchored to the circle-info button on the left of the
           toolbar; mutually exclusive with the settings popover. -->
      <div ref="infoAnchorRef" class="ai-assistant__info-wrapper">
        <button
          v-if="!isDisabledByFlag"
          type="button"
          class="ai-assistant__info-btn"
          :class="{ 'is-open': infoOpen }"
          aria-haspopup="dialog"
          :aria-expanded="infoOpen"
          aria-label="About the AI assistant"
          title="About the AI assistant"
          @click.stop="toggleInfo"
        >
          <i class="fa-solid fa-circle-info" aria-hidden="true"></i>
        </button>
        <div
          v-if="infoOpen"
          class="ai-assistant__info-popover"
          role="dialog"
          aria-label="AI assistant use cases"
        >
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">About</h4>
            <p class="ai-assistant__info-paragraph">
              Chat about your data and canvas, or describe a change and the assistant proposes graph
              edits for you to review.
            </p>
          </div>
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">Modes</h4>
            <!-- On-device AI can't tool-call, so its modes differ from cloud
                 providers (Chat / Simple build, no Auto-agent or Agent). -->
            <template v-if="!isLocalSelected">
              <ul class="ai-assistant__info-list">
                <li><strong>Chat</strong> — Q&amp;A only; never modifies the canvas.</li>
                <li>
                  <strong>Auto-agent</strong> (default) — chats; auto-promotes to the agent when you
                  describe a build.
                </li>
                <li>
                  <strong>Agent</strong> — every send runs the agent and proposes graph changes.
                </li>
              </ul>
              <p class="ai-assistant__info-note">
                Agent variant (Staged / Single-shot / Live) is in <strong>Settings</strong>.
              </p>
            </template>
            <template v-else>
              <ul class="ai-assistant__info-list">
                <li><strong>Chat</strong> — Q&amp;A only; never modifies the canvas.</li>
                <li>
                  <strong>Simple build</strong> — generates a whole flow from one prompt; review and
                  add it to the canvas with one click.
                </li>
              </ul>
              <p class="ai-assistant__info-note">
                On-device AI can't run Auto-agent or Agent (no tool-calling).
              </p>
            </template>
          </div>
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">On-device AI</h4>
            <p class="ai-assistant__info-paragraph">
              On-device AI runs offline and is for simple use-cases only. For larger, more capable
              models, add a provider (e.g. Ollama or OpenRouter) in <strong>Connections</strong>.
            </p>
          </div>
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">Mention with @</h4>
            <ul class="ai-assistant__info-list">
              <li><code>@flow</code> — the whole graph</li>
              <li><code>@name</code> or <code>@id</code> — a specific node</li>
            </ul>
          </div>
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">Try asking</h4>
            <ul class="ai-assistant__info-list">
              <li>"Group by city, count unique customers"</li>
              <li>"Filter to the last 30 days"</li>
              <li>"What columns have nulls?"</li>
              <li>"Why is this filter dropping all rows?"</li>
              <li>"Explain what this formula does"</li>
            </ul>
          </div>
          <div class="ai-assistant__info-section">
            <h4 class="ai-assistant__info-heading">Tips</h4>
            <ul class="ai-assistant__info-list">
              <li><kbd>↑</kbd> in the composer recalls your last prompt.</li>
              <li v-if="!isLocalSelected">
                <kbd>{{ modKeyLabel }}</kbd> + <kbd>⇧</kbd> + <kbd>M</kbd> cycles modes (Chat →
                Auto-agent → Agent).
              </li>
              <li v-else>
                <kbd>{{ modKeyLabel }}</kbd> + <kbd>⇧</kbd> + <kbd>M</kbd> toggles modes (Chat ↔
                Simple build).
              </li>
              <li>
                Chat history is per-flow and persists across reloads. Switching flows is automatic —
                no need to clear.
              </li>
              <li>
                <strong>Clear</strong> wipes this flow's history. Use it when switching tasks so
                earlier context doesn't bleed into the next question.
              </li>
            </ul>
          </div>
        </div>
      </div>
      <!-- Provider / model / agent-variant pickers live in a popover
           behind this gear button so the header stays compact in
           steady state. The popover anchors to the wrapper below and
           dismisses on outside click + Esc. -->
      <div ref="settingsAnchorRef" class="ai-assistant__settings-wrapper">
        <button
          v-if="!isDisabledByFlag"
          type="button"
          class="ai-assistant__settings-btn"
          :class="{ 'is-open': settingsOpen }"
          :disabled="aiStore.isStreaming || isAgentRunning"
          aria-haspopup="dialog"
          :aria-expanded="settingsOpen"
          aria-label="AI settings"
          title="AI settings"
          @click.stop="toggleSettings"
        >
          <span class="material-icons" aria-hidden="true">settings</span>
        </button>
        <div
          v-if="settingsOpen"
          class="ai-assistant__settings-popover"
          role="dialog"
          aria-label="AI settings"
        >
          <div v-if="aiStore.providers.length > 0" class="ai-assistant__settings-section">
            <label class="ai-assistant__settings-label" for="ai-settings-provider">
              Provider
            </label>
            <select
              id="ai-settings-provider"
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
                {{ providerLabel(provider.provider) }}
                <template v-if="provider.status === 'env_fallback'"> (env)</template>
                <template v-else-if="provider.status === 'unconfigured'">
                  (not configured)
                </template>
              </option>
            </select>
          </div>
          <div v-if="showModelPicker" class="ai-assistant__settings-section">
            <label class="ai-assistant__settings-label" for="ai-settings-model">Model</label>
            <select
              id="ai-settings-model"
              class="ai-assistant__select"
              :value="aiStore.selectedModel ?? ''"
              :disabled="aiStore.isStreaming || isAgentRunning"
              :title="aiStore.selectedModel ?? 'Pick a model'"
              @change="handleModelChange"
            >
              <option v-for="model in availableModels" :key="model" :value="model">
                {{ model }}
              </option>
            </select>
            <p v-if="isLocalSelected" class="ai-assistant__settings-hint">
              Best for simple tasks. For more capable models, use a cloud provider (e.g. Ollama or
              OpenRouter).
            </p>
          </div>
          <!-- Agent variant + verification are agent-only — hidden for the
               local model, which can't tool-call (its modes are Chat /
               Simple build). -->
          <div v-if="!isLocalSelected" class="ai-assistant__settings-section">
            <span class="ai-assistant__settings-label">Agent variant</span>
            <p class="ai-assistant__settings-hint">
              How the agent plans and executes changes. Used when Send dispatches an agent run
              (Auto-agent or Agent mode).
            </p>
            <label class="ai-assistant__settings-radio">
              <input
                type="radio"
                name="ai-agent-surface"
                value="agent_staged"
                :checked="aiStore.selectedAgentSurface === 'agent_staged'"
                :disabled="aiStore.isStreaming || isAgentRunning"
                @change="handleAgentSurfaceChange"
              />
              <span class="ai-assistant__settings-radio-body">
                <span class="ai-assistant__settings-radio-name">Staged</span>
                <span class="ai-assistant__settings-radio-desc">
                  Multi-stage planner. Reviews each step before staging. Reliable on small / local
                  models.
                </span>
              </span>
            </label>
            <label class="ai-assistant__settings-radio">
              <input
                type="radio"
                name="ai-agent-surface"
                value="agent_complex"
                :checked="aiStore.selectedAgentSurface === 'agent_complex'"
                :disabled="aiStore.isStreaming || isAgentRunning"
                @change="handleAgentSurfaceChange"
              />
              <span class="ai-assistant__settings-radio-body">
                <span class="ai-assistant__settings-radio-name">Single-shot full</span>
                <span class="ai-assistant__settings-radio-desc">
                  Exposes the entire tool catalog at once. Best for large models that handle complex
                  prompts well.
                </span>
              </span>
            </label>
            <label class="ai-assistant__settings-radio">
              <input
                type="radio"
                name="ai-agent-surface"
                value="agent_live"
                :checked="aiStore.selectedAgentSurface === 'agent_live'"
                :disabled="aiStore.isStreaming || isAgentRunning"
                @change="handleAgentSurfaceChange"
              />
              <span class="ai-assistant__settings-radio-body">
                <span class="ai-assistant__settings-radio-name">Live (REPL)</span>
                <span class="ai-assistant__settings-radio-desc">
                  Applies each step live to the canvas, runs the affected subgraph, retries on
                  failure. Every step commits immediately, no staged diff.
                </span>
              </span>
            </label>
            <!-- Divider so the verify-completion gate doesn't read as a
                 fourth Agent variant. Agent variant chooses *how* the
                 agent runs; verify completion is an orthogonal post-run
                 check that applies regardless of variant. -->
            <hr class="ai-assistant__settings-divider" aria-hidden="true" />
            <span class="ai-assistant__settings-label">Verification</span>
            <!-- Opt-in verify-completion gate. After classify picks
                 op_kind="other" (intending to terminate), the agent
                 runs one extra LLM round to walk the plan as a
                 checklist. Catches the case where a multi-step plan
                 terminates after step 1. -->
            <label class="ai-assistant__settings-radio">
              <input
                type="checkbox"
                name="ai-agent-verify-plan"
                :checked="aiStore.verifyPlanCompletion"
                :disabled="aiStore.isStreaming || isAgentRunning"
                @change="handleVerifyPlanCompletionChange"
              />
              <span class="ai-assistant__settings-radio-body">
                <span class="ai-assistant__settings-radio-name">Verify plan completion</span>
                <span class="ai-assistant__settings-radio-desc">
                  After the agent finishes, double-check that every plan step was applied. Adds one
                  LLM round per agent run. Useful when multi-step plans (e.g. add a node mid-flow
                  plus the rewires) sometimes terminate after step 1.
                </span>
              </span>
            </label>
          </div>
          <!-- Footer link: navigates to Connections → AI Providers
               where users add API keys, configure model overrides,
               and test credentials. -->
          <div class="ai-assistant__settings-footer">
            <button
              type="button"
              class="ai-assistant__settings-link"
              @click="handleManageProviders"
            >
              Manage providers in Connections
              <span class="material-icons" aria-hidden="true">arrow_forward</span>
            </button>
          </div>
        </div>
      </div>
      <button
        v-if="aiStore.messages.length > 0 || agentStore.events.length > 0"
        type="button"
        class="ai-assistant__clear"
        :disabled="aiStore.isStreaming || isAgentRunning"
        aria-label="Clear conversation"
        title="Clear conversation"
        @click="handleClear"
      >
        <span class="material-icons" aria-hidden="true">delete_outline</span>
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

      <!-- Fastest path for a brand-new user: run fully offline, no API key.
           Shown only when the platform supports a local model and none is
           installed yet. One click downloads the recommended model and selects
           it, so the next message just works. -->
      <div v-if="aiStore.canSetupLocal" class="ai-assistant__local-setup">
        <button
          type="button"
          class="ai-assistant__local-setup-btn"
          :disabled="aiStore.localSetupInProgress"
          @click="handleSetupLocal"
        >
          <i class="fa-solid fa-download"></i>
          <span>{{
            aiStore.localSetupInProgress
              ? localSetupProgressLabel
              : `Set up On-device AI (~${localSetupSizeLabel})`
          }}</span>
        </button>
        <div v-if="aiStore.localSetupInProgress" class="ai-assistant__local-setup-track">
          <div
            class="ai-assistant__local-setup-bar"
            :class="{ 'is-indeterminate': aiStore.localSetupPct === null }"
            :style="
              aiStore.localSetupPct !== null ? { width: aiStore.localSetupPct + '%' } : undefined
            "
          ></div>
        </div>
        <p class="ai-assistant__notice-hint">
          Runs on your machine, no API key — best for simple tasks. For more capable models, add a
          provider (e.g. Ollama or OpenRouter).
        </p>
      </div>

      <p class="ai-assistant__notice-hint">
        {{ aiStore.canSetupLocal ? "Or open" : "Open" }}
        <button type="button" class="ai-assistant__notice-link" @click="handleManageProviders">
          Connections → AI Providers
        </button>
        and add an API key (or run a local Ollama server) to start chatting.
      </p>
    </div>

    <div v-else class="ai-assistant__chat">
      <AiDiffPanel />
      <!-- Promotion banner intentionally hidden. The
           auto-promote-from-chat dispatch (ai-store.ts
           ``_dispatchPromotedAgent``) and the underlying
           ``promotionBanner`` / ``acceptPromotion`` /
           ``undoPromotion`` store plumbing are kept intact so the
           banner can be restored cheaply when the UX is wanted
           again. For now the chat → agent_staged hand-off is silent. -->
      <div v-if="false && aiStore.promotionBanner !== null" class="ai-assistant__promo-banner">
        <p class="ai-assistant__promo-text">
          <span class="ai-assistant__promo-icon">✨</span>
          <span> Switched to Agent mode — {{ aiStore.promotionBanner?.reason }}. </span>
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
      <!-- Post-agent_live layout-reorganize prompt. agent_live
           commits each step to the canvas live; on a multi-step run
           the new nodes can land in less-than-tidy positions, so the
           banner offers a one-click route to the existing "Reset
           layout graph" routine. Only renders when the just-finished
           run was agent_live AND at least one node was applied.
           Lives ABOVE the messages container (not inside it) so the
           auto-scroll-to-bottom in ``.ai-assistant__messages``
           doesn't push it off-screen. -->
      <div
        v-if="agentStore.liveLayoutPromptVisible"
        class="ai-assistant__layout-prompt"
        role="status"
      >
        <span class="ai-assistant__layout-prompt-text"> Reorganize the canvas layout? </span>
        <div class="ai-assistant__layout-prompt-actions">
          <button
            class="ai-assistant__layout-prompt-btn ai-assistant__layout-prompt-btn--accept"
            @click="agentStore.acceptLayoutReorganize"
          >
            Reorganize
          </button>
          <button
            class="ai-assistant__layout-prompt-btn"
            @click="agentStore.dismissLayoutReorganize"
          >
            Dismiss
          </button>
        </div>
      </div>
      <!-- Always-on surface chip. Renders for every active agent run
           regardless of surface so the operator can verify at a
           glance which agent (agent_complex / agent_staged /
           agent_live) is actually executing. -->
      <div
        v-if="agentStore.status === 'running' && agentStore.currentSurface"
        class="ai-assistant__surface-chip"
        :class="{
          'ai-assistant__surface-chip--legacy':
            agentStore.currentSurface === 'agent' || agentStore.currentSurface === 'agent_complex',
          'ai-assistant__surface-chip--live': agentStore.currentSurface === 'agent_live',
        }"
        role="status"
        aria-label="Active agent surface"
      >
        <span class="ai-assistant__surface-chip-label">surface</span>
        <span class="ai-assistant__surface-chip-name">
          {{ agentStore.currentSurface }}
        </span>
      </div>
      <!-- Agent_staged stage badge. Renders for both staged and live
           surfaces (they share the same 4-stage state machine through
           fill_settings); ``agent_complex`` shows nothing — the
           badge has no meaning there. -->
      <div
        v-if="
          agentStore.status === 'running' &&
          (agentStore.currentSurface === 'agent_staged' ||
            agentStore.currentSurface === 'agent_live')
        "
        class="ai-assistant__stage-badge"
        role="status"
        aria-live="polite"
      >
        <span class="ai-assistant__stage-step">{{ stageStepLabel }}</span>
        <span class="ai-assistant__stage-name">{{ stageReadableLabel }}</span>
        <span v-if="agentStore.pickedNodeType" class="ai-assistant__stage-detail">
          ({{ agentStore.pickedNodeType }})
        </span>
        <span
          v-else-if="agentStore.pickedOpKind && agentStore.pickedOpKind !== 'add'"
          class="ai-assistant__stage-detail"
        >
          ({{ agentStore.pickedOpKind }})
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
          <AiAvatar size="lg" />
          <h3 class="ai-assistant__empty-heading">What can I help you with?</h3>
          <p class="ai-assistant__empty-subtitle">
            <template v-if="aiStore.mode === 'agent'">
              Agent mode — describe a goal and I'll propose the steps for review.
            </template>
            <template v-else-if="aiStore.mode === 'chat'">
              Chat mode — I'll answer but won't change the graph.
            </template>
            <template v-else>
              Ask about your data or describe a transformation. I'll auto-promote to agent when you
              describe a build.
            </template>
          </p>
          <div class="ai-assistant__suggestions">
            <button
              v-for="chip in suggestionChips"
              :key="chip"
              type="button"
              class="ai-assistant__suggestion-chip"
              @click="handleSuggestionPick(chip)"
            >
              {{ chip }}
            </button>
          </div>
        </div>
        <template v-for="(item, idx) in timelineItems" :key="`${item.kind}-${item.at}-${idx}`">
          <AiMessage
            v-if="item.kind === 'message'"
            :message="item.data"
            @add-build="handleAddBuild"
          />
          <AiAgentRun
            v-else
            :events="item.events"
            :is-running="isAgentRunning && idx === timelineItems.length - 1"
          />
        </template>
        <!-- Standalone thinking bubble — shown only while the agent is
             running AND no agent_run card has appeared yet (i.e. before
             the first event arrives). Once events stream in, the
             dots move *inside* the active run's card so the user
             doesn't see two separate "the agent is busy" indicators. -->
        <div v-if="showStandaloneThinking" class="ai-assistant__thinking">
          <AiAvatar size="md" />
          <span class="ai-assistant__thinking-role">Agent</span>
          <AiThinkingDots label="Agent is thinking" />
        </div>
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
        <!-- Unified send-mode dropdown. Local hides Auto-agent/Agent (the local
             model can't tool-call); both surfaces get Simple build. -->
        <select
          class="ai-assistant__mode-select"
          :value="aiStore.mode"
          :disabled="isAgentRunning || aiStore.isStreaming"
          :title="modeTooltip"
          @change="handleModeChange"
        >
          <option value="chat">Chat</option>
          <option v-if="!isLocalSelected" value="auto">Auto-agent</option>
          <option v-if="!isLocalSelected" value="agent">Agent</option>
          <option value="simple">Simple build</option>
        </select>
        <span v-if="kbdHint" class="ai-assistant__kbd-hint" aria-hidden="true">
          {{ kbdHint }}
        </span>
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
          {{ aiStore.mode === "simple" ? "Build" : "Send" }}
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

/* Toolbar — the parent DraggableItem already provides the title/minimize
   bar, so this row is purely a settings + clear toolbar. Right-aligned
   icons that fade in on hover keep the chrome quiet. */
.ai-assistant__header {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 4px;
  padding-bottom: 4px;
  min-width: 0;
}

.ai-assistant__select {
  flex: 1 1 0;
  min-width: 0;
  padding: 4px 6px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 11px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__select:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* Icon-only toolbar buttons (Clear, Settings). Transparent until hover
   so the chrome doesn't compete with chat content. */
.ai-assistant__clear {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  padding: 0;
  border-radius: 6px;
  border: 1px solid transparent;
  background-color: transparent;
  color: var(--color-text-tertiary, #a0aec0);
  cursor: pointer;
  transition:
    background-color var(--transition-fast, 120ms ease),
    border-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease);
}

.ai-assistant__clear:hover:enabled {
  background-color: var(--color-background-secondary, #f8f9fa);
  border-color: var(--color-border-primary, #e2e8f0);
  color: var(--color-text-primary, #1a1a2e);
}

.ai-assistant__clear:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__clear .material-icons {
  font-size: 16px;
  line-height: 1;
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
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: 32px 8px 16px;
  gap: 10px;
}

.ai-assistant__empty-heading {
  margin: 4px 0 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
  line-height: 1.3;
}

.ai-assistant__empty-subtitle {
  margin: 0 0 8px;
  font-size: 12px;
  color: var(--color-text-tertiary, #718096);
  line-height: 1.5;
  max-width: 320px;
}

.ai-assistant__suggestions {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
  max-width: 360px;
  margin-top: 4px;
}

.ai-assistant__suggestion-chip {
  padding: 8px 12px;
  border-radius: 8px;
  border: 1px solid var(--color-border-primary, #e2e8f0);
  background-color: var(--color-background-primary, #ffffff);
  font-size: 12px;
  color: var(--color-text-secondary, #4a5568);
  text-align: left;
  cursor: pointer;
  transition:
    background-color var(--transition-fast, 120ms ease),
    border-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease);
  font-family: inherit;
  line-height: 1.4;
}

.ai-assistant__suggestion-chip:hover {
  border-color: var(--color-accent-purple, #667eea);
  background-color: var(--color-background-hover, #f0f7ff);
  color: var(--color-text-primary, #1a1a2e);
}

/* Agent thinking placeholder — minimal bubble at the end of the
   message list. Matches the agent-card surface but doesn't carry the
   full padding / shadow weight, so it reads as "transient state". */
.ai-assistant__thinking {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  margin-top: 4px;
  border-radius: 10px;
  background-color: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
}

.ai-assistant__thinking-role {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1;
  margin-right: 2px;
}

/* agent_live post-run layout-reorganize prompt. Sticks to the top of
   the chat trail so the user sees it the moment the run completes;
   auto-clears on the next session start (handled in the store) or
   on dismiss / accept. */
.ai-assistant__layout-prompt {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  margin-bottom: 8px;
  border-radius: 8px;
  background-color: var(--color-accent-soft, rgba(124, 58, 237, 0.08));
  border: 1px solid var(--color-accent, #7c3aed);
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__layout-prompt-text {
  flex: 1 1 auto;
}

.ai-assistant__layout-prompt-actions {
  display: flex;
  gap: 6px;
  flex: 0 0 auto;
}

.ai-assistant__layout-prompt-btn {
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background, #ffffff);
  color: var(--color-text-primary, #24292e);
  font-size: 12px;
  cursor: pointer;
}

.ai-assistant__layout-prompt-btn:hover {
  background-color: var(--color-background-secondary, #f6f8fa);
}

.ai-assistant__layout-prompt-btn--accept {
  background-color: var(--color-accent, #7c3aed);
  color: #ffffff;
  border-color: var(--color-accent, #7c3aed);
}

.ai-assistant__layout-prompt-btn--accept:hover {
  filter: brightness(0.95);
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

/* Inline one-click local-AI onboarding card inside the no-provider notice. */
.ai-assistant__local-setup {
  margin: 10px 0;
}

.ai-assistant__local-setup-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 14px;
  border: none;
  border-radius: 6px;
  background-color: var(--color-accent, #6b4eff);
  color: #fff;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}

.ai-assistant__local-setup-btn:hover:not(:disabled) {
  filter: brightness(1.05);
}

.ai-assistant__local-setup-btn:disabled {
  opacity: 0.7;
  cursor: progress;
}

.ai-assistant__local-setup-track {
  margin-top: 8px;
  height: 6px;
  background-color: var(--color-background-muted, #e1e4e8);
  border-radius: 999px;
  overflow: hidden;
}

.ai-assistant__local-setup-bar {
  height: 100%;
  background-color: var(--color-accent, #6b4eff);
  border-radius: 999px;
  transition: width 0.2s ease;
}

.ai-assistant__local-setup-bar.is-indeterminate {
  width: 40%;
  animation: ai-local-setup-slide 1.2s ease-in-out infinite;
}

@keyframes ai-local-setup-slide {
  0% {
    margin-left: -40%;
  }
  100% {
    margin-left: 100%;
  }
}

.ai-assistant__notice-link {
  background: none;
  border: none;
  padding: 0;
  font: inherit;
  color: var(--color-primary, #2563eb);
  font-weight: 600;
  cursor: pointer;
  text-decoration: underline;
}

.ai-assistant__notice-link:hover,
.ai-assistant__notice-link:focus-visible {
  color: var(--color-primary-hover, #1d4ed8);
  outline: none;
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
  padding: 8px 10px;
  border-radius: 8px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-family: inherit;
  font-size: 13px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
  box-sizing: border-box;
  transition:
    border-color var(--transition-fast, 120ms ease),
    box-shadow var(--transition-fast, 120ms ease);
}

/* Single soft purple ring on focus — the only colour cue, no animation. */
.ai-assistant__textarea:focus {
  outline: none;
  border-color: var(--color-accent-purple, #667eea);
  box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.12);
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

.ai-assistant__mode-select {
  flex-shrink: 0;
  padding: 4px 6px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
  font-size: 11px;
  font-weight: 500;
  cursor: pointer;
  min-width: 100px;
}

.ai-assistant__mode-select:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* Conditional teaching cue — only shown when there's text to send.
   Sits between the mode dropdown and Send so it reads as "press this
   to do that". */
.ai-assistant__kbd-hint {
  flex: 1 1 auto;
  text-align: right;
  font-size: 10px;
  color: var(--color-text-tertiary, #a0aec0);
  user-select: none;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.ai-assistant__btn {
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
  transition:
    background var(--transition-fast, 120ms ease),
    box-shadow var(--transition-fast, 120ms ease),
    filter var(--transition-fast, 120ms ease);
}

.ai-assistant__btn--primary {
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #667eea) 0%,
    var(--color-gradient-purple-end, #764ba2) 100%
  );
  color: var(--color-text-inverse, #ffffff);
}

.ai-assistant__btn--primary:hover:enabled {
  filter: brightness(0.95);
  box-shadow: 0 1px 3px rgba(102, 126, 234, 0.3);
}

.ai-assistant__btn--primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__btn--danger {
  background-color: var(--color-danger, #c53030);
  color: var(--color-text-inverse, #ffffff);
}

/* Info popover (circle-info button → use cases + tips). The whole
   toolbar cluster is right-aligned, so anchoring `right: 0` makes the
   popover extend leftward into the drawer body instead of off the
   right edge. Same posture as the settings popover. */
.ai-assistant__info-wrapper {
  position: relative;
  flex-shrink: 0;
}

.ai-assistant__info-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  padding: 0;
  border-radius: 6px;
  border: 1px solid transparent;
  background-color: transparent;
  color: var(--color-text-tertiary, #a0aec0);
  cursor: pointer;
  font-size: 14px;
  line-height: 1;
  transition:
    background-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease),
    border-color var(--transition-fast, 120ms ease);
}

.ai-assistant__info-btn:hover {
  background-color: var(--color-background-secondary, #f8f9fa);
  border-color: var(--color-border-primary, #e2e8f0);
  color: var(--color-text-primary, #1a1a2e);
}

.ai-assistant__info-btn.is-open {
  border-color: var(--color-accent-purple, #667eea);
  color: var(--color-accent-purple, #667eea);
  background-color: var(--color-background-primary, #ffffff);
}

.ai-assistant__info-popover {
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  z-index: 50;
  min-width: 360px;
  max-width: 420px;
  padding: 14px;
  border-radius: 8px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-primary, #ffffff);
  box-shadow: var(--shadow-md, 0 4px 12px rgba(0, 0, 0, 0.12));
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.ai-assistant__info-section {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.ai-assistant__info-heading {
  margin: 0;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--color-text-tertiary, #6b7280);
}

.ai-assistant__info-paragraph {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-primary, #24292e);
  line-height: 1.5;
}

.ai-assistant__info-note {
  margin: 4px 0 0;
  font-size: 11px;
  color: var(--color-text-tertiary, #a0aec0);
  line-height: 1.4;
  font-style: italic;
}

.ai-assistant__info-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.ai-assistant__info-list li {
  font-size: 12px;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1.5;
}

.ai-assistant__info-list code {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 11px;
  padding: 1px 5px;
  border-radius: 4px;
  background-color: var(--color-background-tertiary, #f1f3f5);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__info-list kbd {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 11px;
  padding: 1px 5px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary, #e2e8f0);
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
  box-shadow: 0 1px 0 var(--color-border-primary, #e2e8f0);
}

/* Settings popover (gear button → provider / model / agent-variant).
   Anchors to the wrapper in the header so it floats below the gear
   without affecting layout flow. */
.ai-assistant__settings-wrapper {
  position: relative;
  flex-shrink: 0;
}

.ai-assistant__settings-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  padding: 0;
  border-radius: 6px;
  border: 1px solid transparent;
  background-color: transparent;
  color: var(--color-text-tertiary, #a0aec0);
  cursor: pointer;
  transition:
    background-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease),
    border-color var(--transition-fast, 120ms ease);
}

.ai-assistant__settings-btn:hover:enabled {
  background-color: var(--color-background-secondary, #f8f9fa);
  border-color: var(--color-border-primary, #e2e8f0);
  color: var(--color-text-primary, #1a1a2e);
}

.ai-assistant__settings-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__settings-btn.is-open {
  border-color: var(--color-accent-purple, #667eea);
  color: var(--color-accent-purple, #667eea);
  background-color: var(--color-background-primary, #ffffff);
}

.ai-assistant__settings-btn .material-icons {
  font-size: 16px;
  line-height: 1;
}

/* Right-aligned popover anchored to the gear so it doesn't drift off
   the drawer's right edge in narrow widths. ~290px gives the radio
   descriptions room to breathe at small sizes. */
.ai-assistant__settings-popover {
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  z-index: 50;
  min-width: 420px;
  max-width: 460px;
  padding: 14px;
  border-radius: 8px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-primary, #ffffff);
  box-shadow: var(--shadow-md, 0 4px 12px rgba(0, 0, 0, 0.12));
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.ai-assistant__settings-section {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.ai-assistant__settings-label {
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--color-text-tertiary, #6b7280);
}

.ai-assistant__settings-hint {
  margin: 0 0 2px;
  font-size: 11px;
  color: var(--color-text-tertiary, #6b7280);
  line-height: 1.4;
}

.ai-assistant__settings-radio {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 6px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background-color var(--transition-fast, 120ms ease);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__settings-radio:hover {
  background-color: var(--color-background-secondary, #f8f9fa);
}

.ai-assistant__settings-radio input[type="radio"] {
  flex-shrink: 0;
  cursor: pointer;
  margin-top: 2px;
}

.ai-assistant__settings-radio:has(input:disabled) {
  opacity: 0.6;
  cursor: not-allowed;
}

.ai-assistant__settings-radio-body {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.ai-assistant__settings-radio-name {
  font-size: 12px;
  font-weight: 600;
  color: var(--color-text-primary, #24292e);
  line-height: 1.3;
}

.ai-assistant__settings-radio-desc {
  font-size: 11px;
  color: var(--color-text-tertiary, #6b7280);
  line-height: 1.4;
}

.ai-assistant__settings-divider {
  border: 0;
  border-top: 1px solid var(--color-border-primary, #e2e8f0);
  margin: 4px 0 2px;
}

/* Footer link to the full AI Providers admin page in Connections.
   Looks like a quiet link, not a button — discoverable but not
   competing with the inline controls above. */
.ai-assistant__settings-footer {
  display: flex;
  flex-direction: column;
  padding-top: 4px;
  margin-top: 2px;
  border-top: 1px solid var(--color-border-primary, #e2e8f0);
}

.ai-assistant__settings-link {
  display: inline-flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  padding: 8px 6px 4px;
  border: none;
  background: transparent;
  color: var(--color-accent-purple, #667eea);
  font-size: 12px;
  font-weight: 500;
  font-family: inherit;
  cursor: pointer;
  text-align: left;
  transition: color var(--transition-fast, 120ms ease);
}

.ai-assistant__settings-link:hover {
  color: var(--color-accent-purple-hover, #4f5fcc);
}

.ai-assistant__settings-link .material-icons {
  font-size: 14px;
  line-height: 1;
}

/* Drift banner + event list classes follow below. */

/* Surface chip. Always renders during an active agent run so
   operators can verify which agent variant is in use. Default accent
   posture matches the stage badge; ``agent_complex`` gets a muted
   warning tint so a stray full-catalog dispatch stands out visually. */
.ai-assistant__surface-chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 8px;
  margin: 0 0 4px 0;
  align-self: flex-start;
  background-color: var(--color-accent-soft, rgba(111, 66, 193, 0.06));
  border: 1px solid var(--color-accent, #6f42c1);
  border-radius: 10px;
  font-size: 10px;
  font-weight: 500;
  color: var(--color-accent, #6f42c1);
  width: fit-content;
}

.ai-assistant__surface-chip--legacy {
  background-color: var(--color-warning-soft, rgba(204, 119, 0, 0.08));
  border-color: var(--color-warning, #cc7700);
  color: var(--color-warning, #cc7700);
}

/* Live surface chip: distinct accent so the user immediately sees
   the agent is mutating the canvas live (not bundling for review).
   Mid-saturation amethyst — not a warning colour, but visually
   different from the default staged chip. */
.ai-assistant__surface-chip--live {
  background-color: var(--color-accent-soft, rgba(124, 58, 237, 0.1));
  border-color: var(--color-accent, #7c3aed);
  color: var(--color-accent, #7c3aed);
}

.ai-assistant__surface-chip-label {
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  opacity: 0.75;
}

.ai-assistant__surface-chip-name {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-weight: 600;
}

/* Staged-agent stage badge. Compact pill that lives between the
   awaiting banner and the drift banner; only renders when the active
   run is on the agent_staged surface. Visual posture matches the
   rest of the agent surface (accent border, soft secondary
   background). */
.ai-assistant__stage-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  margin: 0 0 8px 0;
  align-self: flex-start;
  background-color: var(--color-accent-soft, rgba(111, 66, 193, 0.08));
  border: 1px solid var(--color-accent, #6f42c1);
  border-radius: 12px;
  font-size: 11px;
  font-weight: 500;
  color: var(--color-accent, #6f42c1);
  width: fit-content;
}

.ai-assistant__stage-step {
  font-weight: 700;
  letter-spacing: 0.3px;
  text-transform: uppercase;
  font-size: 10px;
}

.ai-assistant__stage-name {
  font-style: italic;
}

.ai-assistant__stage-detail {
  color: var(--color-text-secondary, #57606a);
  font-size: 10px;
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

/* Chat → agent auto-promotion banner. Sits above the messages and
   below the diff panel; visually distinct from the drift banner so
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
