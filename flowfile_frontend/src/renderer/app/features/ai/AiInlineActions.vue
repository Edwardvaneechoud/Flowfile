<script setup lang="ts">
// AI Inline Actions menu (W21). Rendered inside an <el-popover> from
// genericNodeSettings.vue's header. Each menu item triggers a single-shot
// streaming AI call (Explain / Optimise / Document / Regenerate code /
// Suggest filters) and pipes the response into the W20 chat drawer.
//
// Read-only by construction — the backend passes ``tools=None`` to the
// provider's ``stream()`` so no graph mutation can happen here. The
// "Regenerate code" item emits a code snippet to the drawer for the user
// to copy-paste manually; direct application is W31 / W41 / W35 territory.

import { computed } from "vue";
import { MagicStick } from "@element-plus/icons-vue";

import { useAiStore } from "../../stores/ai-store";
import { useFlowStore } from "../../stores/flow-store";
import type { InlineActionType } from "../../api/ai.api";

const props = defineProps<{
  flowId: number;
  nodeId: number;
  // Optional human label used in the synthetic chat user message. Falls
  // back to "node {id}" inside the store when omitted.
  nodeName?: string;
}>();

const emit = defineEmits<{
  (e: "picked"): void;
}>();

const aiStore = useAiStore();
const flowStore = useFlowStore();

interface ActionItem {
  action: InlineActionType;
  label: string;
  description: string;
  // When set, the item is hidden unless the resolved node_type is in this set.
  requiresNodeType?: ReadonlySet<string>;
}

// Polars / Python / SQL — same set the backend's _CODE_BEARING_NODE_TYPES
// guards in ``inline_action_routes.py``. Kept narrow on purpose; if the
// backend list grows, mirror the change here.
const CODE_BEARING_NODE_TYPES: ReadonlySet<string> = new Set([
  "polars_code",
  "python_script",
  "sql_query",
]);

const ACTIONS: readonly ActionItem[] = [
  {
    action: "explain",
    label: "Explain",
    description: "Plain-language description of what this node does.",
  },
  {
    action: "optimise",
    label: "Optimise",
    description: "Concrete suggestions for performance / clarity.",
  },
  {
    action: "document",
    label: "Document",
    description: "Write a short description for the node's `description` field.",
  },
  {
    action: "regenerate_code",
    label: "Regenerate code",
    description: "Rewrite the code snippet, preserving the schema.",
    requiresNodeType: CODE_BEARING_NODE_TYPES,
  },
  {
    action: "suggest_filters",
    label: "Suggest filters",
    description: "Useful filter conditions over the upstream columns.",
  },
];

// Resolve the node_type from the live VueFlow graph so we can hide the
// "Regenerate code" item on non-code-bearing nodes. When the lookup
// fails (e.g. node was just removed), the item is hidden — defensive
// default that matches the backend's 422 rejection.
const resolvedNodeType = computed<string | null>(() => {
  const instance = flowStore.vueFlowInstance;
  if (!instance) return null;
  const node = instance.findNode?.(String(props.nodeId));
  if (!node) return null;
  // VueFlow's `type` is the registered node-type string (matches the
  // backend's `node_type`). Fall back to `data.type` for older graphs.
  const fromTop = (node as { type?: string }).type;
  if (typeof fromTop === "string" && fromTop.length > 0) return fromTop;
  const fromData = (node.data as { type?: string } | undefined)?.type;
  return typeof fromData === "string" && fromData.length > 0 ? fromData : null;
});

const visibleActions = computed<ActionItem[]>(() =>
  ACTIONS.filter((item) => {
    if (!item.requiresNodeType) return true;
    const nodeType = resolvedNodeType.value;
    return nodeType !== null && item.requiresNodeType.has(nodeType);
  }),
);

const isBusy = computed(() => aiStore.isStreaming);
const hasProvider = computed(() => aiStore.hasConfiguredProvider);

const disabledReason = computed<string | null>(() => {
  if (!hasProvider.value)
    return "Configure a provider in Settings → AI Providers to enable AI actions.";
  if (isBusy.value) return "Another AI request is in progress — wait or cancel it first.";
  return null;
});

const handlePick = async (action: InlineActionType): Promise<void> => {
  if (disabledReason.value !== null) return;
  emit("picked");
  await aiStore.runInlineAction(props.flowId, props.nodeId, action, props.nodeName);
};
</script>

<template>
  <div class="ai-inline-actions" role="menu">
    <div class="ai-inline-actions__header">
      <el-icon class="ai-inline-actions__header-icon">
        <MagicStick />
      </el-icon>
      <span class="ai-inline-actions__header-label">AI actions</span>
    </div>

    <div v-if="disabledReason" class="ai-inline-actions__notice" role="status">
      {{ disabledReason }}
    </div>

    <ul class="ai-inline-actions__list">
      <li v-for="item in visibleActions" :key="item.action" class="ai-inline-actions__item">
        <button
          type="button"
          role="menuitem"
          class="ai-inline-actions__button"
          :disabled="disabledReason !== null"
          :title="item.description"
          @click="handlePick(item.action)"
        >
          <span class="ai-inline-actions__label">{{ item.label }}</span>
          <span class="ai-inline-actions__description">{{ item.description }}</span>
        </button>
      </li>
    </ul>
  </div>
</template>

<style scoped>
.ai-inline-actions {
  display: flex;
  flex-direction: column;
  min-width: 240px;
  max-width: 320px;
  font-size: 0.875rem;
}

.ai-inline-actions__header {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--el-border-color-lighter);
  color: var(--el-text-color-primary);
  font-weight: 500;
}

.ai-inline-actions__header-icon {
  color: var(--el-color-primary);
  font-size: 1rem;
}

.ai-inline-actions__header-label {
  flex-grow: 1;
}

.ai-inline-actions__notice {
  margin-top: 0.5rem;
  padding: 0.5rem 0.6rem;
  background-color: var(--el-fill-color-lighter);
  border-radius: 4px;
  color: var(--el-text-color-secondary);
  font-size: 0.75rem;
  line-height: 1.4;
}

.ai-inline-actions__list {
  list-style: none;
  margin: 0.5rem 0 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 0.15rem;
}

.ai-inline-actions__item {
  margin: 0;
  padding: 0;
}

.ai-inline-actions__button {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.15rem;
  width: 100%;
  padding: 0.45rem 0.6rem;
  border: none;
  background: transparent;
  border-radius: 4px;
  cursor: pointer;
  text-align: left;
  color: var(--el-text-color-primary);
  transition: background-color 0.12s ease;
}

.ai-inline-actions__button:hover:not(:disabled) {
  background-color: var(--el-fill-color);
}

.ai-inline-actions__button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.ai-inline-actions__label {
  font-weight: 500;
  font-size: 0.875rem;
}

.ai-inline-actions__description {
  color: var(--el-text-color-secondary);
  font-size: 0.75rem;
  line-height: 1.3;
}
</style>
