<template>
  <el-popover
    placement="right"
    :width="260"
    trigger="hover"
    :show-after="350"
    :disabled="!hasTooltip || !!suppressTooltip"
    popper-class="node-list-item-popover"
  >
    <template #reference>
      <div
        class="node-item"
        :data-tutorial-node="node.item"
        draggable="true"
        @dragstart="emit('dragstart', $event, node)"
        @contextmenu.prevent.stop="emit('contextmenu', $event, node)"
      >
        <img :src="iconUrl" :alt="node.name" class="node-image" />
        <span class="node-name">{{ node.name }}</span>
        <span
          v-if="node.execution_environment === 'kernel'"
          class="kernel-badge"
          :class="kernelBadgeClass"
          :title="kernelBadgeTitle"
        >
          <KernelBadgeIcon />
        </span>
      </div>
    </template>
    <div class="node-tooltip">
      <div class="node-tooltip-title">{{ node.drawer_title || node.name }}</div>
      <!-- eslint-disable-next-line vue/no-v-html -- sanitised by renderSafeMarkdown -->
      <div v-if="introHtml" class="node-tooltip-intro" v-html="introHtml"></div>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { NodeTemplate } from "../../types";
import { useNodeIconUrl } from "../../composables/useCustomNodeIcon";
import { readinessKey, useKernelReadiness } from "../../composables/useKernelReadiness";
import { renderSafeMarkdown } from "../../lib/markdown";
import KernelBadgeIcon from "./KernelBadgeIcon.vue";

const props = defineProps<{ node: NodeTemplate; suppressTooltip?: boolean }>();

// Both must be declared: el-popover is the root and does not forward $attrs to
// its trigger, so an undeclared native-named listener is silently dropped.
const emit = defineEmits<{
  dragstart: [event: DragEvent, node: NodeTemplate];
  contextmenu: [event: MouseEvent, node: NodeTemplate];
}>();

// Custom-node icons come from a JWT-gated endpoint; built-in glyphs resolve
// directly. useNodeIconUrl routes both correctly (authed blob vs bundled asset).
const iconUrl = useNodeIconUrl(() => props.node.image);

const introHtml = computed(() =>
  props.node.drawer_intro ? renderSafeMarkdown(props.node.drawer_intro) : "",
);

const hasTooltip = computed(() => !!(props.node.drawer_title || introHtml.value));

// Reads the shared readiness cache (batch-fetched once by NodeList) — no HTTP here.
const { readiness, unavailable } = useKernelReadiness();

const kernelReadiness = computed(() => {
  if (unavailable.value || props.node.execution_environment !== "kernel") return null;
  const deps = props.node.dependencies;
  if (!deps?.length) return null;
  return readiness.value[readinessKey(deps)] ?? null;
});

const kernelBadgeClass = computed(() =>
  kernelReadiness.value ? `kernel-badge--${kernelReadiness.value.level}` : "",
);

const kernelBadgeTitle = computed(() => {
  const base = "Runs in isolated kernel";
  const summary = kernelReadiness.value;
  if (!summary) return base;
  if (summary.level === "full")
    return `${base} — kernel "${summary.best_kernel_name}" has all required packages`;
  if (summary.level === "partial")
    return `${base} — kernel "${summary.best_kernel_name}" is missing some required packages`;
  // Distinct from "none": we couldn't check, rather than checked and found nothing.
  if (summary.level === "unknown") return `${base} — kernel packages could not be verified`;
  return `${base} — no kernel has the required packages yet`;
});
</script>

<style scoped>
.node-item {
  display: flex;
  align-items: center;
  padding: var(--spacing-2) var(--spacing-4);
  cursor: pointer;
  user-select: none;
  transition: background-color var(--transition-fast);
  border-bottom: 1px solid var(--color-border-light);
  height: 32px;
}

.node-item:hover {
  background-color: var(--color-background-tertiary);
}

.node-image {
  width: 24px;
  height: 24px;
  margin-right: var(--spacing-2-5);
}

.node-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.kernel-badge {
  display: inline-flex;
  align-items: center;
  margin-left: auto;
  color: var(--color-text-secondary);
  opacity: 0.7;
}

/* Readiness tint only — no layout change when the summary is missing. */
.kernel-badge--full {
  color: var(--color-success);
  opacity: 0.9;
}

.kernel-badge--partial {
  color: var(--color-warning);
  opacity: 0.9;
}

/* Unverifiable reads as neutral, never as the success tint. */
.kernel-badge--unknown {
  color: var(--color-text-secondary);
}

.kernel-badge--none {
  color: var(--color-text-secondary);
}

.node-tooltip-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-1);
}

.node-tooltip-intro {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.node-tooltip-intro :deep(p) {
  margin: 0 0 var(--spacing-1);
}

.node-tooltip-intro :deep(p:last-child) {
  margin-bottom: 0;
}

.node-tooltip-intro :deep(code) {
  font-family: var(--font-family-mono, monospace);
  font-size: 0.9em;
  background: var(--color-background-muted);
  padding: 0 3px;
  border-radius: 3px;
}
</style>
