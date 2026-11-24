<template>
  <span class="source-badge" :class="`kind-${kind}`" :title="source">
    <i :class="SOURCE_ICONS[kind]"></i>
    {{ SOURCE_LABELS[kind] }}
  </span>
</template>

<script setup lang="ts">
import type { SemanticSourceKind } from "../../../types/semantic.types";
import { SOURCE_ICONS, SOURCE_LABELS } from "./semanticFormat";

defineProps<{ kind: SemanticSourceKind; source?: string }>();
</script>

<style scoped>
.source-badge {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  line-height: 18px;
  white-space: nowrap;
}

.kind-ai {
  color: var(--color-primary);
}
</style>
