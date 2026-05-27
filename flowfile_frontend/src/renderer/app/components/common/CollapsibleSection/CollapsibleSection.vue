<template>
  <section class="section collapsible-section" :class="{ 'is-nested': nested }">
    <div class="section-head">
      <h3
        class="cs-trigger"
        role="button"
        tabindex="0"
        :aria-expanded="open"
        @click="toggle"
        @keydown.enter.prevent="toggle"
        @keydown.space.prevent="toggle"
      >
        <i class="fa-solid fa-chevron-right cs-chevron" :class="{ 'is-open': open }" />
        <i v-if="icon" :class="[icon, 'section-icon']" />
        <span class="cs-title">{{ title }}</span>
        <span v-if="summary" class="cs-summary">{{ summary }}</span>
        <span v-else-if="count !== undefined" class="cs-count">{{ count }}</span>
      </h3>
      <div v-if="$slots.actions" class="cs-actions" @click.stop>
        <slot name="actions" />
      </div>
    </div>
    <el-collapse-transition>
      <div v-show="open" class="cs-body">
        <slot />
      </div>
    </el-collapse-transition>
  </section>
</template>

<script setup lang="ts">
import { useSectionToggle } from "./useSectionToggle";

const props = withDefaults(
  defineProps<{
    title: string;
    icon?: string;
    defaultOpen?: boolean;
    persistKey?: string;
    summary?: string;
    count?: number;
    nested?: boolean;
  }>(),
  {
    defaultOpen: true,
    nested: false,
  },
);

const { open, toggle } = useSectionToggle(props.persistKey, props.defaultOpen);
</script>

<style scoped>
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

/* Reuses the global `.section h3` / `.section-icon` look; we only add the
   click affordance and chevron, and drop the default heading margin so the
   header row stays compact (spacing moves to .cs-body). */
.cs-trigger {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex: 1;
  min-width: 0;
  margin: 0;
  cursor: pointer;
  user-select: none;
}

.cs-trigger:hover .cs-chevron,
.cs-trigger:focus-visible .cs-chevron {
  color: var(--color-primary);
}

.cs-chevron {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  transition: transform var(--transition-fast);
}

.cs-chevron.is-open {
  transform: rotate(90deg);
}

.cs-title {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cs-summary,
.cs-count {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
}

.cs-count {
  background: var(--color-background-secondary);
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-full);
  line-height: 1.8;
}

.cs-actions {
  flex-shrink: 0;
}

.cs-body {
  margin-top: var(--spacing-3);
}

/* Nested variant: subordinate heading (smaller than the parent section title)
   and tighter spacing, for sub-sections inside another CollapsibleSection. */
.is-nested {
  margin-bottom: var(--spacing-3);
}

.is-nested .cs-trigger {
  font-size: var(--font-size-sm);
}

.is-nested .cs-body {
  margin-top: var(--spacing-2);
}
</style>
