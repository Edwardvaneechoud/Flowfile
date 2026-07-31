<template>
  <div
    class="column-stats-panel"
    :style="panelStyle"
    role="dialog"
    :aria-label="`Statistics for ${columnName}`"
  >
    <div class="csp-header">
      <span class="csp-title" :title="columnName">{{ columnName }}</span>
      <span v-if="dataType" class="csp-dtype" :title="dataType">{{ dataType }}</span>
      <button class="csp-close" type="button" aria-label="Close" @click="emit('close')">
        <span class="material-icons" aria-hidden="true">close</span>
      </button>
    </div>

    <div v-if="loading" class="csp-body">
      <div v-for="i in 3" :key="i" class="csp-skeleton"></div>
      <p class="csp-note">Computing statistics — a locally-run step may re-run.</p>
    </div>

    <div v-else-if="errorKind === 'not-run'" class="csp-body">
      <p class="csp-message">{{ errorDetail || "Run this step to see column statistics." }}</p>
    </div>

    <div v-else-if="errorKind === 'error'" class="csp-body">
      <p class="csp-message">Could not compute statistics.</p>
      <button class="csp-retry" type="button" @click="emit('retry')">Retry</button>
    </div>

    <div v-else-if="stats" class="csp-body">
      <div v-if="badges.length || (nullPct !== null && nullPct > 0)" class="csp-badges">
        <span
          v-for="badge in badges"
          :key="badge.label"
          class="status-badge"
          :class="badge.className"
          >{{ badge.label }}</span
        >
        <span
          v-if="nullPct !== null && nullPct > 0"
          class="status-badge"
          :class="nullSeverityClass(nullPct)"
        >
          {{ nullPct }}% null
        </span>
      </div>

      <div
        v-if="nullPct !== null"
        class="csp-null-bar"
        role="img"
        :aria-label="`${nullPct}% of values are null`"
        :title="`${nullPct}% null`"
      >
        <div class="csp-null-bar__filled" :style="{ width: `${100 - nullPct}%` }"></div>
      </div>

      <div class="meta-grid csp-grid">
        <div class="meta-card">
          <span class="meta-label">Rows</span>
          <span class="meta-value">{{ formatCount(totalRowCount(stats)) }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Filled</span>
          <span class="meta-value">{{ formatCount(stats.number_of_filled_values) }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Nulls</span>
          <span class="meta-value">{{ formatCount(stats.number_of_empty_values) }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Unique</span>
          <span class="meta-value">{{ uniqueDisplay }}</span>
        </div>
        <!-- Min/Max/Avg carry arbitrary-length cell values, so they span both
             columns and clamp to two lines; the full value is on hover. -->
        <div v-if="stats.min_value !== null" class="meta-card csp-card--wide">
          <span class="meta-label">Min</span>
          <span class="meta-value csp-bound" :title="stats.min_value ?? ''">{{
            truncateValue(stats.min_value)
          }}</span>
        </div>
        <div v-if="stats.max_value !== null" class="meta-card csp-card--wide">
          <span class="meta-label">Max</span>
          <span class="meta-value csp-bound" :title="stats.max_value ?? ''">{{
            truncateValue(stats.max_value)
          }}</span>
        </div>
        <div v-if="stats.average_value !== null" class="meta-card csp-card--wide">
          <span class="meta-label">Avg</span>
          <span class="meta-value csp-bound" :title="stats.average_value ?? ''">{{
            truncateValue(stats.average_value)
          }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// Presentation-only stats popover. The parent (dataPreview.vue) owns fetching,
// caching, and dismissal; the backend ships exact counts on the ordinary
// FileColumn and every derived number (percentages, badges) comes from
// columnQuality.ts. Placement is derived from the click-captured anchor rect
// alone (placement.ts) — never measured — so the first paint is the final one.
import { computed } from "vue";
import type { FileColumn } from "../../../types/node.types";
import { PANEL_WIDTH, computePlacement } from "./placement";
import {
  formatCount,
  nullSeverityClass,
  pctNull,
  pctUnique,
  qualityBadges,
  totalRowCount,
  truncateValue,
} from "./columnQuality";

const props = defineProps<{
  columnName: string;
  dataType?: string;
  stats: FileColumn | null;
  loading: boolean;
  errorKind: "not-run" | "error" | null;
  // Server-provided 409 reason (not run yet / Performance mode), shown verbatim.
  errorDetail?: string | null;
  anchorRect: DOMRect;
}>();

const emit = defineEmits<{ close: []; retry: [] }>();

const badges = computed(() => (props.stats ? qualityBadges(props.stats) : []));
const nullPct = computed(() => (props.stats ? pctNull(props.stats) : null));

const uniqueDisplay = computed(() => {
  if (!props.stats || props.stats.number_of_unique_values === null) return "—";
  const count = formatCount(props.stats.number_of_unique_values);
  const pct = pctUnique(props.stats);
  return pct === null ? count : `${count} (${pct}%)`;
});

// Frozen for the panel's lifetime: window.inner* is read once, and the parent
// dismisses the panel on resize/scroll rather than repositioning it.
const panelStyle = computed<Record<string, string>>(() => {
  const { left, top, bottom, maxHeight } = computePlacement(props.anchorRect, {
    width: window.innerWidth,
    height: window.innerHeight,
  });
  return {
    left: `${left}px`,
    width: `${PANEL_WIDTH}px`,
    maxHeight: `${maxHeight}px`,
    ...(top !== null ? { top: `${top}px` } : { bottom: `${bottom}px` }),
  };
});
</script>

<style scoped>
.column-stats-panel {
  position: fixed;
  z-index: 4000;
  /* width / max-height / the pinned edge come from placement.ts via :style */
  display: flex;
  flex-direction: column;
  /* Last line of defence: no child may paint outside the rounded border. */
  overflow: hidden;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  box-shadow: var(--shadow-lg, 0 8px 24px rgba(0, 0, 0, 0.18));
  font-size: 12px;
}

.csp-header {
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 10px;
  border-bottom: 1px solid var(--color-border-primary);
}

.csp-title {
  font-weight: 600;
  color: var(--color-text-primary);
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.csp-dtype {
  /* Polars types get long (Datetime(time_unit='us', time_zone='UTC')); an
     unshrinkable pill pushed the header past the panel edge. */
  flex-shrink: 1;
  min-width: 0;
  max-width: 45%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 1px 6px;
  border-radius: 4px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: 10px;
}

.csp-close {
  margin-left: auto;
  flex-shrink: 0;
  display: inline-flex;
  border: none;
  background: transparent;
  padding: 0;
  cursor: pointer;
  color: var(--color-text-secondary);
}

.csp-close .material-icons {
  font-size: 15px;
}

.csp-close:hover {
  color: var(--color-text-primary);
}

.csp-body {
  flex: 1 1 auto;
  /* Required, or the flex child refuses to shrink and overflow-y never engages. */
  min-height: 0;
  overflow-y: auto;
  padding: 10px;
}

.csp-skeleton {
  height: 12px;
  border-radius: 4px;
  background: var(--color-background-secondary);
  margin-bottom: 8px;
  animation: csp-pulse 1.2s ease-in-out infinite;
}

@keyframes csp-pulse {
  50% {
    opacity: 0.45;
  }
}

.csp-note,
.csp-message {
  margin: 0;
  color: var(--color-text-secondary);
  /* errorDetail is server text shown verbatim — an unbroken path must not widen. */
  overflow-wrap: anywhere;
}

.csp-note {
  font-size: 11px;
}

.csp-retry {
  margin-top: 8px;
  padding: 3px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  cursor: pointer;
}

.csp-retry:hover {
  background: var(--color-background-hover);
}

.csp-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 8px;
}

.csp-null-bar {
  height: 4px;
  border-radius: 2px;
  background: var(--color-status-danger-bg, rgba(239, 68, 68, 0.35));
  overflow: hidden;
  margin-bottom: 10px;
}

.csp-null-bar__filled {
  height: 100%;
  background: var(--color-accent, #6366f1);
}

.csp-grid {
  /* minmax(0, 1fr) — not 1fr. A bare 1fr implies minmax(auto, 1fr), whose auto
     minimum resolves to each card's min-content width, i.e. the whole min/max
     string. That, not the panel width, pushed the cards outside the panel. */
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--spacing-1-5);
  /* .meta-grid ships a bottom margin for full-page panels; dead space here. */
  margin-bottom: 0;
}

.csp-grid .meta-card {
  min-width: 0;
  padding: var(--spacing-2);
}

.csp-grid .meta-card.csp-card--wide {
  grid-column: 1 / -1;
}

.csp-bound {
  /* Two-line clamp with an automatic trailing ellipsis. All four declarations
     are required; dropping any one silently disables the clamp. */
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  overflow: hidden;
  /* Unbroken tokens (ids, URLs, hashes) break instead of widening the card. */
  overflow-wrap: anywhere;
  line-height: var(--line-height-tight);
}
</style>
