<template>
  <section class="cell-output" :class="`cell-output--${output.state}`" :data-output-state="output.state">
    <p v-if="output.state === 'running'" class="output-note output-note--busy">
      <span class="output-spinner" aria-hidden="true"></span>
      Running…
    </p>
    <p v-else-if="output.state === 'blocked'" class="output-note">
      <svg class="output-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" /></svg>
      Not run: {{ output.message }}
    </p>
    <div v-else-if="output.state === 'error'" class="output-failure">
      <svg class="output-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
      <pre class="output-error" role="alert">{{ output.message }}</pre>
    </div>
    <template v-else>
      <div v-if="output.rows.length" class="output-rows">
        <RowTable :rows="output.rows" :limit="output.rows.length" />
      </div>
      <p class="output-note">{{ summary }}</p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import RowTable from '../RowTable.vue'
import type { CellOutput } from '../../stores/notebook-store'

const props = defineProps<{ output: CellOutput }>()

const count = new Intl.NumberFormat('en-US')

const summary = computed(() => {
  if (props.output.state !== 'rows') return ''
  const { rows, total, columns } = props.output
  if (rows.length === 0) return columns.length ? `No rows. Columns: ${columns.join(', ')}` : 'No rows.'
  if (total > rows.length) return `Showing ${count.format(rows.length)} of ${count.format(total)} rows`
  return rows.length === 1 ? '1 row' : `${count.format(rows.length)} rows`
})
</script>

<style scoped>
.cell-output {
  margin: 0 8px 8px 0;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
  cursor: default;
}

.output-note {
  display: flex;
  align-items: center;
  gap: 7px;
  margin: 0;
  padding: 6px 12px;
  font-size: 11.5px;
  color: var(--color-text-tertiary);
}

.output-note--busy {
  color: var(--color-accent);
}

.output-icon {
  flex: 0 0 auto;
  width: 13px;
  height: 13px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
}

.output-spinner {
  flex: 0 0 auto;
  width: 11px;
  height: 11px;
  border: 2px solid currentColor;
  border-top-color: transparent;
  border-radius: 50%;
  animation: output-spin 0.7s linear infinite;
}

@keyframes output-spin {
  to {
    transform: rotate(360deg);
  }
}

.output-failure {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 9px 12px;
  background: var(--nb-danger-tint, color-mix(in srgb, var(--color-danger) 10%, transparent));
  color: var(--color-danger-hover);
}

[data-theme='dark'] .output-failure {
  color: #fca5a5;
}

.output-failure .output-icon {
  margin-top: 2px;
}

.output-error {
  min-width: 0;
  margin: 0;
  font-family: var(--font-family-mono);
  font-size: 12px;
  line-height: 1.5;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

/* One scroller for both directions, so the horizontal bar stays in view. */
.output-rows {
  max-height: 248px;
  overflow: auto;
}

.cell-output--rows .output-note {
  border-top: 1px solid var(--color-border-light);
}

.output-rows :deep(.row-table-wrap) {
  border: none;
  border-radius: 0;
  overflow: visible;
}

.output-rows :deep(.row-table) {
  font-size: 12px;
}

.output-rows :deep(.row-table th) {
  position: sticky;
  top: 0;
  z-index: 1;
  padding: 6px 12px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--nb-surface, var(--color-background-secondary));
  font-size: 11.5px;
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
}

.output-rows :deep(.row-table td) {
  padding: 5px 12px;
  border-bottom: 1px solid var(--color-border-light);
  font-family: var(--font-family-mono);
  font-size: 12px;
  font-weight: 400;
  font-variant-numeric: tabular-nums;
}

.output-rows :deep(.row-table tbody tr:last-child td) {
  border-bottom: none;
}

.output-rows :deep(.row-table tbody tr:hover td) {
  background: var(--nb-active-line, var(--color-background-hover));
}
</style>
