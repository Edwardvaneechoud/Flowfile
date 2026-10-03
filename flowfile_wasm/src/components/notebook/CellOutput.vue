<template>
  <section class="cell-output" :class="`cell-output--${output.state}`" :data-output-state="output.state">
    <p v-if="output.state === 'running'" class="output-note">Running…</p>
    <p v-else-if="output.state === 'blocked'" class="output-note">Not run: {{ output.message }}</p>
    <pre v-else-if="output.state === 'error'" class="output-error" role="alert">{{ output.message }}</pre>
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
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  cursor: default;
}

.output-note {
  margin: 0;
  padding: 6px 10px;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.output-error {
  margin: 0;
  padding: 8px 10px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  color: #dc2626;
}

/* One scroller for both directions, so the horizontal bar stays in view. */
.output-rows {
  max-height: 260px;
  overflow: auto;
}

.output-rows :deep(.row-table-wrap) {
  border: none;
  border-radius: 0;
  overflow: visible;
}

.output-rows :deep(.row-table th) {
  position: sticky;
  top: 0;
}
</style>
