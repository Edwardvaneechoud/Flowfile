<template>
  <div class="row-table-wrap">
    <table v-if="rows.length" class="row-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in shown" :key="index">
          <td v-for="column in columns" :key="column" :class="{ empty: row[column] === null || row[column] === undefined }">
            {{ format(row[column]) }}
          </td>
        </tr>
      </tbody>
    </table>
    <p v-else class="row-table-empty">No rows.</p>
    <p v-if="hidden > 0" class="row-table-more">…and {{ hidden }} more</p>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{ rows: Record<string, unknown>[]; limit?: number }>(), {
  limit: 8
})

const shown = computed(() => props.rows.slice(0, props.limit))
const hidden = computed(() => Math.max(0, props.rows.length - props.limit))

const columns = computed(() => {
  const seen: string[] = []
  for (const row of shown.value) {
    for (const column of Object.keys(row)) if (!seen.includes(column)) seen.push(column)
  }
  return seen
})

// A missing value is the interesting case in almost every lesson here, so it
// gets a glyph rather than rendering as an empty cell you might not notice.
const format = (value: unknown): string =>
  value === null || value === undefined
    ? '—'
    : typeof value === 'object'
      ? JSON.stringify(value)
      : String(value)
</script>

<style scoped>
.row-table-wrap {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: auto;
  max-height: 220px;
}

.row-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11.5px;
}

.row-table th,
.row-table td {
  padding: 4px 10px;
  text-align: left;
  white-space: nowrap;
  border-bottom: 1px solid var(--color-border-primary);
}

.row-table th {
  position: sticky;
  top: 0;
  font-weight: 600;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
}

.row-table td {
  color: var(--color-text-primary);
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}

.row-table td.empty {
  color: var(--color-text-secondary);
  opacity: 0.6;
}

.row-table-empty,
.row-table-more {
  margin: 0;
  padding: 6px 10px;
  font-size: 11px;
  color: var(--color-text-secondary);
}
</style>
