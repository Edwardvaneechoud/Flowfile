<template>
  <div class="dry-run-results" data-testid="dry-run-results">
    <div v-if="result.error" class="error-card">
      <div class="error-head">
        <i class="fa-solid fa-circle-exclamation"></i>
        <span class="error-kind">{{ errorKindLabel }}</span>
      </div>
      <p class="error-message">{{ result.error }}</p>
      <details v-if="result.traceback" class="error-traceback">
        <summary>Traceback</summary>
        <pre>{{ result.traceback }}</pre>
      </details>
    </div>

    <template v-else>
      <div class="run-meta">
        <span class="meta-chip">
          <i class="fa-solid fa-clock"></i> {{ result.duration_ms }} ms
        </span>
        <span class="meta-chip"> <i class="fa-solid fa-server"></i> {{ executedInLabel }} </span>
      </div>

      <div v-for="output in result.outputs" :key="output.name" class="output-block">
        <div class="output-head">
          <span class="output-name">{{ output.name }}</span>
          <span class="output-rows">
            {{ output.row_count }} row(s){{ output.truncated ? " (truncated)" : "" }}
          </span>
        </div>

        <div class="schema-chips">
          <span v-for="col in output.columns" :key="col.name" class="schema-chip">
            {{ col.name }}
            <span class="schema-dtype">{{ col.data_type }}</span>
          </span>
        </div>

        <div class="result-scroll">
          <table class="result-table">
            <thead>
              <tr>
                <th v-for="col in output.columns" :key="col.name">{{ col.name }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, rIdx) in normalizedRows(output)" :key="rIdx">
                <td
                  v-for="col in output.columns"
                  :key="col.name"
                  :data-testid="`result-cell-${col.name}-${rIdx}`"
                >
                  {{ formatCell(row[col.name]) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>

    <details v-if="result.logs.length" class="logs-block">
      <summary>Logs ({{ result.logs.length }})</summary>
      <pre class="logs-pre">{{ result.logs.join("\n") }}</pre>
    </details>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { DryRunResponse, DryRunOutput } from "../../../../api/nodeDesigner";

const props = defineProps<{ result: DryRunResponse }>();

const errorKindLabel = computed(() => {
  const kind = props.result.error_kind;
  if (!kind) return "Error";
  return kind.charAt(0).toUpperCase() + kind.slice(1) + " error";
});

const executedInLabel = computed(() =>
  props.result.executed_in === "worker" ? "Worker" : "In core",
);

// Rows may arrive as arrays (positional) or objects (keyed); normalize to objects.
function normalizedRows(output: DryRunOutput): Record<string, unknown>[] {
  return output.rows.map((row) => {
    if (Array.isArray(row)) {
      const obj: Record<string, unknown> = {};
      output.columns.forEach((col, i) => {
        obj[col.name] = row[i];
      });
      return obj;
    }
    return row as Record<string, unknown>;
  });
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
</script>

<style scoped>
.dry-run-results {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.error-card {
  border: 1px solid var(--color-danger, #ef4444);
  border-radius: 6px;
  background: var(--color-danger-light, #fef2f2);
  padding: 0.75rem;
}

.error-head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: var(--color-danger-hover, #b91c1c);
}

.error-message {
  margin: 0.5rem 0 0;
  font-size: 0.8125rem;
  color: var(--color-text-primary, #374151);
  white-space: pre-wrap;
}

.error-traceback {
  margin-top: 0.5rem;
}

.error-traceback pre {
  margin: 0.5rem 0 0;
  font-size: 0.6875rem;
  overflow-x: auto;
  background: var(--color-background-primary, #fff);
  padding: 0.5rem;
  border-radius: 4px;
}

.run-meta {
  display: flex;
  gap: 0.5rem;
}

.meta-chip {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.6875rem;
  padding: 0.25rem 0.5rem;
  border-radius: 999px;
  background: var(--color-background-secondary, #f3f4f6);
  color: var(--color-text-secondary, #6b7280);
}

.output-block {
  border: 1px solid var(--color-border-primary, #e5e7eb);
  border-radius: 6px;
  padding: 0.625rem;
}

.output-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
}

.output-name {
  font-weight: 600;
  font-size: 0.8125rem;
}

.output-rows {
  font-size: 0.6875rem;
  color: var(--color-text-secondary, #6b7280);
}

.schema-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 0.375rem;
  margin-bottom: 0.5rem;
}

.schema-chip {
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  font-size: 0.6875rem;
  padding: 0.125rem 0.5rem;
  border-radius: 4px;
  background: var(--color-background-secondary, #f3f4f6);
  color: var(--color-text-primary, #374151);
}

.schema-dtype {
  color: var(--color-accent, #0891b2);
  font-family: var(--font-family-mono, monospace);
}

.result-scroll {
  overflow-x: auto;
  border: 1px solid var(--color-border-light, #e5e7eb);
  border-radius: 4px;
  max-height: 240px;
  overflow-y: auto;
}

.result-table {
  border-collapse: collapse;
  width: 100%;
  font-size: 0.75rem;
}

.result-table th,
.result-table td {
  border: 1px solid var(--color-border-light, #e5e7eb);
  padding: 0.25rem 0.5rem;
  text-align: left;
  white-space: nowrap;
}

.result-table th {
  background: var(--color-background-secondary, #f3f4f6);
  font-weight: 600;
}

.logs-block summary {
  font-size: 0.75rem;
  cursor: pointer;
  color: var(--color-text-secondary, #6b7280);
}

.logs-pre {
  margin: 0.5rem 0 0;
  font-size: 0.6875rem;
  font-family: var(--font-family-mono, monospace);
  background: var(--color-background-secondary, #f3f4f6);
  padding: 0.5rem;
  border-radius: 4px;
  overflow-x: auto;
  max-height: 160px;
  overflow-y: auto;
}
</style>
