<template>
  <header class="nb-toolbar">
    <button
      class="nb-btn nb-btn--run"
      data-action="run-all"
      :disabled="!notebook.canRun"
      title="Run the whole flow and show every cell's rows. Changed cells are pushed first."
      @click="notebook.runAll()"
    >
      <svg class="nb-icon nb-icon--solid" viewBox="0 0 24 24" aria-hidden="true"><path d="M7 4.5v15l12-7.5z" /></svg>
      Run all
    </button>
    <button
      class="nb-btn nb-btn--push"
      :class="{ 'is-armed': notebook.changedCount > 0 }"
      data-action="push"
      :disabled="!notebook.canPush"
      title="Apply the changed cells to the canvas, as one step you can undo. Nothing runs."
      @click="notebook.push()"
    >
      <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 16V4M7 9l5-5 5 5M5 20h14" /></svg>
      Push
      <span v-if="notebook.changedCount" class="nb-btn-count">{{ notebook.changedCount }}</span>
    </button>
    <button
      v-if="Object.keys(notebook.outputs).length"
      class="nb-btn nb-btn--quiet"
      data-action="clear-outputs"
      :disabled="notebook.running"
      title="Remove the rows shown under the cells; nothing on the canvas changes"
      @click="notebook.clearOutputs()"
    >
      Clear outputs
    </button>
    <span class="nb-status" :data-status="status.kind" role="status">
      <span v-if="status.kind === 'busy'" class="nb-spinner" aria-hidden="true"></span>
      <span v-else class="nb-status-dot" aria-hidden="true"></span>
      {{ status.text }}
    </span>
    <span class="nb-keys" title="Shift+Enter runs a cell and moves on · Ctrl/Cmd+Enter runs it in place">
      <kbd>Shift</kbd><kbd>Enter</kbd> to run
    </span>
  </header>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useNotebookStore } from '../../stores/notebook-store'

const notebook = useNotebookStore()

/** What the notebook is doing, or how it stands against the canvas. */
const status = computed<{ kind: 'busy' | 'changed' | 'synced'; text: string }>(() => {
  if (notebook.syncing) return { kind: 'busy', text: 'Pushing to the canvas…' }
  if (notebook.running) return { kind: 'busy', text: 'Running…' }
  const changed = notebook.changedCount
  if (changed) return { kind: 'changed', text: `${changed} changed ${changed === 1 ? 'cell' : 'cells'}, not on the canvas yet` }
  return { kind: 'synced', text: 'In step with the canvas' }
})
</script>

<style scoped>
.nb-toolbar {
  position: sticky;
  top: 0;
  z-index: 3;
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 0 -14px;
  padding: 9px 14px;
  border-bottom: 1px solid var(--color-border-primary);
  background: color-mix(in srgb, var(--nb-surface) 88%, transparent);
  backdrop-filter: blur(8px);
}

.nb-btn {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  white-space: nowrap;
  gap: 6px;
  height: 28px;
  padding: 0 12px;
  border: 1px solid transparent;
  border-radius: var(--border-radius-md);
  font: inherit;
  font-size: 12.5px;
  font-weight: var(--font-weight-medium);
  line-height: 1;
  cursor: pointer;
  transition:
    background-color var(--transition-base),
    border-color var(--transition-base),
    color var(--transition-base),
    box-shadow var(--transition-base);
}

.nb-btn:disabled {
  cursor: default;
  opacity: 0.5;
}

.nb-btn:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
}

.nb-btn--run {
  background: var(--color-accent-purple);
  color: #fff;
  box-shadow: var(--shadow-xs);
}

.nb-btn--run:hover:not(:disabled) {
  background: var(--color-accent-purple-hover);
}

.nb-btn--push,
.nb-btn--quiet {
  border-color: var(--color-border-primary);
  background: var(--nb-card);
  color: var(--color-text-secondary);
}

.nb-btn--quiet:hover:not(:disabled) {
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.nb-btn--push.is-armed {
  border-color: var(--color-accent);
  background: var(--color-accent);
  color: #fff;
  box-shadow: var(--shadow-xs);
}

.nb-btn--push.is-armed:hover:not(:disabled) {
  border-color: var(--color-accent-hover);
  background: var(--color-accent-hover);
}

.nb-btn-count {
  min-width: 17px;
  padding: 2px 5px;
  border-radius: var(--border-radius-full);
  background: rgba(255, 255, 255, 0.25);
  font-size: 11px;
  font-variant-numeric: tabular-nums;
  text-align: center;
}

.nb-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  margin-left: 4px;
  overflow: hidden;
  font-size: 12px;
  color: var(--color-text-tertiary);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.nb-status-dot {
  flex: 0 0 auto;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--color-success);
}

.nb-status[data-status='changed'] {
  color: var(--color-text-secondary);
}

.nb-status[data-status='changed'] .nb-status-dot {
  background: var(--color-warning);
}

.nb-status[data-status='busy'] {
  color: var(--color-accent);
}

.nb-keys {
  flex: 0 0 auto;
  margin-left: auto;
  font-size: 11px;
  color: var(--color-text-muted);
  white-space: nowrap;
}

.nb-keys kbd {
  display: inline-block;
  margin-right: 3px;
  padding: 1px 5px;
  border: 1px solid var(--color-border-primary);
  border-bottom-width: 2px;
  border-radius: var(--border-radius-sm);
  background: var(--nb-card);
  font-family: inherit;
  font-size: 10.5px;
  color: var(--color-text-tertiary);
}
</style>
