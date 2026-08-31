<template>
  <div class="placeholder-settings">
    <div class="placeholder-header">
      <span class="placeholder-lock" aria-hidden="true">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
      </span>
      <span class="placeholder-badge">Not available in the browser</span>
    </div>

    <p class="placeholder-reason">{{ reason }}</p>

    <p class="placeholder-note">
      This node arrived through a share link as a placeholder — its settings did
      not travel. It stays on the canvas so you can see the flow's shape, but it
      (and anything downstream of it) can't run here.
    </p>

    <div class="placeholder-upgrade">
      <p class="placeholder-upgrade-title">This node runs in the full Flowfile installation.</p>
      <a
        class="placeholder-upgrade-btn"
        href="https://flowfile.io/install/"
        target="_blank"
        rel="noopener noreferrer"
      >
        <span>Install the full version</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
      </a>
    </div>

    <a
      v-if="docsUrl"
      class="placeholder-docs"
      :href="docsUrl"
      target="_blank"
      rel="noopener noreferrer"
    >
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
      <span>Learn more about this node</span>
    </a>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  reason: string
  docsUrl?: string
}>()
</script>

<style scoped>
.placeholder-settings {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 12px 4px;
  color: var(--text-primary);
}

.placeholder-header {
  display: flex;
  align-items: center;
  gap: 8px;
}

.placeholder-lock {
  display: flex;
  color: var(--color-text-muted);
}

.placeholder-badge {
  font-size: 12px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 10px;
  background-color: var(--bg-hover, rgba(128, 128, 128, 0.15));
  color: var(--text-secondary);
}

.placeholder-reason {
  margin: 0;
  font-size: 13px;
  font-weight: 500;
}

.placeholder-note {
  margin: 0;
  font-size: 12px;
  line-height: 1.5;
  color: var(--text-secondary);
}

.placeholder-upgrade {
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.placeholder-upgrade-title {
  margin: 0;
  font-size: 12px;
  font-weight: 600;
}

.placeholder-upgrade-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  align-self: flex-start;
  font-size: 12px;
  font-weight: 600;
  padding: 5px 10px;
  border-radius: 6px;
  background-color: var(--accent-color, #3b82f6);
  color: #fff;
  text-decoration: none;
}

.placeholder-upgrade-btn svg {
  width: 13px;
  height: 13px;
}

.placeholder-docs {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--accent-color, #3b82f6);
  text-decoration: none;
}

.placeholder-docs svg {
  width: 14px;
  height: 14px;
}
</style>
