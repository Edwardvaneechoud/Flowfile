<script setup lang="ts">
// Sparkle button that toggles the AI assistant drawer.
//
// Mounted inside the header's status-wrapper alongside the flow-status
// indicator and the results-panel toggle — same control-button visual
// language (32px circle, neutral default, primary-on-active) so the AI
// trigger reads as a peer panel-toggle, not a competing floating widget.
//
// W36 — paired with a small adjacent caret that opens an el-dropdown menu
// with a "Quick command…" entry. The kebab affordance gives users without
// the ⌘K muscle memory (or whose browsers eat the shortcut) a discoverable
// path into the W33 command palette. Single-click on the sparkle still
// toggles the drawer; the palette is one extra click away.

import { computed } from "vue";
import { ArrowDown } from "@element-plus/icons-vue";
import { useEditorStore } from "../../stores/editor-store";
import { useAiCommandPaletteStore } from "../../stores/ai-command-palette-store";

const editorStore = useEditorStore();
const palette = useAiCommandPaletteStore();

const isOpen = computed(() => editorStore.isAiOpen);

const handleClick = (event: MouseEvent): void => {
  event.preventDefault();
  event.stopPropagation();
  editorStore.toggleAiDrawer();
};

const handleQuickCommand = (): void => {
  // Same code path the Cmd+K binding uses (Canvas.vue → palette.toggle).
  // From a menu item click the user's intent is unambiguous "open" — calling
  // open() avoids the surprising close-on-second-click that toggle() would
  // give if the palette was already up.
  palette.open();
};
</script>

<template>
  <div class="ai-trigger" :class="{ 'is-open': isOpen }">
    <button
      class="ai-trigger__main"
      :class="{ 'is-active': isOpen }"
      title="AI Assistant"
      aria-label="Toggle AI assistant"
      @click="handleClick"
    >
      <span class="ai-trigger__icon" aria-hidden="true">✨</span>
    </button>
    <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
      <button
        class="ai-trigger__menu"
        type="button"
        title="More AI actions"
        aria-label="More AI actions"
        @click.stop
      >
        <el-icon><ArrowDown /></el-icon>
      </button>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item @click="handleQuickCommand">
            <span class="ai-trigger__menu-icon" aria-hidden="true">⌨</span>
            <span class="ai-trigger__menu-label">Quick command…</span>
            <span class="ai-trigger__menu-shortcut"> <kbd>⌘</kbd><kbd>K</kbd> </span>
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<style scoped>
.ai-trigger {
  display: inline-flex;
  align-items: center;
  gap: 2px;
}

.ai-trigger__main {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: none;
  background: var(--color-gray-400);
  color: var(--color-button-primary);
  cursor: pointer;
  transition: all 0.3s ease;
  box-shadow: var(--shadow-sm);
}

.ai-trigger__main:hover {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
  transform: translateY(-1px);
  box-shadow: var(--shadow-md);
}

.ai-trigger__main.is-active {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
}

.ai-trigger__icon {
  font-size: 16px;
  line-height: 1;
  pointer-events: none;
}

.ai-trigger__menu {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 28px;
  padding: 0;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
  transition:
    background 0.2s ease,
    color 0.2s ease;
}

.ai-trigger__menu:hover,
.ai-trigger__menu:focus-visible {
  background: var(--color-gray-200, #e5e7eb);
  color: var(--color-text, #1f2937);
  outline: none;
}

.ai-trigger__menu :deep(.el-icon) {
  font-size: 12px;
  line-height: 1;
}

.ai-trigger__menu-icon {
  font-size: 13px;
  margin-right: 8px;
}

.ai-trigger__menu-label {
  flex: 1;
}

.ai-trigger__menu-shortcut {
  display: inline-flex;
  gap: 2px;
  margin-left: 12px;
}

.ai-trigger__menu-shortcut kbd {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", monospace;
  font-size: 10px;
  background: #f3f4f6;
  border: 1px solid #d1d5db;
  border-radius: 3px;
  padding: 0 4px;
  color: #6b7280;
}
</style>
