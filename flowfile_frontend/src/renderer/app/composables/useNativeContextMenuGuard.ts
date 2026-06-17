import { onMounted, onUnmounted } from "vue";

import { isDesktop } from "../../lib/desktop";

/**
 * In the desktop (Tauri) shell, suppress WebKit's native right-click context
 * menu everywhere except inside editable fields. Without this, right-clicks
 * that miss the app's own context-menu handlers (e.g. over a text selection,
 * edges, the minimap, or chrome gaps on the canvas) leak the native WebView
 * menu showing a stray "Copy" entry.
 *
 * Editable targets (text inputs, textareas, contenteditable, CodeMirror) keep
 * the native menu so right-click → paste still works there.
 *
 * No-op in web mode, where the browser's own menu is expected.
 */
export function useNativeContextMenuGuard(): void {
  if (!isDesktop) return;

  const onContextMenu = (event: MouseEvent) => {
    const target = event.target as HTMLElement | null;
    if (
      target &&
      typeof target.closest === "function" &&
      target.closest(
        'input, textarea, .cm-editor, [contenteditable]:not([contenteditable="false"])',
      )
    ) {
      return; // editable field — keep the native menu
    }
    event.preventDefault();
  };

  onMounted(() => document.addEventListener("contextmenu", onContextMenu));
  onUnmounted(() => document.removeEventListener("contextmenu", onContextMenu));
}
