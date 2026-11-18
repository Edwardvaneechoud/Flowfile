<template>
  <el-popover
    :visible="open"
    placement="bottom-start"
    width="auto"
    popper-class="tile-note-popper"
    :persistent="false"
  >
    <template #reference>
      <button
        ref="buttonEl"
        type="button"
        class="tile-note-button"
        :class="{ 'is-open': open }"
        :aria-label="label ? `Note: ${label}` : 'Note'"
        :aria-expanded="open"
        :aria-controls="open ? noteId : undefined"
        data-testid="tile-note-button"
        @pointerenter="onPointerEnter"
        @pointerleave="onPointerLeave"
        @mousedown.stop
        @dblclick.stop
        @click.stop="onClick"
      >
        <el-icon><InfoFilled /></el-icon>
      </button>
    </template>
    <div
      :id="noteId"
      ref="noteEl"
      class="tile-note"
      role="note"
      tabindex="-1"
      data-testid="tile-note"
      @pointerenter="onPointerEnter"
      @pointerleave="onPointerLeave"
      @focusout="onFocusOut"
    >
      <TileNoteBody :markdown="note" />
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import { InfoFilled } from "@element-plus/icons-vue";
import TileNoteBody from "./TileNoteBody.vue";
import { NOTE_CLOSE_DELAY_MS, NOTE_OPEN_DELAY_MS } from "./tileNote";

/**
 * A tile's ⓘ: hovering it opens the note after a short delay and leaving both the icon and the
 * note closes it; a click, a tap or Enter/Space opens it until a click elsewhere or Esc.
 * The popover is controlled here (no Element Plus trigger), so hover and pinning can share it.
 */
const props = defineProps<{
  note: string;
  /** The tile's title, for the button's accessible name. */
  label?: string;
}>();

const noteId = `tile-note-${Math.random().toString(36).slice(2, 10)}`;
const open = ref(false);
const pinned = ref(false);
const buttonEl = ref<HTMLButtonElement | null>(null);
const noteEl = ref<HTMLElement | null>(null);
let timer: ReturnType<typeof setTimeout> | null = null;

const clearTimer = () => {
  if (timer) clearTimeout(timer);
  timer = null;
};

const close = () => {
  clearTimer();
  open.value = false;
  pinned.value = false;
};

const inside = (target: EventTarget | null): boolean =>
  target instanceof Node &&
  (!!buttonEl.value?.contains(target) || !!noteEl.value?.contains(target));

// A touch has no hover, and a pressed button means a drag is passing over the icon.
const onPointerEnter = (e: PointerEvent) => {
  if (e.pointerType === "touch" || e.buttons !== 0) return;
  clearTimer();
  if (!open.value) timer = setTimeout(() => (open.value = true), NOTE_OPEN_DELAY_MS);
};

const onPointerLeave = (e: PointerEvent) => {
  if (e.pointerType === "touch" || pinned.value) return;
  clearTimer();
  timer = setTimeout(() => (open.value = false), NOTE_CLOSE_DELAY_MS);
};

const onClick = async (e: MouseEvent) => {
  clearTimer();
  if (pinned.value) {
    close();
    return;
  }
  open.value = true;
  pinned.value = true;
  // detail is 0 for Enter or Space: move focus into the note so its links can be reached.
  if (e.detail === 0) {
    await nextTick();
    noteEl.value?.focus({ preventScroll: true });
  }
};

// No related target is focus leaving the window (another app, devtools): the note stays.
const onFocusOut = (e: FocusEvent) => {
  if (e.relatedTarget && !inside(e.relatedTarget)) close();
};

const onDocPointerDown = (e: PointerEvent) => {
  if (!inside(e.target)) close();
};

// Capture, so a dialog or present mode around the tile does not also take this Esc.
const onDocKeyDown = (e: KeyboardEvent) => {
  if (e.key !== "Escape") return;
  e.preventDefault();
  e.stopPropagation();
  close();
  buttonEl.value?.focus();
};

const unlisten = () => {
  document.removeEventListener("pointerdown", onDocPointerDown, true);
  document.removeEventListener("keydown", onDocKeyDown, true);
};

watch(open, (isOpen) => {
  if (isOpen) {
    document.addEventListener("pointerdown", onDocPointerDown, true);
    document.addEventListener("keydown", onDocKeyDown, true);
  } else {
    unlisten();
  }
});

watch(
  () => props.note,
  (note) => {
    if (!note.trim()) close();
  },
);

onBeforeUnmount(() => {
  clearTimer();
  unlisten();
});
</script>

<style scoped>
.tile-note-button {
  position: relative;
  z-index: 1;
  display: inline-flex;
  flex: none;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  padding: 0;
  border: 1px solid transparent;
  border-radius: 50%;
  background: transparent;
  color: var(--el-text-color-placeholder);
  font-size: 13px;
  line-height: 1;
  cursor: pointer;
}
/* A 28px touch target around the 16px icon, without moving the header around it. */
.tile-note-button::before {
  content: "";
  position: absolute;
  inset: -7px;
}
.tile-note-button:hover,
.tile-note-button.is-open,
.tile-note-button:focus-visible {
  color: var(--el-color-primary);
  outline: none;
}
.tile-note-button:focus-visible {
  border-color: var(--el-color-primary-light-5);
}
.tile-note {
  box-sizing: border-box;
  max-width: min(360px, calc(100vw - 32px));
  max-height: min(320px, 60vh);
  overflow: auto;
  padding: 10px 12px;
  outline: none;
}
</style>

<style>
.el-popover.el-popper.tile-note-popper {
  padding: 0;
  min-width: 0;
}
</style>
