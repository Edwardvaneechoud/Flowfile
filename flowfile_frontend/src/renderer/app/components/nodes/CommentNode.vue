<script setup lang="ts">
import { useVueFlow } from "@vue-flow/core";
import { NodeResizer, type OnResizeEnd } from "@vue-flow/node-resizer";
import { computed, nextTick, onMounted, ref, watch } from "vue";

import {
  COMMENT_MIN_HEIGHT,
  COMMENT_MIN_WIDTH,
  useCanvasComments,
} from "../../composables/useCanvasComments";
import type { CommentNodeData } from "../../types/flow.types";

const props = defineProps<{
  id: string;
  data: CommentNodeData;
  selected?: boolean;
}>();

const { removeNodes, findNode } = useVueFlow();
const { persistCommentText, persistCommentBounds } = useCanvasComments();

const editing = ref(false);
const draft = ref("");
const textarea = ref<HTMLTextAreaElement | null>(null);

// VueFlow keeps a freshly added node invisible until it has measured it, and
// focus() is a no-op on a hidden element, so focus once the node is measured.
const measured = computed(() => Boolean(findNode(props.id)?.dimensions.width));
watch(
  [editing, measured],
  ([isEditing, isMeasured]) => {
    if (isEditing && isMeasured) nextTick(() => textarea.value?.focus());
  },
  { flush: "post" },
);

function startEditing(): void {
  draft.value = props.data.text;
  editing.value = true;
}

async function commit(): Promise<void> {
  if (!editing.value) return;
  editing.value = false;
  const text = draft.value.trim();
  if (text === props.data.text) return;
  await persistCommentText(props.data.id, text);
}

function cancel(): void {
  editing.value = false;
}

// Deleting through removeNodes routes the backend call through the canvas's
// single nodes-change handler, the same path the Delete key takes.
function onDelete(): void {
  removeNodes([props.id]);
}

async function onResizeEnd({ params }: OnResizeEnd): Promise<void> {
  const node = findNode(props.id);
  await persistCommentBounds({
    comment_id: props.data.id,
    x_position: node ? node.computedPosition.x : params.x,
    y_position: node ? node.computedPosition.y : params.y,
    width: params.width,
    height: params.height,
  });
}

onMounted(() => {
  // A freshly added comment opens straight into the editor.
  if (props.data.autoEdit) startEditing();
});
</script>

<template>
  <div class="comment-node" :class="{ selected, editing }">
    <NodeResizer
      :is-visible="Boolean(selected)"
      :min-width="COMMENT_MIN_WIDTH"
      :min-height="COMMENT_MIN_HEIGHT"
      color="#d97706"
      @resize-end="onResizeEnd"
    />
    <button
      v-if="!editing"
      class="comment-delete"
      title="Delete comment"
      @click.stop="onDelete"
      @mousedown.stop
    >
      ✕
    </button>
    <textarea
      v-if="editing"
      ref="textarea"
      v-model="draft"
      class="comment-editor nodrag nowheel"
      placeholder="Write a comment…"
      @blur="commit"
      @keydown.esc.stop="cancel"
      @keydown.enter.exact.stop
      @keydown.ctrl.enter.prevent="commit"
      @keydown.meta.enter.prevent="commit"
      @mousedown.stop
      @click.stop
    ></textarea>
    <div v-else class="comment-text" title="Double-click to edit" @dblclick.stop="startEditing">
      <span v-if="data.text">{{ data.text }}</span>
      <span v-else class="comment-placeholder">Double-click to write a comment</span>
    </div>
  </div>
</template>

<style scoped>
.comment-node {
  position: relative;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
  border: 1px solid #fcd34d;
  border-radius: 6px;
  background: #fef9c3;
  color: #713f12;
  font-size: 12px;
  line-height: 1.4;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
}
.comment-node.selected {
  box-shadow: 0 0 0 2px rgba(217, 119, 6, 0.45);
}
.comment-text {
  width: 100%;
  height: 100%;
  padding: 8px 10px;
  box-sizing: border-box;
  overflow: hidden;
  white-space: pre-wrap;
  word-break: break-word;
  cursor: grab;
}
.comment-placeholder {
  opacity: 0.55;
  font-style: italic;
}
.comment-editor {
  width: 100%;
  height: 100%;
  padding: 8px 10px;
  box-sizing: border-box;
  border: none;
  border-radius: 6px;
  resize: none;
  background: #fffbeb;
  color: inherit;
  font: inherit;
  outline: 2px solid rgba(217, 119, 6, 0.45);
}
.comment-delete {
  position: absolute;
  top: 2px;
  right: 2px;
  width: 18px;
  height: 18px;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: inherit;
  font-size: 11px;
  line-height: 18px;
  cursor: pointer;
  opacity: 0;
  transition: opacity 0.15s;
}
.comment-node:hover .comment-delete,
.comment-node.selected .comment-delete {
  opacity: 0.7;
}
.comment-delete:hover {
  opacity: 1;
  background: rgba(217, 119, 6, 0.15);
}
</style>
