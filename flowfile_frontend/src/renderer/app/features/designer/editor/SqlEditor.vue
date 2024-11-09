<template>
  <div class="sql-editor-root">
    <!-- Single root element -->
    <!-- All other elements of your component should be inside this div -->
    <codemirror
      v-model="code"
      placeholder="Code goes here..."
      :style="{ height: '300px' }"
      :autofocus="true"
      :indent-with-tab="true"
      :tab-size="2"
      :extensions="extensions"
      @ready="handleReady"
      @focus="log('focus', $event)"
      @blur="log('blur', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import { EditorView } from "@codemirror/view";
import { EditorState, Extension } from "@codemirror/state";

import { ref, shallowRef, defineExpose, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";

import { autocompletion, CompletionSource } from "@codemirror/autocomplete";

const props = defineProps({
  editorString: { type: String, required: true },
});
const emit = defineEmits(["update-editor-string"]);
const myCustomCompletions: CompletionSource = (context: any) => {
  let word = context.matchBefore(/\w*/);
  if (word?.from == word?.to && !context.explicit) {
    return null;
  }
  return {
    from: word?.from,
    options: [
      { label: "SELECT", type: "keyword" },
      { label: "INSERT", type: "keyword" },
      { label: "UPDATE", type: "keyword" },
      { label: "table_name", type: "keyword" },
      { label: "very_specific", type: "function" },
    ],
  };
};

const insertTextAtCursor = (text: string) => {
  console.log("doing this");
  if (view.value) {
    view.value.dispatch({
      changes: {
        from: view.value.state.selection.main.head,
        to: view.value.state.selection.main.head,
        insert: text,
      },
    });
  }
};

// Define reactive data
const code = ref(props.editorString);
const view = shallowRef<EditorView | null>(null);
const extensions: Extension[] = [
  sql(),
  oneDark,
  EditorState.tabSize.of(2),
  autocompletion({ override: [myCustomCompletions] }),
];

// Event handlers
const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view;
};
const log = (type: any, event: any) => {
  console.log(type, event);
};

watch(code, (newCode: string) => {
  emit("update-editor-string", newCode);
});

defineExpose({ insertTextAtCursor });
</script>

<style>
.sql-editor-root {
  display: flex;
  flex-direction: column;
  /* Additional styles as needed */
}
</style>
