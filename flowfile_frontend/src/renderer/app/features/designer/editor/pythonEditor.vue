<template>
  <div class="polars-editor-root">
    <codemirror
      v-model="code"
      placeholder="Enter Polars code here..."
      :style="{ height: '500px' }"
      :autofocus="true"
      :indent-with-tab="false"
      :tab-size="4"
      :extensions="extensions"
      @ready="handleReady"
      @focus="log('focus', $event)"
      @blur="handleBlur"
    />
    <div v-if="validationError" class="validation-error">
      {{ validationError }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, Extension, Prec } from "@codemirror/state";
import { ref, shallowRef, defineExpose, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { autocompletion, CompletionSource, acceptCompletion } from "@codemirror/autocomplete";
import { indentMore, indentLess } from "@codemirror/commands";
import { bodyTooltips } from "@/utils/codemirrorTooltips";
import { polarsCompletionVals } from "./pythonEditor/polarsCompletions";

const props = defineProps({
  editorString: { type: String, required: true },
});
const emit = defineEmits(["update-editor-string", "validation-error"]);

const validationError = ref<string | null>(null);

const polarsCompletions: CompletionSource = (context: any) => {
  let word = context.matchBefore(/\w*/);
  if (word?.from == word?.to && !context.explicit) {
    return null;
  }
  return {
    from: word?.from,
    options: polarsCompletionVals,
  };
};

const tabKeymap = keymap.of([
  {
    key: "Tab",
    run: (view: EditorView): boolean => {
      if (acceptCompletion(view)) {
        return true;
      }
      return indentMore(view);
    },
  },
  {
    key: "Shift-Tab",
    run: (view: EditorView): boolean => {
      return indentLess(view);
    },
  },
]);

const insertTextAtCursor = (text: string) => {
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

// Replace the whole editor content. `code` is the v-model bound to CodeMirror,
// so assigning it updates the view and fires the `update-editor-string` emit.
const setCode = (text: string) => {
  code.value = text;
};

const code = ref(props.editorString);
const view = shallowRef<EditorView | null>(null);

const extensions: Extension[] = [
  python(),
  oneDark,
  EditorView.theme({
    "&": { fontSize: "12px" },
    ".cm-content": { fontSize: "12px" },
    ".cm-gutters": { fontSize: "12px" },
  }),
  EditorState.tabSize.of(4),
  autocompletion({
    override: [polarsCompletions],
    defaultKeymap: true,
    closeOnBlur: false,
  }),
  bodyTooltips(),
  Prec.highest(tabKeymap),
];

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view;
};

const log = (type: any, event: any) => {
  console.log(type, event);
};

const handleBlur = async () => {
  try {
    validationError.value = null;
    emit("validation-error", null);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    validationError.value = errorMessage;
    emit("validation-error", errorMessage);
  }
};

watch(code, (newCode: string) => {
  emit("update-editor-string", newCode);
});

defineExpose({ insertTextAtCursor, setCode });
</script>

<style>
.polars-editor-root {
  display: flex;
  flex-direction: column;
}

.validation-error {
  margin-top: 8px;
  padding: 8px;
  color: var(--color-error, #ff5555);
  background-color: rgba(255, 85, 85, 0.1);
  border-radius: 4px;
}
/* Syntax highlighting comes from python() + oneDark (Lezer-generated classes);
   no hand-rolled global .cm-* rules — they collide with other editors. */
</style>
