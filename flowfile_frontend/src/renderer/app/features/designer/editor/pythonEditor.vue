<template>
  <div class="polars-editor-root">
    <codemirror
      v-model="code"
      placeholder="Enter Polars code here..."
      :style="{ height: '300px' }"
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
import { EditorState, Extension } from "@codemirror/state";
import { ref, shallowRef, defineExpose, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import {
  autocompletion,
  CompletionSource,
  acceptCompletion,
} from "@codemirror/autocomplete";
import { indentMore } from "@codemirror/commands";
import { polarsCompletionVals } from "./pythonEditor/polarsCompletions";

const props = defineProps({
  editorString: { type: String, required: true },
});
const emit = defineEmits(["update-editor-string", "validation-error"]);

// Track validation errors
const validationError = ref<string | null>(null);

// Polars-specific autocompletions
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

// Custom keymap for tab handling

const tabKeymap = keymap.of([
  {
    key: "Tab",
    run: (view: EditorView): boolean => {
      // If there's an active completion, accept it
      if (acceptCompletion(view)) {
        return true;
      }
      // If no completion is active, perform normal tab indentation
      return indentMore(view);
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

// Define reactive data
const code = ref(props.editorString);
const view = shallowRef<EditorView | null>(null);

// Extensions configuration
const extensions: Extension[] = [
  python(),
  oneDark,
  EditorState.tabSize.of(4),
  autocompletion({
    override: [polarsCompletions],
    defaultKeymap: false, // Disable default keymap
    closeOnBlur: false,
  }),
  tabKeymap,
];

// Rest of the component code remains the same...
const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view;
};

const log = (type: any, event: any) => {
  console.log(type, event);
};

const handleBlur = async (_: any) => {
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

defineExpose({ insertTextAtCursor });
</script>

<style>
.polars-editor-root {
  display: flex;
  flex-direction: column;
}

.validation-error {
  margin-top: 8px;
  padding: 8px;
  color: #ff5555;
  background-color: rgba(255, 85, 85, 0.1);
  border-radius: 4px;
}

/* Polars-specific syntax highlighting */
.cm-variable.cm-polars {
  color: #50fa7b; /* Polars namespace */
}

.cm-property.cm-polars {
  color: #8be9fd; /* Polars methods */
}

.cm-keyword {
  color: #ff79c6;
}

.cm-function {
  color: #50fa7b;
}

.cm-string {
  color: #f1fa8c;
}

.cm-number {
  color: #bd93f9;
}

.cm-comment {
  color: #6272a4;
}
</style>
