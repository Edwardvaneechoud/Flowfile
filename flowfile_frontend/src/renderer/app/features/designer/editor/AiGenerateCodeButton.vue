<template>
  <el-popover
    v-model:visible="open"
    :width="340"
    placement="bottom-start"
    trigger="click"
    popper-class="ai-gen-popover"
    @show="onShow"
  >
    <template #reference>
      <button type="button" class="ai-gen-trigger" :title="title">
        <span class="material-icons">auto_awesome</span>
        <span>Generate</span>
      </button>
    </template>
    <div class="ai-gen-panel" @keydown.stop>
      <div class="ai-gen-panel__title">Generate {{ languageLabel }} code</div>
      <el-input
        ref="promptRef"
        v-model="prompt"
        type="textarea"
        :rows="3"
        :placeholder="placeholder"
        :disabled="store.loading"
        resize="none"
        @keydown.enter.meta.prevent="submit"
        @keydown.enter.ctrl.prevent="submit"
      />
      <p v-if="message" class="ai-gen-panel__msg" :class="{ 'is-error': isError }">{{ message }}</p>
      <div class="ai-gen-panel__actions">
        <el-button size="small" text :disabled="store.loading" @click="open = false">
          Cancel
        </el-button>
        <el-button
          size="small"
          type="primary"
          :loading="store.loading"
          :disabled="!prompt.trim()"
          @click="submit"
        >
          Generate
        </el-button>
      </div>
      <p class="ai-gen-panel__hint">{{ hint }} · ⌘/Ctrl+Enter</p>
    </div>
  </el-popover>
</template>

<script lang="ts" setup>
import { ref, computed, nextTick } from "vue";
import { useAiCodeGeneratorStore } from "../../../stores/ai-code-generator-store";
import type { NodeCodeGenerationResponse } from "../../../api/ai.api";

const props = defineProps<{
  flowId: number;
  nodeId: number | string;
  // "polars_code" | "sql_query" | "python_script" — drives copy only; the
  // backend resolves the real node type from the flow.
  nodeType: string;
}>();

const emit = defineEmits<{ (e: "code-generated", code: string): void }>();

const store = useAiCodeGeneratorStore();

const open = ref(false);
const prompt = ref("");
const message = ref("");
const isError = ref(false);
const promptRef = ref<{ focus?: () => void } | null>(null);

const title = "Generate this node's code from a description";

const languageLabel = computed(() => {
  switch (props.nodeType) {
    case "sql_query":
      return "SQL";
    case "python_script":
      return "Python";
    default:
      return "Polars";
  }
});

const placeholder = computed(() => {
  switch (props.nodeType) {
    case "sql_query":
      return "e.g. Join input_1 and input_2 on customer_id, then sum revenue per region";
    case "python_script":
      return "e.g. Read the input, drop rows with a null email, and publish the result";
    default:
      return "e.g. Filter to active users and add a full_name column";
  }
});

const hint = computed(() =>
  props.nodeType === "python_script"
    ? "Adds a new cell with the generated code"
    : "Replaces the editor with the generated code",
);

const onShow = () => {
  message.value = "";
  isError.value = false;
  nextTick(() => promptRef.value?.focus?.());
};

const reasonText = (res: NodeCodeGenerationResponse): string => {
  if (res.explanation) return res.explanation;
  switch (res.reason) {
    case "timeout":
      return "The model timed out — try again or simplify the request.";
    case "no_code":
      return "Couldn't turn that into code. Try rephrasing.";
    case "empty_prompt":
      return "Describe what this node should do.";
    case "provider_error":
      return "The AI provider returned an error.";
    case "parse_error":
    case "validation_error":
    case "empty_response":
      return "The model returned an unexpected response. Try again.";
    default:
      return "Couldn't generate code. Try rephrasing your request.";
  }
};

const submit = async () => {
  const text = prompt.value.trim();
  if (!text || store.loading) return;
  message.value = "";
  isError.value = false;
  try {
    const res = await store.generateCode({
      flowId: props.flowId,
      nodeId: props.nodeId,
      prompt: text,
    });
    if (res === null) {
      isError.value = true;
      message.value = store.aiDisabled
        ? "AI features are turned off."
        : "Couldn't reach the AI service.";
      return;
    }
    if (res.degraded || !res.code) {
      isError.value = true;
      message.value = reasonText(res);
      return;
    }
    emit("code-generated", res.code);
    prompt.value = "";
    open.value = false;
  } catch {
    isError.value = true;
    message.value = "Something went wrong generating code.";
  }
};
</script>

<style scoped>
.ai-gen-trigger {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px 8px;
  font-size: 12px;
  font-weight: 500;
  color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  border: 1px solid var(--color-accent);
  border-radius: 4px;
  cursor: pointer;
  transition:
    background-color 0.15s,
    color 0.15s;
}

.ai-gen-trigger:hover {
  background-color: var(--color-accent);
  color: var(--color-text-inverse);
}

.ai-gen-trigger .material-icons {
  font-size: 14px;
}

.ai-gen-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.ai-gen-panel__title {
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.ai-gen-panel__msg {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ai-gen-panel__msg.is-error {
  color: var(--color-danger);
}

.ai-gen-panel__actions {
  display: flex;
  justify-content: flex-end;
  gap: 6px;
}

.ai-gen-panel__hint {
  margin: 0;
  font-size: 11px;
  color: var(--color-text-muted);
}
</style>
