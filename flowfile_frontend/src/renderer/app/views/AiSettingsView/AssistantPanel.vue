<template>
  <div>
    <div class="mb-3">
      <h2 class="page-title">Assistant</h2>
      <p class="page-description">
        Which models Flowfile's AI features run on, and how the agent applies changes to your flow.
        These settings apply everywhere: the chat drawer, ⌘K, next-node suggestions, schedule text
        and code generation.
      </p>
    </div>

    <!-- Models: pick the provider + model AI features run on. Optionally
         give a task (charts, dashboards, schedules, settings) its own model. -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Models</h3>
      </div>
      <div class="card-content">
        <div v-if="!aiStore.hasConfiguredProvider" class="info-box">
          <i class="fa-solid fa-circle-info"></i>
          <div class="info-body">
            <p><strong>No provider configured yet</strong></p>
            <p>
              Add an API key under
              <button type="button" class="inline-link" @click="emit('navigate', 'providers')">
                Providers</button
              >, or set up
              <button type="button" class="inline-link" @click="emit('navigate', 'providers')">
                On-device AI</button
              >, then choose the model here.
            </p>
          </div>
        </div>

        <template v-else>
          <span class="option-group__label">Main model</span>
          <p class="hint-text models-intro">
            Used by the assistant on the canvas and by every task below that has no model of its
            own. The chat drawer's model switcher changes this same selection.
          </p>

          <div class="model-row">
            <div class="model-field">
              <label class="model-field__label" for="ai-main-provider"> Provider </label>
              <el-select
                id="ai-main-provider"
                size="default"
                popper-class="model-select-dropdown"
                :model-value="aiStore.selectedProvider ?? ''"
                @change="onMainProviderChange"
              >
                <el-option
                  v-for="opt in providerOptions"
                  :key="opt.value"
                  :value="opt.value"
                  :label="opt.label"
                  :disabled="opt.disabled"
                />
              </el-select>
              <p v-if="mainProviderUnconfigured" class="model-field__sub model-field__sub--warn">
                <i class="fa-solid fa-triangle-exclamation"></i>
                {{ providerLabel(aiStore.selectedProvider ?? "") }} has no API key, so AI calls will
                fail.
                <button type="button" class="inline-link" @click="emit('navigate', 'providers')">
                  Add one under Providers
                </button>
                or pick another provider.
              </p>
              <p v-else class="model-field__sub">
                Greyed-out providers need a key first.
                <button type="button" class="inline-link" @click="emit('navigate', 'providers')">
                  Add a provider
                </button>
              </p>
            </div>
            <div class="model-field">
              <label class="model-field__label" for="ai-main-model">Model</label>
              <!-- Local runs a single loaded model — show it read-only and point
                   to the On-device tab rather than offer a duplicate picker. -->
              <div v-if="isLocalComplex" id="ai-main-model" class="model-static">
                <i class="fa-solid fa-microchip"></i>
                <span>{{ localActiveModelName }}</span>
              </div>
              <el-select
                v-else
                id="ai-main-model"
                size="default"
                popper-class="model-select-dropdown"
                :model-value="aiStore.selectedModel ?? ''"
                @change="onMainModelChange"
              >
                <el-option value="" label="Provider default" />
                <el-option v-for="m in mainModels" :key="m" :value="m" :label="m" />
              </el-select>
            </div>
          </div>
          <span class="option-group__label option-group__label--spaced">Models per task</span>
          <p class="hint-text models-intro">
            A task on "Same as main" follows the main model. Give it a provider to use another.
          </p>

          <div class="task-table" role="table" aria-label="Models per task">
            <div class="task-row task-row--head" role="row">
              <span role="columnheader">Task</span>
              <span role="columnheader">Provider</span>
              <span role="columnheader">Model</span>
            </div>
            <template v-for="group in TASK_GROUPS" :key="group.name">
              <div class="task-group" role="row">
                <span role="cell">{{ group.name }}</span>
              </div>
              <div
                v-for="task in group.tasks"
                :key="task.surface"
                class="task-row"
                role="row"
                :data-testid="`ai-task-row-${task.surface}`"
              >
                <div class="task-name" role="cell">
                  <span class="task-name__label">{{ task.label }}</span>
                  <span class="task-name__hint">{{ task.hint }}</span>
                </div>
                <div class="model-field" role="cell">
                  <el-select
                    :id="`ai-task-provider-${task.surface}`"
                    size="default"
                    placeholder="Same as main"
                    popper-class="model-select-dropdown"
                    :aria-label="`${group.name}, ${task.label}: provider`"
                    :model-value="ownPick(task.surface)?.provider ?? ''"
                    @change="(value: string) => setTaskProvider(task.surface, value)"
                  >
                    <el-option value="" label="Same as main" />
                    <el-option
                      v-for="opt in providerOptions"
                      :key="opt.value"
                      :value="opt.value"
                      :label="opt.label"
                      :disabled="opt.disabled"
                    />
                  </el-select>
                  <p
                    v-if="isUnconfigured(ownPick(task.surface)?.provider ?? null)"
                    class="model-field__sub model-field__sub--warn"
                  >
                    <i class="fa-solid fa-triangle-exclamation"></i>
                    This provider has no API key, so these calls will fail.
                  </p>
                </div>
                <div class="model-field" role="cell">
                  <el-select
                    v-if="ownPick(task.surface)"
                    :id="`ai-task-model-${task.surface}`"
                    size="default"
                    placeholder="Provider default"
                    popper-class="model-select-dropdown"
                    :aria-label="`${group.name}, ${task.label}: model`"
                    :model-value="ownPick(task.surface)?.model ?? ''"
                    @change="(value: string) => setTaskModel(task.surface, value)"
                  >
                    <el-option value="" label="Provider default" />
                    <el-option
                      v-for="m in taskModels(task.surface)"
                      :key="m"
                      :value="m"
                      :label="m"
                    />
                  </el-select>
                  <span v-else class="task-follows">{{ mainModelText }}</span>
                </div>
              </div>
            </template>
          </div>
        </template>
      </div>
    </div>

    <div class="card mb-3" data-testid="ai-data-sharing">
      <div class="card-header">
        <h3 class="card-title">Data sent to the AI</h3>
      </div>
      <div class="card-content">
        <p class="hint-text models-intro">
          The AI always gets names: the table, its columns and their types, plus your prompt. With
          this on it also gets data values: row counts, summary statistics and some of the values,
          masked for emails and card numbers. Today this applies to charts and dashboards.
        </p>
        <div class="option-list">
          <label class="option" :class="{ 'option--disabled': !aiStore.sendDataAllowed }">
            <input
              type="checkbox"
              name="ai-share-data"
              data-testid="ai-share-data"
              :checked="aiStore.sendsData"
              :disabled="!aiStore.sendDataAllowed"
              @change="aiStore.setShareData(($event.target as HTMLInputElement).checked)"
            />
            <span class="option__body">
              <span class="option__name">Send data values</span>
              <span class="option__desc">
                {{
                  aiStore.sendDataAllowed
                    ? "Your choice, on this device."
                    : "Turned off on this server (FLOWFILE_AI_SEND_DATA)."
                }}
              </span>
            </span>
          </label>
        </div>
      </div>
    </div>

    <!-- Simple build output. Applies to every provider; the on-device model is
         the one that gains most from the code form. -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Simple build</h3>
      </div>
      <div class="card-content">
        <p class="hint-text models-intro">
          What the model writes when the chat drawer's Simple build generates a flow from one
          message. Flowfile turns either form into nodes for you to add to the canvas; nothing the
          model writes is executed.
        </p>
        <span class="option-group__label">Output</span>
        <div class="option-list">
          <label v-for="output in SIMPLE_BUILD_OUTPUTS" :key="output.value" class="option">
            <input
              type="radio"
              name="ai-simple-build-output"
              :value="output.value"
              :checked="aiStore.simpleBuildOutput === output.value"
              @change="aiStore.setSimpleBuildOutput(output.value)"
            />
            <span class="option__body">
              <span class="option__name">{{ output.name }}</span>
              <span class="option__desc">{{ output.description }}</span>
            </span>
          </label>
        </div>
      </div>
    </div>

    <!-- Agent behaviour. Used whenever Send dispatches an agent run (Auto-agent
         or Agent mode), including auto-promotion from chat. -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Agent</h3>
      </div>
      <div class="card-content">
        <p class="hint-text models-intro">
          How the agent plans and executes changes when a message runs as an agent (Auto-agent or
          Agent mode in the chat drawer).
        </p>
        <!-- The agent can't run on the local model at all (no tool calling), so
             its controls are greyed out rather than pretending to apply. -->
        <p v-if="isLocalComplex" class="agent-note">
          <i class="fa-solid fa-circle-info"></i>
          Not available with On-device AI. The Agent builds step by step through tool calls, which
          this small model doesn't support, so the chat offers only Chat and Simple build. The
          options below are greyed out until a cloud provider or Ollama is the provider above.
        </p>

        <span class="option-group__label">Agent variant</span>
        <div class="option-list">
          <label
            v-for="variant in AGENT_VARIANTS"
            :key="variant.value"
            class="option"
            :class="{ 'option--disabled': isLocalComplex }"
          >
            <input
              type="radio"
              name="ai-agent-surface"
              :value="variant.value"
              :checked="aiStore.selectedAgentSurface === variant.value"
              :disabled="isLocalComplex"
              @change="aiStore.setSelectedAgentSurface(variant.value)"
            />
            <span class="option__body">
              <span class="option__name">{{ variant.name }}</span>
              <span class="option__desc">{{ variant.description }}</span>
            </span>
          </label>
        </div>

        <!-- Verify completion is an orthogonal post-run check, not a fourth
             variant — hence its own group. -->
        <span class="option-group__label option-group__label--spaced">Verification</span>
        <div class="option-list">
          <label class="option" :class="{ 'option--disabled': isLocalComplex }">
            <input
              type="checkbox"
              name="ai-agent-verify-plan"
              :checked="aiStore.verifyPlanCompletion"
              :disabled="isLocalComplex"
              @change="onVerifyChange"
            />
            <span class="option__body">
              <span class="option__name">Verify plan completion</span>
              <span class="option__desc">
                After the agent finishes, double-check that every plan step was applied. Adds one
                LLM round per agent run. Useful when multi-step plans (e.g. add a node mid-flow plus
                the rewires) sometimes terminate after step 1.
              </span>
            </span>
          </label>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { ElOption, ElSelect } from "element-plus";
import { useAiStore } from "../../stores/ai-store";
import type {
  PersistedAgentSurface,
  PersistedSimpleBuildOutput,
} from "../../stores/ai-store-persistence";
import { useTaskModels, withSelected } from "../../composables/useTaskModels";
import { LOCAL_PROVIDER_ID } from "./localModelApi";

const emit = defineEmits<{
  (e: "navigate", tab: string): void;
}>();

const aiStore = useAiStore();

const SIMPLE_BUILD_OUTPUTS: ReadonlyArray<{
  value: PersistedSimpleBuildOutput;
  name: string;
  description: string;
}> = [
  {
    value: "code",
    name: "FlowFrame code",
    description:
      "The model writes the flow as Flowfile's Python dialect (the same code the Notebook pane shows) and Flowfile reads it into nodes without running it. The code shows in the chat bubble. Faster and more reliable, especially on small and on-device models. The default.",
  },
  {
    value: "json",
    name: "Node settings",
    description:
      "The model writes each node's settings directly, as in earlier releases. Nothing is validated until you add the flow to the canvas.",
  },
];

const AGENT_VARIANTS: ReadonlyArray<{
  value: PersistedAgentSurface;
  name: string;
  description: string;
}> = [
  {
    value: "agent_live",
    name: "Live (REPL)",
    description:
      "Applies each step live to the canvas, runs the affected subgraph, retries on failure. Every step commits immediately, no staged diff. The default.",
  },
  {
    value: "agent_staged",
    name: "Staged",
    description:
      "Multi-stage planner. Reviews each step before staging a diff for you to accept. Reliable on small / local models.",
  },
  {
    value: "agent_complex",
    name: "Single-shot full",
    description:
      "Exposes the entire tool catalog at once. Best for large models that handle complex prompts well.",
  },
];

const {
  providerLabel,
  providerOptions,
  isUnconfigured,
  ownPick,
  taskModels,
  setTaskProvider,
  setTaskModel,
} = useTaskModels();

const mainModels = computed(() =>
  withSelected(aiStore.modelsForProvider(aiStore.selectedProvider), aiStore.selectedModel),
);

// When a tier's provider is On-device AI, its model isn't a separate pick — it's
// whatever the single local server has loaded (chosen on the On-device tab).
const isLocalComplex = computed(() => aiStore.selectedProvider === LOCAL_PROVIDER_ID);
const localActiveModelName = computed(
  () => aiStore.localModelStatus?.modelName ?? "the on-device model",
);
const mainProviderUnconfigured = computed(() => isUnconfigured(aiStore.selectedProvider));

const onMainProviderChange = (value: string): void => {
  if (value) aiStore.setSelectedProvider(value);
};

const onMainModelChange = (value: string): void => {
  aiStore.setSelectedModel(value || null);
};

interface TaskModel {
  surface: string;
  label: string;
  hint: string;
}

// Tasks that can run on their own model, by where they are used; `surface` goes to `resolveSurface`.
const TASK_GROUPS: ReadonlyArray<{ name: string; tasks: ReadonlyArray<TaskModel> }> = [
  {
    name: "Flows",
    tasks: [
      {
        surface: "settings_autocomplete",
        label: "Node settings",
        hint: "Suggestions inside a node's settings, such as join keys.",
      },
      { surface: "cron", label: "Schedules", hint: "Turns a schedule in words into cron." },
    ],
  },
  {
    name: "Semantic models",
    tasks: [
      {
        surface: "semantic_ask",
        label: "Questions",
        hint: "Answers a question about your data by picking an approved metric.",
      },
      {
        surface: "semantic_suggest",
        label: "Suggestions",
        hint: "Writes descriptions and proposes metrics, joins and column roles.",
      },
    ],
  },
  {
    name: "Dashboards",
    tasks: [
      { surface: "chart", label: "Charts", hint: "Builds a chart from a description." },
      {
        surface: "dashboard",
        label: "Dashboard pages",
        hint: "Plans and writes a page; each chart on it is built by the Charts model.",
      },
    ],
  },
];

/** What a task on "Same as main" runs on, shown in its Model column. */
const mainModelText = computed(() => {
  const provider = providerLabel(aiStore.selectedProvider ?? "");
  const model = isLocalComplex.value ? localActiveModelName.value : aiStore.selectedModel;
  return model ? `${provider} · ${model}` : provider;
});

const onVerifyChange = (event: Event): void => {
  aiStore.setVerifyPlanCompletion((event.target as HTMLInputElement).checked);
};
</script>

<style scoped>
.models-intro {
  margin-top: 0;
  margin-bottom: var(--spacing-4);
}

.model-row {
  display: flex;
  gap: var(--spacing-4);
  flex-wrap: wrap;
}

.task-table {
  display: flex;
  flex-direction: column;
}

.task-row {
  display: grid;
  grid-template-columns: minmax(200px, 1.3fr) minmax(160px, 1fr) minmax(160px, 1fr);
  gap: var(--spacing-4);
  align-items: start;
  padding: var(--spacing-2) 0;
}

.task-row .model-field {
  max-width: none;
  min-width: 0;
}

.task-row--head {
  padding-bottom: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.task-group {
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) 0 var(--spacing-1);
  border-top: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.task-name {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding-top: var(--spacing-1);
}

.task-name__label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.task-name__hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.task-follows {
  display: flex;
  align-items: center;
  min-height: 32px;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

@media (max-width: 720px) {
  .task-row {
    grid-template-columns: 1fr;
    gap: var(--spacing-2);
  }

  .task-row--head {
    display: none;
  }
}

.model-field {
  display: flex;
  flex-direction: column;
  flex: 1 1 240px;
  min-width: 180px;
  max-width: 340px;
}

.model-field__label {
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

/* Theme Element Plus's select trigger to the app tokens (it ignores plain
   class styles): a soft filled control that lifts to a teal focus ring. */
.model-field :deep(.el-select__wrapper) {
  background-color: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  box-shadow: inset 0 0 0 1px var(--color-border-primary);
  transition:
    box-shadow 0.15s ease,
    background-color 0.15s ease;
}

.model-field :deep(.el-select__wrapper:hover) {
  background-color: var(--color-background-primary);
  box-shadow: inset 0 0 0 1px var(--color-border-secondary);
}

.model-field :deep(.el-select__wrapper.is-focused) {
  background-color: var(--color-background-primary);
  box-shadow:
    inset 0 0 0 1px var(--color-accent),
    0 0 0 3px var(--color-accent-subtle);
}

.model-field :deep(.el-select__placeholder:not(.is-transparent)) {
  color: var(--color-text-primary);
  font-weight: 300;
}

.model-field :deep(.el-select__caret) {
  color: var(--color-text-tertiary);
}

/* Read-only model value shown when a tier's provider is On-device AI. Mirrors
   the select trigger's shape so the row stays aligned with cloud tiers. */
.model-static {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-height: 32px;
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  box-shadow: inset 0 0 0 1px var(--color-border-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.model-static i {
  color: var(--color-accent);
}

.model-field__sub {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.model-field__sub--warn {
  color: var(--color-warning, #ca8a04);
}

.model-field__sub--warn i {
  margin-right: 2px;
}

.agent-note {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: 0 0 var(--spacing-4);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-muted);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.agent-note i {
  color: var(--color-accent);
  margin-top: 2px;
}

.option-group__label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-tertiary);
}

.option-group__label--spaced {
  margin-top: var(--spacing-4);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}

.option-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.option {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  cursor: pointer;
  transition: border-color var(--transition-fast);
}

.option:hover {
  border-color: var(--color-border-secondary);
}

.option:has(input:checked) {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
}

/* Inert while On-device AI is the provider: visible, but clearly not in play. */
.option--disabled,
.option--disabled:hover {
  opacity: 0.5;
  cursor: not-allowed;
  border-color: var(--color-border-light);
}

.option--disabled:has(input:checked) {
  background-color: var(--color-background-secondary);
  border-color: var(--color-border-light);
}

.option--disabled input {
  cursor: not-allowed;
}

.option input {
  flex-shrink: 0;
  margin-top: 3px;
  cursor: pointer;
}

.option__body {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.option__name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.option__desc {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  line-height: 1.45;
}
</style>
