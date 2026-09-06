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
         split off a cheaper model for lightweight tasks. -->
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
          <p class="hint-text models-intro">
            The chat drawer's model switcher changes this same selection — it is a shortcut, not a
            separate setting.
          </p>

          <!-- Main selection — drives everything unless a split is enabled. -->
          <div class="model-row">
            <div class="model-field">
              <label class="model-field__label" for="ai-main-provider">
                {{ aiStore.splitModels ? "Complex provider" : "Provider" }}
              </label>
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
          <p class="model-tier__hint">
            {{
              aiStore.splitModels
                ? "Heavier tasks: chat, agent, next-node suggestions, command palette (⌘K), code generation."
                : "Used by every AI feature."
            }}
          </p>

          <!-- Simple tier — own provider + model, shown only when split on. -->
          <div v-if="aiStore.splitModels" class="model-row model-row--simple">
            <div class="model-field">
              <label class="model-field__label" for="ai-simple-provider">Simple provider</label>
              <el-select
                id="ai-simple-provider"
                size="default"
                popper-class="model-select-dropdown"
                :model-value="simpleProviderName ?? ''"
                @change="onSimpleProviderChange"
              >
                <el-option
                  v-for="opt in providerOptions"
                  :key="opt.value"
                  :value="opt.value"
                  :label="opt.label"
                  :disabled="opt.disabled"
                />
              </el-select>
              <p v-if="simpleProviderUnconfigured" class="model-field__sub model-field__sub--warn">
                <i class="fa-solid fa-triangle-exclamation"></i>
                {{ providerLabel(simpleProviderName ?? "") }} has no API key, so simple-task calls
                will fail.
              </p>
            </div>
            <div class="model-field">
              <label class="model-field__label" for="ai-simple-model">Model</label>
              <div v-if="isLocalSimple" id="ai-simple-model" class="model-static">
                <i class="fa-solid fa-microchip"></i>
                <span>{{ localActiveModelName }}</span>
              </div>
              <el-select
                v-else
                id="ai-simple-model"
                size="default"
                popper-class="model-select-dropdown"
                :model-value="aiStore.simpleModel ?? ''"
                @change="onSimpleModelChange"
              >
                <el-option value="" label="Provider default" />
                <el-option v-for="m in simpleModels" :key="m" :value="m" :label="m" />
              </el-select>
            </div>
          </div>
          <p v-if="aiStore.splitModels" class="model-tier__hint">
            Lighter tasks: cron text→schedule and settings autocomplete.
          </p>

          <el-checkbox
            class="model-split-toggle"
            :model-value="aiStore.splitModels"
            @change="onSplitToggle"
          >
            Use a separate, cheaper model for simple tasks
          </el-checkbox>
        </template>
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
import { ElCheckbox, ElOption, ElSelect } from "element-plus";
import { useAiStore } from "../../stores/ai-store";
import type { PersistedAgentSurface } from "../../stores/ai-store-persistence";
import { LOCAL_PROVIDER_ID, LOCAL_PROVIDER_LABEL } from "./localModelApi";

const emit = defineEmits<{
  (e: "navigate", tab: string): void;
}>();

const aiStore = useAiStore();

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

// Keep the currently-selected model as a valid <option> even if it isn't in
// the provider's curated list (so the select never renders blank).
const _withSelected = (list: string[], selected: string | null): string[] =>
  selected && !list.includes(selected) ? [...list, selected] : list;

const mainModels = computed(() =>
  _withSelected(aiStore.modelsForProvider(aiStore.selectedProvider), aiStore.selectedModel),
);

const simpleProviderName = computed(() => aiStore.simpleProvider ?? aiStore.selectedProvider);

const simpleModels = computed(() =>
  _withSelected(aiStore.modelsForProvider(simpleProviderName.value), aiStore.simpleModel),
);

// When a tier's provider is On-device AI, its model isn't a separate pick — it's
// whatever the single local server has loaded (chosen on the On-device tab).
const isLocalComplex = computed(() => aiStore.selectedProvider === LOCAL_PROVIDER_ID);
const isLocalSimple = computed(() => simpleProviderName.value === LOCAL_PROVIDER_ID);
const localActiveModelName = computed(
  () => aiStore.localModelStatus?.modelName ?? "the on-device model",
);
// Humanize the synthetic ``local`` provider's label (wire id stays "local").
const providerLabel = (name: string): string =>
  name === LOCAL_PROVIDER_ID ? LOCAL_PROVIDER_LABEL : name;

// Every provider is listed so the user can see what exists, not just what
// already has a key; unconfigured ones are greyed out (they would fail at
// call time) and the hint below the select points at the Providers tab.
const providerOptions = computed(() =>
  aiStore.providers.map((p) => ({
    value: p.provider,
    label:
      p.status === "unconfigured"
        ? `${providerLabel(p.provider)} (not configured)`
        : p.status === "env_fallback"
          ? `${providerLabel(p.provider)} (env key)`
          : providerLabel(p.provider),
    disabled: p.status === "unconfigured",
  })),
);

// A persisted selection can point at a provider whose key was since removed
// (or never existed on this install); warn instead of silently failing later.
const isUnconfigured = (name: string | null): boolean =>
  name !== null &&
  aiStore.providers.some((p) => p.provider === name && p.status === "unconfigured");
const mainProviderUnconfigured = computed(() => isUnconfigured(aiStore.selectedProvider));
const simpleProviderUnconfigured = computed(() => isUnconfigured(simpleProviderName.value));

// el-checkbox emits CheckboxValueType (string | number | boolean).
const onSplitToggle = (checked: unknown): void => {
  aiStore.setSplitModels(checked === true);
};

const onMainProviderChange = (value: string): void => {
  if (value) aiStore.setSelectedProvider(value);
};

const onMainModelChange = (value: string): void => {
  aiStore.setSelectedModel(value || null);
};

const onSimpleProviderChange = (value: string): void => {
  if (value) aiStore.setSimpleProvider(value);
};

const onSimpleModelChange = (value: string): void => {
  aiStore.setSimpleModel(value || null);
};

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

.model-row--simple {
  margin-top: var(--spacing-4);
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

.model-tier__hint {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
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

.model-split-toggle {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  cursor: pointer;
}

.model-split-toggle :deep(.el-checkbox__label) {
  font-weight: 400;
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
