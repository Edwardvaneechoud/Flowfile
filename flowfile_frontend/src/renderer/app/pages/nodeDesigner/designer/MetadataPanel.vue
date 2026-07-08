<template>
  <div class="panel metadata-panel">
    <div class="panel-header">
      <h3>Node Settings</h3>
    </div>
    <div class="panel-content">
      <el-collapse v-model="openGroups">
        <el-collapse-item title="Identity" name="identity">
          <div class="form-stack">
            <div class="form-field">
              <label for="node-name">Node Name *</label>
              <input
                id="node-name"
                v-model="nodeMetadata.node_name"
                type="text"
                class="form-input"
                placeholder="My Custom Node"
              />
            </div>
            <div class="form-field">
              <label for="node-category">Category *</label>
              <el-select
                id="node-category"
                v-model="nodeMetadata.node_category"
                class="category-select"
                data-testid="node-category-select"
                filterable
                allow-create
                default-first-option
                placeholder="Pick or type a category"
              >
                <el-option-group label="Standard">
                  <el-option
                    v-for="category in STANDARD_CATEGORIES"
                    :key="category"
                    :label="category"
                    :value="category"
                  />
                </el-option-group>
                <el-option-group v-if="customCategories.length > 0" label="Your categories">
                  <el-option
                    v-for="category in customCategories"
                    :key="category"
                    :label="category"
                    :value="category"
                  />
                </el-option-group>
              </el-select>
              <p class="field-hint">Becomes the node's group in the palette.</p>
            </div>
            <div class="form-field">
              <IconSelector v-model="nodeMetadata.node_icon" />
            </div>
          </div>
        </el-collapse-item>

        <el-collapse-item title="I/O" name="io">
          <div class="form-stack">
            <div class="form-field">
              <label for="node-inputs">Number of Inputs</label>
              <el-input-number
                id="node-inputs"
                v-model="nodeMetadata.number_of_inputs"
                :min="0"
                :max="10"
                size="small"
              />
            </div>
            <div class="form-field">
              <label for="node-outputs">Number of Outputs</label>
              <el-input-number
                id="node-outputs"
                v-model="nodeMetadata.number_of_outputs"
                :min="1"
                :max="10"
                size="small"
                :disabled="store.environment.kind === 'kernel'"
              />
            </div>
            <div v-if="showOutputNames" class="form-field">
              <label>Output Names</label>
              <div class="output-names-list">
                <div
                  v-for="(name, index) in nodeMetadata.output_names"
                  :key="index"
                  class="output-name-row"
                >
                  <span class="output-handle-label">output-{{ index }}</span>
                  <input
                    :value="name"
                    type="text"
                    class="form-input"
                    placeholder="output name"
                    @input="updateOutputName(index, ($event.target as HTMLInputElement).value)"
                  />
                  <button
                    v-if="nodeMetadata.output_names.length > 1"
                    class="btn-icon-sm"
                    title="Remove output"
                    @click="removeOutputName(index)"
                  >
                    <i class="fa-solid fa-xmark"></i>
                  </button>
                </div>
                <button class="add-output-btn" @click="addOutputName">
                  <i class="fa-solid fa-plus"></i>
                  Add Output
                </button>
              </div>
            </div>
          </div>
        </el-collapse-item>

        <el-collapse-item title="Execution" name="execution">
          <div class="form-stack">
            <ExecutionEnvironmentPicker />
          </div>
        </el-collapse-item>

        <el-collapse-item title="Description" name="description">
          <div class="form-stack">
            <div class="form-field">
              <label for="node-title">Title</label>
              <input
                id="node-title"
                v-model="nodeMetadata.title"
                type="text"
                class="form-input"
                placeholder="My Custom Node"
              />
            </div>
            <div class="form-field">
              <label for="node-intro">Description</label>
              <textarea
                id="node-intro"
                v-model="nodeMetadata.intro"
                class="form-input intro-textarea"
                rows="3"
                placeholder="A custom node for data processing"
              ></textarea>
            </div>
          </div>
        </el-collapse-item>
      </el-collapse>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import axios from "axios";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import IconSelector from "../IconSelector.vue";
import ExecutionEnvironmentPicker from "./ExecutionEnvironmentPicker.vue";

const store = useNodeDesignerStore();
const nodeMetadata = store.nodeMetadata;

const openGroups = ref(["identity", "io", "execution", "description"]);

// Built-in palette groups a custom node may join (slug-matched backend-side),
// plus the default catch-all.
const STANDARD_CATEGORIES = [
  "Custom",
  "Input",
  "Transform",
  "Combine",
  "Aggregate",
  "ML",
  "Output",
];

const customCategories = ref<string[]>([]);

// Output names editor shows for multi-output nodes or the isolated kernel env.
const showOutputNames = computed(
  () => nodeMetadata.number_of_outputs > 1 || store.environment.kind === "kernel",
);

async function fetchExistingCategories() {
  try {
    const response = await axios.get("/user_defined_components/list-custom-nodes");
    const seen = new Set(STANDARD_CATEGORIES.map((c) => c.toLowerCase()));
    const found: string[] = [];
    for (const node of response.data || []) {
      const category = (node.node_category || "").trim();
      if (category && !seen.has(category.toLowerCase())) {
        seen.add(category.toLowerCase());
        found.push(category);
      }
    }
    customCategories.value = found.sort((a, b) => a.localeCompare(b));
  } catch {
    customCategories.value = [];
  }
}

function addOutputName() {
  const nextIndex = nodeMetadata.output_names.length;
  nodeMetadata.output_names.push(`output_${nextIndex}`);
  nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
}

function removeOutputName(index: number) {
  if (nodeMetadata.output_names.length > 1) {
    nodeMetadata.output_names.splice(index, 1);
    nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
  }
}

function updateOutputName(index: number, value: string) {
  nodeMetadata.output_names[index] = value;
}

onMounted(() => {
  fetchExistingCategories();
});
</script>

<style scoped>
.metadata-panel .panel-content {
  padding-top: 0;
}

.form-stack {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  padding: 0.25rem 0.125rem;
}

.form-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.form-field label {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--text-secondary);
}

.form-input {
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--input-bg);
  color: var(--text-primary);
  font-size: 0.875rem;
}

.form-input:focus {
  outline: none;
  border-color: var(--primary-color);
}

.intro-textarea {
  resize: vertical;
  font-family: inherit;
}

.output-names-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.output-name-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.output-handle-label {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--text-secondary);
  white-space: nowrap;
  min-width: 60px;
}

.btn-icon-sm {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  padding: 0;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
  flex-shrink: 0;
}

.btn-icon-sm:hover {
  background: var(--color-background-danger, #fee2e2);
  color: var(--color-text-danger, #dc2626);
  border-color: var(--color-text-danger, #dc2626);
}

.add-output-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.75rem;
  background: transparent;
  color: var(--color-button-primary);
  border: 1px dashed var(--color-button-primary);
  border-radius: var(--border-radius-sm);
  font-size: 0.8125rem;
  cursor: pointer;
  transition: background var(--transition-fast);
  align-self: flex-start;
}

.add-output-btn:hover {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
}

.add-output-btn i {
  font-size: 0.6875rem;
}

.kernel-error {
  font-size: 0.8125rem;
  color: var(--color-text-danger, #dc2626);
}

.form-checkbox {
  width: 18px;
  height: 18px;
  margin-right: 0.5rem;
  accent-color: var(--primary-color, #4a6cf7);
}

.checkbox-label {
  display: flex;
  align-items: center;
  font-size: var(--font-size-sm, 0.875rem);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary, #6b7280);
}
</style>
