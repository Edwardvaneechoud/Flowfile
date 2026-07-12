<template>
  <div class="panel metadata-panel">
    <div class="panel-header">
      <h3>Node Settings</h3>
    </div>
    <div class="panel-content">
      <CollapsibleSection
        title="Identity"
        icon="fa-solid fa-id-card"
        persist-key="nd-meta-identity"
      >
        <template v-if="nodeMetadata.node_name" #actions>
          <span class="name-pill" :title="nodeMetadata.node_name">{{
            nodeMetadata.node_name
          }}</span>
        </template>
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
      </CollapsibleSection>

      <CollapsibleSection
        title="I/O"
        icon="fa-solid fa-arrow-right-arrow-left"
        persist-key="nd-meta-io"
      >
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
              <button class="btn btn-sm btn-secondary add-output-btn" @click="addOutputName">
                <i class="fa-solid fa-plus"></i>
                Add Output
              </button>
            </div>
            <p class="field-hint">
              Your <code>process()</code> must return a dict keyed by these names.
            </p>
          </div>
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title="Execution"
        icon="fa-solid fa-microchip"
        persist-key="nd-meta-execution"
      >
        <div class="form-stack">
          <ExecutionEnvironmentPicker />
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title="Description"
        icon="fa-solid fa-align-left"
        persist-key="nd-meta-description"
      >
        <div class="form-stack">
          <div class="form-field">
            <label for="node-title">Title</label>
            <input
              id="node-title"
              v-model="nodeMetadata.title"
              type="text"
              class="form-input"
              :placeholder="nodeMetadata.node_name || 'My Custom Node'"
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
      </CollapsibleSection>

      <CollapsibleSection
        title="Publishing"
        icon="fa-solid fa-share-nodes"
        persist-key="nd-meta-publishing"
      >
        <div class="form-stack">
          <div class="form-field">
            <label for="node-author">Author</label>
            <input
              id="node-author"
              v-model="nodeMetadata.author"
              type="text"
              class="form-input"
              placeholder="Your name or GitHub handle"
            />
          </div>
          <div class="form-field">
            <label for="node-version">Version</label>
            <input
              id="node-version"
              v-model="nodeMetadata.version"
              type="text"
              class="form-input"
              placeholder="1.0.0"
            />
          </div>
          <div class="form-field">
            <label for="node-tags">Tags</label>
            <el-select
              id="node-tags"
              v-model="nodeMetadata.tags"
              class="tags-select"
              data-testid="node-tags-select"
              multiple
              filterable
              allow-create
              default-first-option
              :multiple-limit="8"
              placeholder="Pick or type search keywords"
            >
              <!-- keep already-selected tags valid options so their labels render -->
              <el-option
                v-for="tag in nodeMetadata.tags"
                :key="`sel-${tag}`"
                :label="tag"
                :value="tag"
              />
              <el-option-group v-if="localTagSuggestions.length > 0" label="Your tags">
                <el-option
                  v-for="tag in localTagSuggestions"
                  :key="`local-${tag}`"
                  :label="tag"
                  :value="tag"
                />
              </el-option-group>
              <el-option-group v-if="communityTagSuggestions.length > 0" label="Community">
                <el-option
                  v-for="tag in communityTagSuggestions"
                  :key="`comm-${tag}`"
                  :label="tag"
                  :value="tag"
                />
              </el-option-group>
            </el-select>
            <p class="field-hint">
              Reuse an existing tag or type a new one — up to 8. Used when sharing the node via the
              community registry.
            </p>
          </div>
        </div>
      </CollapsibleSection>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import axios from "axios";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import { useCommunityNodesStore } from "@/stores/community-nodes-store";
import IconSelector from "../IconSelector.vue";
import ExecutionEnvironmentPicker from "./ExecutionEnvironmentPicker.vue";
import CollapsibleSection from "../../../components/common/CollapsibleSection/CollapsibleSection.vue";

const store = useNodeDesignerStore();
const communityStore = useCommunityNodesStore();
const nodeMetadata = store.nodeMetadata;

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
// Tags already used by the user's local custom nodes (suggestion source).
const localTags = ref<string[]>([]);

const selectedTagsLower = computed(() => new Set(nodeMetadata.tags.map((t) => t.toLowerCase())));

// "Your tags": local tags not already selected on this node.
const localTagSuggestions = computed(() =>
  localTags.value.filter((t) => !selectedTagsLower.value.has(t.toLowerCase())),
);

// "Community": registry tags not already selected and not already in "Your tags".
const communityTagSuggestions = computed(() => {
  const localLower = new Set(localTags.value.map((t) => t.toLowerCase()));
  return communityStore.allTags.filter((t) => {
    const lower = t.toLowerCase();
    return !selectedTagsLower.value.has(lower) && !localLower.has(lower);
  });
});

// Output names editor shows for multi-output nodes or the isolated kernel env.
const showOutputNames = computed(
  () => nodeMetadata.number_of_outputs > 1 || store.environment.kind === "kernel",
);

// One request feeds both the category and tag suggestion lists.
async function fetchLocalNodeMeta() {
  try {
    const response = await axios.get("/user_defined_components/list-custom-nodes");
    const catSeen = new Set(STANDARD_CATEGORIES.map((c) => c.toLowerCase()));
    const foundCats: string[] = [];
    const tagSeen = new Set<string>();
    const foundTags: string[] = [];
    for (const node of response.data || []) {
      const category = (node.node_category || "").trim();
      if (category && !catSeen.has(category.toLowerCase())) {
        catSeen.add(category.toLowerCase());
        foundCats.push(category);
      }
      for (const raw of node.tags || []) {
        const tag = (raw || "").trim();
        if (tag && !tagSeen.has(tag.toLowerCase())) {
          tagSeen.add(tag.toLowerCase());
          foundTags.push(tag);
        }
      }
    }
    customCategories.value = foundCats.sort((a, b) => a.localeCompare(b));
    localTags.value = foundTags.sort((a, b) => a.localeCompare(b));
  } catch {
    customCategories.value = [];
    localTags.value = [];
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
  fetchLocalNodeMeta();
  // Community tags feed the "Community" suggestions. Load once per session
  // (cached, non-fatal — offline just falls back to local + typed tags).
  if (!communityStore.loaded) {
    communityStore.loadIndex(false).catch(() => {
      /* offline / registry unavailable: local + typed tags still work */
    });
  }
});
</script>

<style scoped>
.name-pill {
  display: inline-block;
  max-width: 140px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  vertical-align: middle;
  padding: 0.125rem 0.625rem;
  border-radius: var(--border-radius-full);
  background: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
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
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.field-hint {
  margin: 0.25rem 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.field-hint code {
  padding: 0.05rem 0.3rem;
  border-radius: 3px;
  background: var(--color-background-tertiary);
  font-family: var(--font-family-mono, monospace);
  font-size: 0.95em;
}

.form-input {
  padding: 0.5rem;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-base);
}

.form-input:focus {
  outline: none;
  border-color: var(--color-accent);
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
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
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
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  flex-shrink: 0;
}

.btn-icon-sm:hover {
  background: var(--color-danger-light);
  color: var(--color-danger);
  border-color: var(--color-danger);
}

.add-output-btn {
  align-self: flex-start;
}
</style>
