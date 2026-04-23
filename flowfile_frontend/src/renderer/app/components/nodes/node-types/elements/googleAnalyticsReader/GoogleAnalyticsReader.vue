<template>
  <div v-if="dataLoaded && nodeGaReader" class="ga-reader-container">
    <generic-node-settings
      v-model="nodeGaReader"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="form-group">
          <label for="ga-connection-select">Google Analytics Connection</label>
          <div v-if="connectionsAreLoading" class="loading-state">
            <div class="loading-spinner"></div>
            <p>Loading connections...</p>
          </div>
          <div v-else>
            <select
              id="ga-connection-select"
              v-model="nodeGaReader.google_analytics_settings.ga_connection_name"
              class="form-control minimal-select"
              @change="handleConnectionChange"
            >
              <option value="">Select a connection...</option>
              <option
                v-for="conn in gaConnections"
                :key="conn.connectionName"
                :value="conn.connectionName"
              >
                {{ conn.connectionName }}
                <template v-if="conn.defaultPropertyId">
                  — default property {{ conn.defaultPropertyId }}
                </template>
              </option>
            </select>
            <div
              v-if="!nodeGaReader.google_analytics_settings.ga_connection_name"
              class="helper-text"
            >
              <i class="fa-solid fa-info-circle"></i>
              Create a Google Analytics connection in the Connections manager first.
            </div>
          </div>
        </div>
      </div>

      <div v-if="nodeGaReader.google_analytics_settings.ga_connection_name" class="listbox-wrapper">
        <h4 class="section-subtitle">Report</h4>

        <div class="form-group">
          <label for="ga-property-id">GA4 Property ID</label>
          <input
            id="ga-property-id"
            v-model="nodeGaReader.google_analytics_settings.property_id"
            type="text"
            class="form-control"
            placeholder="e.g. 123456789"
          />
        </div>

        <div class="form-group">
          <label for="ga-range-preset">Quick Range</label>
          <select
            id="ga-range-preset"
            v-model="rangePreset"
            class="form-control"
            @change="applyRangePreset"
          >
            <option value="custom">Custom...</option>
            <option value="yesterday">Yesterday only</option>
            <option value="today">Today so far</option>
            <option value="last_7">Last 7 days</option>
            <option value="last_14">Last 14 days</option>
            <option value="last_28">Last 28 days</option>
            <option value="last_30">Last 30 days</option>
            <option value="last_90">Last 90 days</option>
          </select>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            Presets use GA4's relative date tokens (e.g. <code>30daysAgo</code>) so every flow run
            evaluates a rolling window against the current date.
          </div>
        </div>

        <div class="form-row">
          <div class="form-group half">
            <label for="ga-start-date">Start Date</label>
            <input
              id="ga-start-date"
              v-model="nodeGaReader.google_analytics_settings.start_date"
              type="text"
              class="form-control"
              placeholder="7daysAgo or YYYY-MM-DD"
              @input="rangePreset = 'custom'"
            />
          </div>
          <div class="form-group half">
            <label for="ga-end-date">End Date</label>
            <input
              id="ga-end-date"
              v-model="nodeGaReader.google_analytics_settings.end_date"
              type="text"
              class="form-control"
              placeholder="yesterday or YYYY-MM-DD"
              @input="rangePreset = 'custom'"
            />
          </div>
        </div>

        <div class="form-group">
          <label for="ga-metrics">Metrics</label>
          <el-select
            id="ga-metrics"
            v-model="nodeGaReader.google_analytics_settings.metrics"
            multiple
            filterable
            allow-create
            default-first-option
            placeholder="Pick metrics (e.g. sessions, totalUsers)"
            class="ga-multiselect"
          >
            <el-option-group
              v-for="group in metricGroups"
              :key="group.label"
              :label="group.label"
            >
              <el-option
                v-for="opt in group.options"
                :key="opt.name"
                :value="opt.name"
                :label="opt.label"
              >
                <span class="option-label">{{ opt.label }}</span>
                <span class="option-api-name">{{ opt.name }}</span>
              </el-option>
            </el-option-group>
          </el-select>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            Type to filter, or enter a custom metric name (e.g.
            <code>customEvent:my_param</code>). See the
            <a
              href="https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema"
              target="_blank"
              rel="noopener"
              >API schema reference</a
            >.
          </div>
        </div>

        <div class="form-group">
          <label for="ga-dimensions">Dimensions</label>
          <el-select
            id="ga-dimensions"
            v-model="nodeGaReader.google_analytics_settings.dimensions"
            multiple
            filterable
            allow-create
            default-first-option
            placeholder="Pick dimensions (e.g. date, pagePath, eventName)"
            class="ga-multiselect"
          >
            <el-option-group
              v-for="group in dimensionGroups"
              :key="group.label"
              :label="group.label"
            >
              <el-option
                v-for="opt in group.options"
                :key="opt.name"
                :value="opt.name"
                :label="opt.label"
              >
                <span class="option-label">{{ opt.label }}</span>
                <span class="option-api-name">{{ opt.name }}</span>
              </el-option>
            </el-option-group>
          </el-select>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            For event details, pick from the <strong>Page / screen</strong>,
            <strong>Event</strong> or <strong>Links &amp; files</strong> groups — e.g.
            <code>pagePath</code>, <code>pageTitle</code>, <code>eventName</code>,
            <code>linkUrl</code>.
          </div>
        </div>

        <div class="form-group">
          <label for="ga-limit">Row Limit (optional)</label>
          <input
            id="ga-limit"
            v-model.number="limitModel"
            type="number"
            class="form-control"
            min="1"
            placeholder="Leave blank for all rows"
          />
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            Leave blank to fetch every row GA returns (paginated in 100k-row chunks).
          </div>
        </div>
      </div>

      <!-- Filters -->
      <div v-if="nodeGaReader.google_analytics_settings.ga_connection_name" class="listbox-wrapper">
        <h4 class="section-subtitle">Filters</h4>
        <div
          v-if="
            nodeGaReader.google_analytics_settings.metrics.length === 0 &&
            nodeGaReader.google_analytics_settings.dimensions.length === 0
          "
          class="helper-text"
        >
          <i class="fa-solid fa-info-circle"></i>
          Select at least one metric or dimension before adding filters.
        </div>
        <div v-else>
          <div
            v-for="(filter, index) in nodeGaReader.google_analytics_settings.filters"
            :key="index"
            class="filter-row"
          >
            <select
              :value="filter.field"
              class="form-control filter-field"
              @change="onFilterFieldChange(index, ($event.target as HTMLSelectElement).value)"
            >
              <option value="">Field...</option>
              <optgroup
                v-if="nodeGaReader.google_analytics_settings.dimensions.length"
                label="Dimensions"
              >
                <option
                  v-for="dim in nodeGaReader.google_analytics_settings.dimensions"
                  :key="'dim-' + dim"
                  :value="dim"
                >
                  {{ dim }}
                </option>
              </optgroup>
              <optgroup
                v-if="nodeGaReader.google_analytics_settings.metrics.length"
                label="Metrics"
              >
                <option
                  v-for="met in nodeGaReader.google_analytics_settings.metrics"
                  :key="'met-' + met"
                  :value="met"
                >
                  {{ met }}
                </option>
              </optgroup>
            </select>

            <select v-model="filter.operator" class="form-control filter-operator">
              <option v-for="op in operatorsFor(filter.field)" :key="op.value" :value="op.value">
                {{ op.label }}
              </option>
            </select>

            <input
              v-model="filter.value"
              type="text"
              class="form-control filter-value"
              :placeholder="valuePlaceholderFor(filter.operator)"
            />

            <button
              type="button"
              class="btn btn-icon"
              title="Case-sensitive match"
              :class="{ active: filter.case_sensitive }"
              :disabled="isMetricField(filter.field)"
              @click="filter.case_sensitive = !filter.case_sensitive"
            >
              Aa
            </button>

            <button
              type="button"
              class="btn btn-icon btn-danger"
              title="Remove filter"
              @click="removeFilter(index)"
            >
              <i class="fa-solid fa-trash-alt"></i>
            </button>
          </div>

          <button type="button" class="btn btn-secondary btn-add-filter" @click="addFilter">
            <i class="fa-solid fa-plus"></i> Add Filter
          </button>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            Multiple filters on the same kind (dimension or metric) are AND-combined. Use
            <code>in_list</code> / <code>between</code> with comma-separated values.
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { computed, ref } from "vue";
import { ElMessage, ElOption, ElOptionGroup, ElSelect } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { createNodeGoogleAnalyticsReader } from "./utils";
import { GA4_DIMENSION_GROUPS, GA4_METRIC_GROUPS } from "./ga4Catalog";
import type {
  GoogleAnalyticsFilter,
  NodeGoogleAnalyticsReader,
} from "../../../../../types/node.types";
import { fetchGoogleAnalyticsConnections } from "../../../../../views/GoogleAnalyticsConnectionView/api";
import type { GoogleAnalyticsConnectionInterface } from "../../../../../views/GoogleAnalyticsConnectionView/GoogleAnalyticsConnectionTypes";

const dimensionGroups = GA4_DIMENSION_GROUPS;
const metricGroups = GA4_METRIC_GROUPS;

interface Props {
  nodeId: number;
}

defineProps<Props>();

const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const nodeGaReader = ref<NodeGoogleAnalyticsReader | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGaReader,
});

const gaConnections = ref<GoogleAnalyticsConnectionInterface[]>([]);
const connectionsAreLoading = ref(false);

const limitModel = computed({
  get: () => nodeGaReader.value?.google_analytics_settings.limit ?? null,
  set: (value) => {
    if (!nodeGaReader.value) return;
    nodeGaReader.value.google_analytics_settings.limit =
      value === null || Number.isNaN(value) || value === 0 ? null : Number(value);
  },
});

// --- Date-range presets ----------------------------------------------------

const RANGE_PRESETS: Record<string, { start: string; end: string }> = {
  yesterday: { start: "yesterday", end: "yesterday" },
  today: { start: "today", end: "today" },
  last_7: { start: "7daysAgo", end: "yesterday" },
  last_14: { start: "14daysAgo", end: "yesterday" },
  last_28: { start: "28daysAgo", end: "yesterday" },
  last_30: { start: "30daysAgo", end: "yesterday" },
  last_90: { start: "90daysAgo", end: "yesterday" },
};

const rangePreset = ref<string>("custom");

const detectPreset = (start: string, end: string): string => {
  for (const [key, range] of Object.entries(RANGE_PRESETS)) {
    if (range.start === start && range.end === end) return key;
  }
  return "custom";
};

const applyRangePreset = () => {
  if (!nodeGaReader.value) return;
  if (rangePreset.value === "custom") return;
  const preset = RANGE_PRESETS[rangePreset.value];
  if (!preset) return;
  nodeGaReader.value.google_analytics_settings.start_date = preset.start;
  nodeGaReader.value.google_analytics_settings.end_date = preset.end;
};

// --- Filter helpers --------------------------------------------------------

const DIMENSION_OPERATORS = [
  { value: "equals", label: "equals" },
  { value: "not_equals", label: "not equals" },
  { value: "contains", label: "contains" },
  { value: "begins_with", label: "begins with" },
  { value: "ends_with", label: "ends with" },
  { value: "regex", label: "matches regex" },
  { value: "in_list", label: "in list" },
  { value: "not_in_list", label: "not in list" },
];

const METRIC_OPERATORS = [
  { value: "equals", label: "=" },
  { value: "not_equals", label: "≠" },
  { value: "less_than", label: "<" },
  { value: "less_equal", label: "≤" },
  { value: "greater_than", label: ">" },
  { value: "greater_equal", label: "≥" },
  { value: "between", label: "between" },
];

const isMetricField = (field: string): boolean =>
  !!nodeGaReader.value?.google_analytics_settings.metrics.includes(field);

const isDimensionField = (field: string): boolean =>
  !!nodeGaReader.value?.google_analytics_settings.dimensions.includes(field);

const operatorsFor = (field: string) => {
  if (isMetricField(field)) return METRIC_OPERATORS;
  if (isDimensionField(field)) return DIMENSION_OPERATORS;
  // Default while no field chosen yet — dimension set is more common.
  return DIMENSION_OPERATORS;
};

const valuePlaceholderFor = (operator: string): string => {
  if (operator === "in_list" || operator === "not_in_list") return "NL, BE, DE";
  if (operator === "between") return "10, 100";
  if (operator === "regex") return "^home.*";
  return "value";
};

const addFilter = () => {
  if (!nodeGaReader.value) return;
  nodeGaReader.value.google_analytics_settings.filters.push({
    field: "",
    operator: "equals",
    value: "",
    case_sensitive: false,
  } as GoogleAnalyticsFilter);
};

const removeFilter = (index: number) => {
  if (!nodeGaReader.value) return;
  nodeGaReader.value.google_analytics_settings.filters.splice(index, 1);
};

const onFilterFieldChange = (index: number, newField: string) => {
  if (!nodeGaReader.value) return;
  const filter = nodeGaReader.value.google_analytics_settings.filters[index];
  filter.field = newField;
  // Reset operator if the current one isn't valid for the new field's category.
  const validOps = operatorsFor(newField).map((op) => op.value);
  if (!validOps.includes(filter.operator)) {
    filter.operator = "equals";
  }
};

const handleConnectionChange = () => {
  if (!nodeGaReader.value) return;
  const name = nodeGaReader.value.google_analytics_settings.ga_connection_name;
  const conn = gaConnections.value.find((c) => c.connectionName === name);
  if (conn?.defaultPropertyId && !nodeGaReader.value.google_analytics_settings.property_id) {
    nodeGaReader.value.google_analytics_settings.property_id = conn.defaultPropertyId;
  }
};

const fetchConnections = async () => {
  connectionsAreLoading.value = true;
  try {
    gaConnections.value = await fetchGoogleAnalyticsConnections();
  } catch (error) {
    console.error("Error fetching GA connections:", error);
    ElMessage.error("Failed to load Google Analytics connections");
  } finally {
    connectionsAreLoading.value = false;
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchConnections(),
    ]);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeGaReader.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeGoogleAnalyticsReader(nodeStore.flow_id, nodeId);
      // Ensure the filters array exists when loading flows saved before the
      // filters feature shipped.
      if (nodeGaReader.value && !nodeGaReader.value.google_analytics_settings.filters) {
        nodeGaReader.value.google_analytics_settings.filters = [];
      }
      // Pick the right preset for the restored start/end dates.
      if (nodeGaReader.value) {
        rangePreset.value = detectPreset(
          nodeGaReader.value.google_analytics_settings.start_date,
          nodeGaReader.value.google_analytics_settings.end_date,
        );
      }
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading GA reader node data:", error);
    dataLoaded.value = false;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.ga-reader-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: #4a5568;
}

.form-row {
  display: flex;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
  width: 100%;
  box-sizing: border-box;
}

.half {
  flex: 1;
  min-width: 0;
  max-width: calc(50% - 0.375rem);
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
}

.form-group {
  margin-bottom: 0.75rem;
  width: 100%;
}

label {
  display: block;
  margin-bottom: 0.25rem;
  font-size: 0.875rem;
  font-weight: 500;
  color: #4a5568;
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.helper-text {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.8125rem;
  color: #718096;
}

.helper-text i {
  color: #4299e1;
  font-size: 0.875rem;
}

.helper-text a {
  color: #4299e1;
  text-decoration: underline;
}

.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
  padding: 1rem;
}

.loading-state p {
  margin: 0;
  color: #718096;
  font-size: 0.875rem;
}

.loading-spinner {
  width: 2rem;
  height: 2rem;
  border: 2px solid #e2e8f0;
  border-top-color: #4299e1;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

/* Metrics / dimensions multiselect */
.ga-multiselect {
  width: 100%;
}

.ga-multiselect :deep(.el-select__tags) {
  max-height: 160px;
  overflow-y: auto;
}

.option-label {
  margin-right: 0.5rem;
}

.option-api-name {
  color: #a0aec0;
  font-size: 0.75rem;
  font-family: var(--font-family-mono, monospace);
}

/* Filter builder */
.filter-row {
  display: grid;
  grid-template-columns: 1.4fr 1.2fr 1.6fr auto auto;
  gap: 0.5rem;
  align-items: center;
  margin-bottom: 0.5rem;
}

.filter-field,
.filter-operator,
.filter-value {
  min-width: 0;
}

.btn-icon {
  padding: 0.4rem 0.55rem;
  font-size: 0.75rem;
  font-weight: 600;
  border: 1px solid #e2e8f0;
  background: #fff;
  border-radius: 4px;
  cursor: pointer;
  line-height: 1;
  white-space: nowrap;
}

.btn-icon.active {
  background: #ebf4ff;
  border-color: #4299e1;
  color: #2b6cb0;
}

.btn-icon:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.btn-icon.btn-danger {
  color: #c53030;
}

.btn-add-filter {
  margin-top: 0.25rem;
}

code {
  background: #f7fafc;
  padding: 0 0.25rem;
  border-radius: 3px;
  font-size: 0.8em;
}

@media (max-width: 640px) {
  .filter-row {
    grid-template-columns: 1fr;
  }
}
</style>
