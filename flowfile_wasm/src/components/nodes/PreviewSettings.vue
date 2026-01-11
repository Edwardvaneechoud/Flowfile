<template>
  <div class="settings-form">
    <div class="preview-info">
      <div class="info-icon">
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
          <circle cx="12" cy="12" r="3"/>
        </svg>
      </div>
      <div class="info-text">
        <h4>Preview Node</h4>
        <p>This node displays the data from the connected input. No configuration needed.</p>
      </div>
    </div>

    <div v-if="result" class="stats">
      <div class="stat">
        <span class="stat-label">Rows</span>
        <span class="stat-value">{{ result.data?.total_rows ?? 0 }}</span>
      </div>
      <div class="stat">
        <span class="stat-label">Columns</span>
        <span class="stat-value">{{ result.data?.columns?.length ?? 0 }}</span>
      </div>
    </div>

    <!-- Data Table with AG Grid -->
    <div v-if="hasData" class="data-preview">
      <label>Data Preview</label>
      <div class="table-container">
        <ag-grid-vue
          :default-col-def="defaultColDef"
          :column-defs="columnDefs"
          class="ag-theme-balham"
          :row-data="rowData"
          :style="{ width: '100%', height: gridHeight }"
          @grid-ready="onGridReady"
        />
      </div>
    </div>

    <!-- Schema Preview -->
    <div v-if="result?.schema" class="schema-preview">
      <label>Schema</label>
      <div class="schema-list">
        <div v-for="col in result.schema" :key="col.name" class="schema-item">
          <span class="col-name">{{ col.name }}</span>
          <span class="col-type">{{ col.data_type }}</span>
        </div>
      </div>
    </div>

    <!-- No data message -->
    <div v-if="!result && !isLoading" class="no-data">
      <p>No data available. Connect an input node and run the flow to see data.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { BaseNodeSettings, NodeResult } from '../../types'
import { AgGridVue } from "@ag-grid-community/vue3"
import { GridApi } from "@ag-grid-community/core"
import { ModuleRegistry } from "@ag-grid-community/core"
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model"

// Register AG Grid modules
ModuleRegistry.registerModules([ClientSideRowModelModule])

const props = defineProps<{
  nodeId: number
  settings: BaseNodeSettings
}>()

const flowStore = useFlowStore()
const gridApi = ref<GridApi | null>(null)
const gridHeight = ref('250px')
const isLoading = ref(false)

const result = computed<NodeResult | undefined>(() => {
  return flowStore.getNodeResult(props.nodeId)
})

const hasData = computed(() => {
  return result.value?.data && result.value.data.data && result.value.data.data.length > 0
})

// Default column definition for AG Grid
const defaultColDef = {
  editable: false,
  filter: true,
  sortable: true,
  resizable: true,
  minWidth: 100,
}

// Convert data to AG Grid format
const columnDefs = computed(() => {
  if (!result.value?.data?.columns) return []

  return result.value.data.columns.map((colName: string) => ({
    field: colName,
    headerName: colName,
    resizable: true,
  }))
})

// Convert row data to AG Grid format (array of objects)
const rowData = computed(() => {
  if (!result.value?.data?.columns || !result.value?.data?.data) return []

  const columns = result.value.data.columns
  return result.value.data.data.map((row: any[]) => {
    const rowObj: Record<string, any> = {}
    columns.forEach((col: string, index: number) => {
      rowObj[col] = row[index]
    })
    return rowObj
  })
})

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api
}

// Calculate dynamic grid height
const calculateGridHeight = () => {
  const rowCount = rowData.value.length
  const headerHeight = 40
  const rowHeight = 28
  const maxHeight = 300
  const minHeight = 150

  const calculatedHeight = headerHeight + (rowCount * rowHeight)
  gridHeight.value = `${Math.min(Math.max(calculatedHeight, minHeight), maxHeight)}px`
}

onMounted(() => {
  calculateGridHeight()
  window.addEventListener('resize', calculateGridHeight)
})

onUnmounted(() => {
  window.removeEventListener('resize', calculateGridHeight)
})
</script>

<style scoped>
.settings-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.preview-info {
  display: flex;
  gap: 16px;
  padding: 16px;
  background: var(--color-accent-subtle);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.info-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  color: var(--color-accent);
}

.info-icon svg {
  width: 24px;
  height: 24px;
}

.info-text h4 {
  margin: 0 0 4px 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.info-text p {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
}

.stats {
  display: flex;
  gap: 16px;
}

.stat {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 16px;
  background: var(--color-background-tertiary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.stat-label {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.stat-value {
  font-size: var(--font-size-4xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.data-preview label,
.schema-preview label {
  display: block;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  margin-bottom: 8px;
  color: var(--color-text-primary);
}

.table-container {
  border-radius: var(--border-radius-md);
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
}

.schema-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-sm);
}

.schema-item {
  display: flex;
  justify-content: space-between;
  padding: 8px 12px;
  background: var(--color-background-primary);
}

.schema-item:nth-child(even) {
  background: var(--color-background-tertiary);
}

.col-name {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.col-type {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.no-data {
  padding: var(--spacing-4);
  text-align: center;
  color: var(--color-text-secondary);
  background: var(--color-background-tertiary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.no-data p {
  margin: 0;
  font-size: var(--font-size-md);
}
</style>
