<template>
  <div class="settings-form">
    <div class="preview-info">
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
      <div class="data-preview-header">
        <label>Data Preview</label>
        <button class="expand-btn" @click="toggleExpanded" title="Expand to bottom half">
          <span class="material-icons">open_in_full</span>
        </button>
      </div>
      <div class="table-container">
        <ag-grid-vue
          :default-col-def="defaultColDef"
          :column-defs="columnDefs"
          class="ag-theme-balham"
          :row-data="rowData"
          :pagination="true"
          :paginationPageSize="100"
          :enableCellTextSelection="true"
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

    <!-- Expanded Preview Overlay -->
    <Teleport to="body">
      <div v-if="isExpanded" class="expanded-preview-overlay">
        <div class="expanded-preview-panel">
          <div class="expanded-header">
            <span class="expanded-title">
              <span class="material-icons">table_chart</span>
              Data Preview
            </span>
            <div class="expanded-stats">
              <span class="stat-badge">{{ result?.data?.total_rows ?? 0 }} rows</span>
              <span class="stat-badge">{{ result?.data?.columns?.length ?? 0 }} columns</span>
            </div>
            <div class="expanded-actions">
              <button class="action-btn" @click="autoSizeColumns" title="Auto-size columns">
                <span class="material-icons">fit_screen</span>
              </button>
              <button class="action-btn" @click="toggleExpanded" title="Collapse">
                <span class="material-icons">close_fullscreen</span>
              </button>
              <button class="action-btn close" @click="toggleExpanded" title="Close">
                <span class="material-icons">close</span>
              </button>
            </div>
          </div>
          <div class="expanded-content">
            <ag-grid-vue
              :default-col-def="defaultColDef"
              :column-defs="columnDefs"
              class="ag-theme-balham"
              :row-data="rowData"
              :pagination="true"
              :paginationPageSize="100"
              :enableCellTextSelection="true"
              style="width: 100%; height: 100%;"
              @grid-ready="onExpandedGridReady"
            />
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { BaseNodeSettings, NodeResult, ColumnSchema } from '../../types'
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
const expandedGridApi = ref<GridApi | null>(null)
const gridHeight = ref('250px')
const isLoading = ref(false)
const isExpanded = ref(false)

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
  flex: 1,
}

// Build schema map for data types
const schemaMap = computed(() => {
  const map = new Map<string, ColumnSchema>()
  if (result.value?.schema) {
    result.value.schema.forEach(col => map.set(col.name, col))
  }
  return map
})

// Convert data to AG Grid format with data type tooltips
const columnDefs = computed(() => {
  if (!result.value?.data?.columns) return []

  return result.value.data.columns.map((colName: string) => {
    const schema = schemaMap.value.get(colName)
    const dataType = schema?.data_type || 'Unknown'

    return {
      field: colName,
      headerName: colName,
      headerTooltip: `Type: ${dataType}`,
      resizable: true,
    }
  })
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

const onExpandedGridReady = (params: { api: GridApi }) => {
  expandedGridApi.value = params.api
}

// Toggle expanded view
const toggleExpanded = () => {
  isExpanded.value = !isExpanded.value
}

// Auto-size columns
const autoSizeColumns = () => {
  if (expandedGridApi.value) {
    expandedGridApi.value.autoSizeAllColumns()
  }
}

// Calculate dynamic grid height
const calculateGridHeight = () => {
  const rowCount = rowData.value.length
  const headerHeight = 40
  const rowHeight = 28
  const paginationHeight = 48
  const maxHeight = 300
  const minHeight = 150

  const calculatedHeight = headerHeight + (rowCount * rowHeight) + paginationHeight
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

.data-preview-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}

.data-preview-header label,
.schema-preview label {
  display: block;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  margin: 0;
}

.expand-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 6px;
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  cursor: pointer;
  transition: all 0.15s;
  color: var(--color-text-primary);
}

.expand-btn:hover {
  background: var(--color-background-hover);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.expand-btn .material-icons {
  font-size: 18px;
}

.table-container {
  border-radius: var(--border-radius-md);
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
}

.schema-preview label {
  margin-bottom: 8px;
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

/* Expanded Preview Overlay */
.expanded-preview-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  z-index: 9999;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
}

.expanded-preview-panel {
  height: 70vh;
  background: var(--color-background-secondary);
  border-top: 2px solid var(--color-border-primary);
  display: flex;
  flex-direction: column;
  box-shadow: 0 -4px 20px rgba(0, 0, 0, 0.2);
}

.expanded-header {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 12px 16px;
  background: var(--color-background-tertiary);
  border-bottom: 1px solid var(--color-border-primary);
  flex-shrink: 0;
}

.expanded-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.expanded-title .material-icons {
  font-size: 20px;
  color: var(--color-accent);
}

.expanded-stats {
  display: flex;
  gap: 12px;
  margin-left: auto;
}

.stat-badge {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  background: var(--color-background-primary);
  padding: 4px 10px;
  border-radius: var(--border-radius-sm);
  border: 1px solid var(--color-border-light);
}

.expanded-actions {
  display: flex;
  gap: 8px;
}

.action-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 6px;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  cursor: pointer;
  transition: all 0.15s;
  color: var(--color-text-primary);
}

.action-btn:hover {
  background: var(--color-background-hover);
  border-color: var(--color-accent);
}

.action-btn.close:hover {
  background: var(--color-danger-light);
  border-color: var(--color-danger);
  color: var(--color-danger);
}

.action-btn .material-icons {
  font-size: 18px;
}

.expanded-content {
  flex: 1;
  overflow: hidden;
  min-height: 0;
}
</style>
