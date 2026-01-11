<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Join columns</div>

    <div class="join-type-selector">
      <label class="join-type-label">Join Type:</label>
      <select v-model="joinType" @change="emitUpdate" class="select" style="flex: 1;">
        <option value="inner">inner</option>
        <option value="left">left</option>
        <option value="right">right</option>
        <option value="full">full</option>
        <option value="semi">semi</option>
        <option value="anti">anti</option>
        <option value="cross">cross</option>
      </select>
    </div>

    <div class="table-wrapper" style="margin-top: 12px;">
      <div class="selectors-header">
        <div class="selectors-title">L</div>
        <div class="selectors-title">R</div>
        <div class="selectors-title"></div>
      </div>
      <div class="selectors-container">
        <div
          v-for="(mapping, index) in joinMapping"
          :key="index"
          class="selectors-row"
        >
          <select v-model="mapping.left_col" @change="emitUpdate" class="select" style="flex: 1;">
            <option value="">Select column...</option>
            <option v-for="col in leftColumns" :key="col.name" :value="col.name">
              {{ col.name }}
            </option>
          </select>
          <select v-model="mapping.right_col" @change="emitUpdate" class="select" style="flex: 1;">
            <option value="">Select column...</option>
            <option v-for="col in rightColumns" :key="col.name" :value="col.name">
              {{ col.name }}
            </option>
          </select>
          <div class="action-buttons">
            <button
              v-if="index !== joinMapping.length - 1"
              class="action-button remove-button"
              @click="removeMapping(index)"
            >
              -
            </button>
            <button
              v-if="index === joinMapping.length - 1"
              class="action-button add-button"
              @click="addJoinCondition"
            >
              +
            </button>
          </div>
        </div>
        <!-- Empty state: show add button -->
        <div v-if="joinMapping.length === 0" class="selectors-row">
          <select class="select" style="flex: 1;" disabled>
            <option>Select column...</option>
          </select>
          <select class="select" style="flex: 1;" disabled>
            <option>Select column...</option>
          </select>
          <div class="action-buttons">
            <button class="action-button add-button" @click="addJoinCondition">+</button>
          </div>
        </div>
      </div>
    </div>

    <div v-if="showColumnSelection" style="margin-top: 12px;">
      <div class="listbox-subtitle">Output columns</div>
      <div class="help-text" style="padding: 8px;">
        Configure which columns to include in the output using the full Flowfile editor.
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { JoinSettings, JoinType, JoinMapping, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: JoinSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: JoinSettings): void
}>()

const flowStore = useFlowStore()

const joinType = ref<JoinType>(props.settings.join_input?.join_type || props.settings.join_input?.how || 'inner')
const joinMapping = ref<JoinMapping[]>(props.settings.join_input?.join_mapping || [])
const leftSuffix = ref(props.settings.join_input?.left_suffix || '_left')
const rightSuffix = ref(props.settings.join_input?.right_suffix || '_right')

const JOIN_TYPES_WITHOUT_COLUMN_SELECTION: JoinType[] = ['anti', 'semi']

const showColumnSelection = computed(() => {
  return joinType.value && !JOIN_TYPES_WITHOUT_COLUMN_SELECTION.includes(joinType.value)
})

const leftColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getLeftInputSchema(props.nodeId)
})

const rightColumns = computed<ColumnSchema[]>(() => {
  return flowStore.getRightInputSchema(props.nodeId)
})

watch(() => props.settings.join_input, (newInput) => {
  if (newInput) {
    joinType.value = newInput.join_type || newInput.how || 'inner'
    joinMapping.value = [...(newInput.join_mapping || [])]
    leftSuffix.value = newInput.left_suffix || '_left'
    rightSuffix.value = newInput.right_suffix || '_right'
  }
}, { deep: true })

function addJoinCondition() {
  joinMapping.value.push({
    left_col: '',
    right_col: ''
  })
}

function removeMapping(index: number) {
  joinMapping.value.splice(index, 1)
  emitUpdate()
}

function emitUpdate() {
  const settings: JoinSettings = {
    ...props.settings,
    is_setup: joinMapping.value.some(m => m.left_col && m.right_col),
    join_input: {
      join_type: joinType.value,
      how: joinType.value,
      join_mapping: [...joinMapping.value],
      left_suffix: leftSuffix.value,
      right_suffix: rightSuffix.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
/* Component uses global styles from main.css */
.action-buttons {
  display: flex;
  gap: 4px;
  min-width: 60px;
  justify-content: center;
}
</style>
