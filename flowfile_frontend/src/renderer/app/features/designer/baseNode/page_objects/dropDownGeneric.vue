<template>
  <div>
    <p v-if="title !== ''" class="label">{{ title }}</p>
    <div class="select-wrapper">
      <dropDown
        v-if="isLoaded"
        v-model="localSelectedValue"
        :column-options="optionList"
        :allow-other="allowOther"
        :placeholder="placeholder"
      />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch, defineProps, defineEmits, onMounted } from "vue";
import dropDown from "./dropDown.vue";

const props = defineProps({
  modelValue: {
    type: String,
    default: "NewField",
  },
  optionList: {
    type: Array as () => string[],
    required: true,
  },
  title: {
    type: String,
    default: "",
  },
  allowOther: {
    type: Boolean,
    default: true,
  },
  placeholder: {
    type: String,
    default: "Select an option",
  },
});

const isLoaded = ref<boolean>(false);

const emit = defineEmits(["update:modelValue", "change"]);

const localSelectedValue = ref(props.modelValue);

watch(
  () => props.modelValue,
  (newVal) => {
    localSelectedValue.value = newVal;
  },
);

watch(localSelectedValue, (newVal) => {
  emit("update:modelValue", newVal);
  emit("change", newVal);
});

onMounted(() => {
  isLoaded.value = true;
});
</script>

<style scoped>
.label {
  font-weight: bold;
  margin-bottom: 8px;
  color: #333;
}

.select-box {
  width: 100%;
  padding: 10px 12px;
  font-size: 14px;
  line-height: 1.4;
  border: 1px solid #ddd;
  border-radius: 4px;
  outline: none;
  transition:
    border-color 0.2s,
    box-shadow 0.2s;
  z-index: 1000;
}

.select-box:focus {
  border-color: #3498db;
  box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
}

.options-list {
  position: absolute;
  width: 100%;
  border: 1px solid #ddd;
  border-top: none;
  border-radius: 0 0 4px 4px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 0;
  background: #fff;
}

.options-list li {
  padding: 10px 12px;
  cursor: pointer;
  font-size: 14px;
  color: #333;
  transition: background-color 0.2s;
  z-index: 1000;
}

.options-list li:not(:last-child) {
  border-bottom: 1px solid #eee;
}

.options-list li:hover {
  background-color: #f0f0f0;
}
</style>
