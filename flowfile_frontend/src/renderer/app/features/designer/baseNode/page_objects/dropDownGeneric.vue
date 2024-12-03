<template>
  <div>
    <p v-if="title !== ''" class="label">{{ title }}</p>
    <div class="select-wrapper">
      <dropDown
        v-model="localSelectedValue"
        :column-options="optionList"
        :allow-other="allowOther"
        :placeholder="placeholder"
        :is-loading="isLoading"
      />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
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
  isLoading: {
    type: Boolean,
    default: false,
  },
});

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
</script>

<style scoped>
.label {
  font-weight: bold;
  margin-bottom: 8px;
  color: #333;
}

.select-wrapper {
  width: 100%;
  position: relative;
}
</style>
