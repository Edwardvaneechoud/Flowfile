<template>
  <pop-over :content="tooltipText">
    <div class="icon-wrapper">
      <span class="unavailable-icon">
        {{ iconText }}
      </span>
    </div>
  </pop-over>
</template>

<script lang="ts" setup>
import PopOver from "../../editor/PopOver.vue";

defineProps({
  iconText: {
    type: String,
    default: "!", // Default to '!' if no input is provided
  },
  tooltipText: {
    type: String,
    default: "Field not available", // Default tooltip text
  },
});
</script>

<style scoped>
.icon-wrapper {
  position: relative;
  display: inline-block;
}

.unavailable-icon {
  display: inline-block;
  width: 16px;
  height: 16px;
  line-height: 16px;
  text-align: center;
  background-color: #d9534f; /* Bootstrap danger color */
  color: white;
  font-weight: bold;
  font-size: 1.2em;
  border-radius: 50%; /* Makes it round */
  margin-left: 8px;
  cursor: default;
  user-select: none;
  transition:
    background-color 0.3s,
    transform 0.3s;
  z-index: 1000;
}

.unavailable-icon:hover {
  background-color: #c9302c; /* Darker red on hover */
  transform: scale(1.1); /* Slightly enlarges the icon */
}

.tooltip-text {
  visibility: hidden;
  background-color: #333;
  color: #fff;
  text-align: center;
  border-radius: 4px;
  padding: 5px;
  position: absolute;
  z-index: 1;
  bottom: 125%; /* Position the tooltip above the icon */
  left: 50%;
  transform: translateX(-50%);
  white-space: nowrap;
  opacity: 0;
  transition: opacity 0.3s;
}

.icon-wrapper:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
  z-index: 10000;
}
</style>
