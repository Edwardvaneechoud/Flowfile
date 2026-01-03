<template>
  <div
    :class="['logo-container', { 'column-layout': positionAppName === 'below' }]"
    @click="OnClick"
  >
    <img src="/images/flowfile.svg" :alt="altText" :style="{ width: width, height: height }" />
    <span v-if="appName" class="app-name">{{ appName }}</span>
  </div>
</template>

<script setup lang="ts">
import { defineProps } from "vue";

const props = defineProps({
  width: {
    type: String,
    default: "100px",
  },
  height: {
    type: String,
    default: "auto",
  },
  altText: {
    type: String,
    default: "Logo",
  },
  appName: {
    type: String,
    default: "",
  },
  positionAppName: {
    type: String,
    default: "right",
  },
  clickAction: {
    type: Function,
    default: null,
  },
});

const OnClick = (event: MouseEvent) => {
  if (props.clickAction) {
    props.clickAction(event);
  }
};
</script>

<style scoped>
.logo-container {
  display: flex;
  align-items: center;
}

.column-layout {
  flex-direction: column;
  align-items: flex-start; /* Adjust if you want to center align */
}

.app-name {
  font-weight: bold;
  font-size: 1.5em; /* Adjust the size as needed */
  font-family: "Arial", sans-serif; /* Change to your desired font-family */
  margin-top: 10px; /* Margin for when the app name is below */
}

.app-name.right {
  order: 2;
  margin-left: 10px;
}

.app-name.left {
  order: -1;
  margin-right: 10px;
}
</style>
