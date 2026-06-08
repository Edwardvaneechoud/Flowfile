<template>
  <Canvas />
</template>

<script setup lang="ts">
// Designer view: the flow-graph canvas. Shared chrome (header, nav tabs, theme
// toggle, Pyodide init) lives in AppLayout.vue, which renders this via <router-view/>.
import Canvas from '../components/Canvas.vue'
</script>
