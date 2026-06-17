<template>
  <!-- App mode: the header (AppLayout) drives flow actions, so the in-canvas
       toolbar is hidden. The embeddable library keeps its own toolbar. -->
  <Canvas :show-toolbar="false" />
</template>

<script setup lang="ts">
// Designer view: the flow-graph canvas. Shared chrome (header, nav tabs, theme
// toggle, Pyodide init) lives in AppLayout.vue, which renders this via <router-view/>.
import Canvas from '../components/Canvas.vue'
</script>
