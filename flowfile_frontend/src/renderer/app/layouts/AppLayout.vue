<template>
  <div class="app-layout">
    <Header />
    <div class="app-layout__content">
      <div class="app-layout__sidebar-wrapper" :class="{ minimized: isCollapse }">
        <Sidebar :is-collapse="isCollapse" @toggle-collapse="toggleCollapse" />
      </div>
      <div class="app-layout__page">
        <router-view />
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import Header from "../features/header/Header.vue";
import Sidebar from "../features/sidebar/Sidebar.vue";

const isCollapse = ref(true);

const toggleCollapse = () => {
  isCollapse.value = !isCollapse.value;
};
</script>

<style lang="scss">
$tabletBreakPointPX: 1000;

.app-layout {
  height: 100vh;
  display: flex;
  flex-direction: column;

  &__content {
    display: flex;
    flex-grow: 1;

    @media screen and (max-width: $tabletBreakPointPX) {
      .app-layout__sidebar-wrapper {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        z-index: 999;
        overflow-y: auto;

        &.minimized {
          display: none;
        }
      }

      .app-layout__page {
        width: 100%;
        background-color: #ffffff;
      }
    }
  }

  &__sidebar-wrapper {
    width: 16rem;
    background: #ffffff;
    transition: width 0.3s ease-out;

    &.minimized {
      width: 4.5rem;
    }
  }

  &__page {
    flex-grow: 1;
    overflow-y: auto;
  }
}
</style>
