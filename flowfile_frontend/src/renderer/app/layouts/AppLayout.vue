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

    <!-- Force password change modal -->
    <ChangePasswordModal
      :show="showPasswordModal"
      :is-forced="true"
      @close="showPasswordModal = false"
      @success="handlePasswordChanged"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import Header from "../components/layout/Header/AppHeader.vue";
import Sidebar from "../components/layout/Sidebar/Sidebar.vue";
import ChangePasswordModal from "../components/common/ChangePasswordModal/ChangePasswordModal.vue";
import { useAuthStore } from "../stores/auth-store";
import authService from "../services/auth.service";

const authStore = useAuthStore();

const isCollapse = ref(true);
const showPasswordModal = ref(false);

// Show password change modal if user must change password (not in Electron mode)
const mustShowPasswordModal = computed(() => {
  return authStore.mustChangePassword && !authService.isInElectronMode();
});

// Watch for changes and show modal
watch(
  mustShowPasswordModal,
  (newVal) => {
    if (newVal) {
      showPasswordModal.value = true;
    }
  },
  { immediate: true },
);

const toggleCollapse = () => {
  isCollapse.value = !isCollapse.value;
};

const handlePasswordChanged = () => {
  authStore.clearMustChangePassword();
  showPasswordModal.value = false;
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
    overflow: hidden;

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
        background-color: var(--color-background-primary);
      }
    }
  }

  &__sidebar-wrapper {
    width: 16rem;
    background: var(--color-background-primary);
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
