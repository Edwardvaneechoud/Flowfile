<template>
  <div class="ai-settings-view">
    <SettingsTabBar :tabs="tabs" :active-tab="activeTab" @select="handleTabClick" />

    <div class="ai-settings-content">
      <div class="ai-settings-container">
        <!-- Disabled-state: matches the AI subsystem's 503 contract. Shown in
             place of every tab so the admin button is reachable from any deep
             link. The in-app toggle lives in process memory, hence the .env
             hint below it. -->
        <template v-if="aiStore.aiDisabled">
          <div class="mb-3">
            <h2 class="page-title">AI</h2>
            <p class="page-description">Providers, models and assistant behaviour.</p>
          </div>
          <div class="card mb-3">
            <div class="card-content">
              <div class="info-box">
                <i class="fa-solid fa-circle-info"></i>
                <div class="info-body">
                  <p><strong>AI features are off</strong></p>
                  <p>{{ AI_DISABLED_DETAIL }}</p>

                  <template v-if="isAdmin">
                    <div class="info-actions">
                      <el-button
                        type="primary"
                        :loading="isEnablingFlag"
                        :disabled="isEnablingFlag"
                        @click="handleEnableFlag"
                      >
                        <i class="fa-solid fa-wand-magic-sparkles"></i>
                        <span>Enable AI features</span>
                      </el-button>
                    </div>
                    <p class="hint-text">
                      This enables AI for the running process. To persist across restarts, add
                      <code>FEATURE_FLAG_AI=true</code> to your <code>.env</code> file.
                    </p>
                  </template>
                  <template v-else>
                    <p class="hint-text">
                      Ask your administrator to enable AI features for this Flowfile install.
                    </p>
                  </template>
                </div>
              </div>
            </div>
          </div>
        </template>

        <template v-else>
          <ProvidersPanel v-if="activeTab === 'providers'" />
          <AssistantPanel v-else-if="activeTab === 'assistant'" @navigate="handleTabClick" />
        </template>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElButton, ElMessage } from "element-plus";
import SettingsTabBar from "../../components/settings/SettingsTabBar.vue";
import { useAiStore } from "../../stores/ai-store";
import { useAuthStore } from "../../stores/auth-store";
import AssistantPanel from "./AssistantPanel.vue";
import ProvidersPanel from "./ProvidersPanel.vue";
import { AI_DISABLED_DETAIL, setAiFeatureFlag } from "./api";
import { AI_SETTINGS_TAB_ALIASES, AI_SETTINGS_TAB_KEYS, aiSettingsTabs } from "./aiSettingsTabs";
import type { AiSettingsTabKey } from "./aiSettingsTabs";

const route = useRoute();
const router = useRouter();
const aiStore = useAiStore();
const authStore = useAuthStore();

const isAdmin = computed(() => authStore.isAdmin);
const isEnablingFlag = ref(false);

const tabs = aiSettingsTabs.map((t) => ({ key: t.key, label: t.label, icon: t.icon }));

const isTabKey = (value: unknown): value is AiSettingsTabKey =>
  typeof value === "string" && (AI_SETTINGS_TAB_KEYS as ReadonlyArray<string>).includes(value);

// Accepts current keys plus retired aliases (``local`` → providers).
const resolveTab = (value: unknown): AiSettingsTabKey | null => {
  if (isTabKey(value)) return value;
  if (typeof value === "string" && value in AI_SETTINGS_TAB_ALIASES) {
    return AI_SETTINGS_TAB_ALIASES[value];
  }
  return null;
};

function getInitialTab(): AiSettingsTabKey {
  return resolveTab(route.query.tab) ?? "providers";
}

const activeTab = ref<AiSettingsTabKey>(getInitialTab());

onMounted(() => {
  router.replace({ query: { ...route.query, tab: activeTab.value } });
  // One load feeds every tab (providers + the synthetic local entry) and
  // flips ``aiDisabled`` when the router answers 503.
  void aiStore.loadProviders();
});

function handleTabClick(tab: string) {
  const resolved = resolveTab(tab);
  if (resolved === null) return;
  activeTab.value = resolved;
  router.replace({ query: { ...route.query, tab: resolved } });
}

// Follow ?tab= changes made elsewhere (sidebar sub-menu, deep links) while
// the view is already mounted.
watch(
  () => route.query.tab,
  (tab) => {
    const resolved = resolveTab(tab);
    if (resolved !== null && resolved !== activeTab.value) {
      activeTab.value = resolved;
    }
  },
);

const handleEnableFlag = async () => {
  // Admin-only path; the UI gates on isAdmin so the 403 branch never fires.
  isEnablingFlag.value = true;
  try {
    const state = await setAiFeatureFlag(true);
    if (state.enabled) {
      ElMessage.success("AI features enabled for this process");
      await aiStore.loadProviders();
    } else {
      ElMessage.error("Failed to enable AI features");
    }
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to enable AI features");
  } finally {
    isEnablingFlag.value = false;
  }
};
</script>

<style src="./aiSettingsShared.css"></style>

<style scoped>
.ai-settings-view {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.ai-settings-content {
  flex: 1;
  overflow: auto;
}

.ai-settings-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}
</style>
