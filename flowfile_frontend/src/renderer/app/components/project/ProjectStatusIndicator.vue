<template>
  <div v-if="store.isActive" class="project-indicator" :class="`is-${store.status}`">
    <el-popover
      :width="340"
      trigger="click"
      placement="bottom-end"
      popper-class="project-pill-popover"
      @show="onShow"
    >
      <template #reference>
        <button class="project-pill" type="button" :title="pillTitle">
          <i class="fa-solid fa-folder project-pill__folder"></i>
          <span class="project-pill__name">{{ store.activeProject?.name }}</span>
          <i :class="glyphClass" class="project-pill__glyph"></i>
        </button>
      </template>

      <div class="project-pop">
        <div class="project-pop__head">
          <span class="project-pop__title">{{ store.activeProject?.name }}</span>
          <button type="button" class="project-pop__link" @click="goToProject">Open project</button>
        </div>

        <div v-if="store.status === 'external'" class="project-pop__banner">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>
            Files changed outside Flowfile.
            <a @click="goToProject">Review them</a> before saving.
          </span>
        </div>

        <div v-if="store.projectionFailed" class="project-pop__banner">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>The last sync to the project folder failed; it may be out of date.</span>
        </div>

        <div v-if="store.error" class="project-pop__banner">
          <i class="fa-solid fa-circle-exclamation"></i>
          <span>{{ store.error }}</span>
        </div>

        <p v-if="store.status === 'clean'" class="project-pop__clean">
          <i class="fa-solid fa-circle-check"></i>
          All changes are saved as the latest version.
        </p>
        <SaveVersionForm v-else @saved="onSaved" />
      </div>
    </el-popover>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useRouter } from "vue-router";
import SaveVersionForm from "./SaveVersionForm.vue";
import { useProjectStore, type ProjectStatus } from "../../stores/project-store";

const store = useProjectStore();
const router = useRouter();

const pillTitle = computed<string>(
  () =>
    (
      ({
        clean: "All changes are saved as the latest version",
        unsaved: "You have unsaved changes — click to save a version",
        external: "Files changed outside Flowfile — click to review",
        none: "",
      }) as Record<ProjectStatus, string>
    )[store.status],
);

const glyphClass = computed<string>(
  () =>
    (
      ({
        clean: "fa-solid fa-circle-check",
        unsaved: "fa-solid fa-circle",
        external: "fa-solid fa-triangle-exclamation",
        none: "",
      }) as Record<ProjectStatus, string>
    )[store.status],
);

const onShow = () => store.refreshActive();
const goToProject = () => router.push({ name: "project" });
const onSaved = () => undefined;
</script>

<style scoped>
.project-indicator {
  display: flex;
  align-items: center;
}

.project-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  max-width: 240px;
  padding: 4px 12px;
  border: 1px solid var(--color-border-primary, #e2e8f0);
  border-radius: 999px;
  background: var(--color-background-secondary, #f8fafc);
  color: var(--color-text-secondary, #475569);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s ease;
}

.project-pill:hover {
  border-color: var(--color-border-secondary, #cbd5e1);
}

.project-pill__folder {
  font-size: 12px;
  opacity: 0.7;
}

.project-pill__name {
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.project-pill__glyph {
  font-size: 11px;
}

.is-clean .project-pill__glyph {
  color: var(--color-success, #16a34a);
}

.is-unsaved .project-pill {
  border-color: var(--color-warning, #d97706);
  background: color-mix(in srgb, var(--color-warning, #d97706) 10%, transparent);
}

.is-unsaved .project-pill__glyph {
  color: var(--color-warning, #d97706);
  font-size: 8px;
}

.is-external .project-pill {
  border-color: var(--color-danger, #ef4444);
  background: color-mix(in srgb, var(--color-danger, #ef4444) 10%, transparent);
}

.is-external .project-pill__glyph {
  color: var(--color-danger, #ef4444);
}

.project-pop {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.project-pop__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.project-pop__title {
  font-weight: 600;
  font-size: 13px;
  color: var(--color-text-primary, #0f172a);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.project-pop__link {
  flex-shrink: 0;
  padding: 0;
  background: transparent;
  border: none;
  color: var(--color-accent, #2563eb);
  font-size: 12px;
  cursor: pointer;
}

.project-pop__banner {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  background: color-mix(in srgb, var(--color-danger, #ef4444) 10%, transparent);
  color: var(--color-text-secondary, #475569);
  font-size: 12px;
  line-height: 1.4;
}

.project-pop__banner i {
  color: var(--color-danger, #ef4444);
  margin-top: 2px;
}

.project-pop__banner a {
  color: var(--color-accent, #2563eb);
  cursor: pointer;
}

.project-pop__clean {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 0;
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
}

.project-pop__clean i {
  color: var(--color-success, #16a34a);
}
</style>
