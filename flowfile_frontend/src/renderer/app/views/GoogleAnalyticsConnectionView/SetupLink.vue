<template>
  <el-popover
    placement="top"
    :width="380"
    trigger="hover"
    :show-after="150"
    :hide-after="150"
    popper-class="setup-link-popper"
  >
    <template #reference>
      <a :href="href" target="_blank" rel="noopener" class="setup-link">
        <slot />
        <i class="fa-solid fa-arrow-up-right-from-square setup-link-icon"></i>
      </a>
    </template>
    <div class="setup-link-popover">
      <p class="setup-link-popover-label">Opens in Google Cloud console</p>
      <code class="setup-link-popover-url">{{ href }}</code>
      <button type="button" class="setup-link-popover-copy" @click="copy">
        <i class="fa-solid fa-copy"></i>
        Copy URL
      </button>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { ElPopover, ElMessage } from "element-plus";

const props = defineProps<{ href: string }>();

const copy = async () => {
  try {
    await navigator.clipboard.writeText(props.href);
    ElMessage.success("URL copied");
  } catch {
    ElMessage.error("Could not access clipboard — copy it manually");
  }
};
</script>

<style scoped>
.setup-link {
  color: var(--color-accent);
  text-decoration: underline;
  text-underline-offset: 2px;
  white-space: nowrap;
}

.setup-link-icon {
  font-size: 0.7em;
  margin-left: 4px;
  opacity: 0.7;
}

.setup-link-popover {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.setup-link-popover-label {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.setup-link-popover-url {
  display: block;
  padding: var(--spacing-2);
  background: var(--color-background-muted);
  border-radius: var(--border-radius-sm);
  font-size: 0.8em;
  word-break: break-all;
  user-select: all;
}

.setup-link-popover-copy {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-1);
  align-self: flex-start;
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-background-muted);
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-sm);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.setup-link-popover-copy:hover {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}
</style>
