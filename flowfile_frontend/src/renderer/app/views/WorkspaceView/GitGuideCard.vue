<template>
  <section class="ws-card">
    <header class="ws-card-head ws-card-head-toggle" @click="open = !open">
      <h3 class="ws-card-title"><i class="fa-brands fa-git-alt" /> Put it under git</h3>
      <i class="fa-solid" :class="open ? 'fa-chevron-up' : 'fa-chevron-down'" />
    </header>

    <div v-show="open" class="ws-guide">
      <p class="ws-muted">
        The exported tree is a normal git repo — version it with whatever git host or CI you use.
        In-app history &amp; one-click rollback are coming soon.
      </p>

      <div class="ws-snippet">
        <div class="ws-snippet-head">
          <span>Commit &amp; push</span>
          <button class="ws-icon-btn" title="Copy" @click="copy(commitSnippet)">
            <i class="fa-regular fa-copy" />
          </button>
        </div>
        <pre>{{ commitSnippet }}</pre>
      </div>

      <div class="ws-snippet">
        <div class="ws-snippet-head">
          <span>Rebuild on a fresh clone</span>
          <button class="ws-icon-btn" title="Copy" @click="copy(rebuildSnippet)">
            <i class="fa-regular fa-copy" />
          </button>
        </div>
        <pre>{{ rebuildSnippet }}</pre>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";

const props = defineProps<{ projectName: string; rootPath: string; gitEnabled: boolean }>();

const open = ref(!props.gitEnabled);

const commitSnippet = computed(
  () =>
    `cd "${props.rootPath}"\n` +
    `git init\n` +
    `git add -A\n` +
    `git commit -m "Flowfile project snapshot"\n` +
    `git remote add origin <your-repo-url>\n` +
    `git push -u origin main`,
);

const rebuildSnippet = computed(
  () =>
    `git clone <your-repo-url>\n` +
    `export FLOWFILE_SECRET_<NAME>=...   # refill values your team holds\n` +
    `flowfile project apply "${props.rootPath}"`,
);

async function copy(text: string) {
  try {
    await navigator.clipboard.writeText(text);
    ElMessage.success("Copied");
  } catch {
    /* clipboard unavailable — ignore */
  }
}
</script>
