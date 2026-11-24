<template>
  <section class="card mb-3" data-testid="project-branches">
    <div class="card-header">
      <h3 class="card-title">
        <i class="fa-solid fa-code-branch card-title__icon"></i>
        Branch and remote
      </h3>
    </div>
    <div class="card-content">
      <p v-if="error" class="branch-error">{{ error }}</p>
      <template v-else-if="state">
        <label class="field-label" for="project-branch-select">Branch</label>
        <div class="branch-row">
          <el-select
            id="project-branch-select"
            :model-value="state.current ?? ''"
            :disabled="busy || !!merge"
            size="default"
            data-testid="project-branch-select"
            @change="onSwitch"
          >
            <el-option v-for="b in state.branches" :key="b.name" :label="b.name" :value="b.name" />
          </el-select>
          <el-button :disabled="busy || !!merge" @click="onNewBranch">New branch</el-button>
        </div>
        <p v-if="switchingTo" class="sync-line switching" data-testid="project-branch-switching">
          <i class="fa-solid fa-spinner fa-spin"></i>
          Switching to {{ switchingTo }} and updating the catalog…
        </p>
        <p v-else-if="syncLine" class="sync-line" data-testid="project-branch-sync">
          {{ syncLine }}
        </p>

        <div class="remote-row">
          <span class="remote-address" :title="state.remote ?? ''">
            <i class="fa-solid fa-cloud"></i>
            {{ state.remote ?? "No remote" }}
            <span v-if="state.remote && !state.has_token" class="no-token">· no token</span>
          </span>
          <button type="button" class="link-btn" @click="openRemoteDialog">
            {{ state.remote ? "Change" : "Set remote" }}
          </button>
        </div>
        <div v-if="state.remote" class="sync-actions">
          <el-button :loading="pulling" :disabled="busy || !!merge" @click="onPull">
            <i class="fa-solid fa-arrow-down"></i>&nbsp;Pull
          </el-button>
          <el-button type="primary" :loading="pushing" :disabled="busy || !!merge" @click="onPush">
            <i class="fa-solid fa-arrow-up"></i>&nbsp;Push
          </el-button>
        </div>

        <div v-if="merge" class="merge-panel" data-testid="project-merge">
          <p class="merge-title">
            Pulling {{ merge.sides.theirs }}: {{ merge.conflicts.length }} to settle
          </p>
          <p class="merge-text">
            Nothing is applied until every item is settled; then the merge is saved as a version and
            the database is rebuilt from it.
          </p>
          <ul class="merge-items">
            <li
              v-for="item in merge.conflicts"
              :key="`${item.path}:${item.key}`"
              class="merge-item"
            >
              <merge-conflict-item
                :item="item"
                :disabled="busy"
                :action-label="(side) => takeLabel(item, side)"
                @take="onSettle(item, $event)"
              >
                <template #title>
                  <strong>{{ itemFlowName(item) }}</strong> · {{ item.label }}
                </template>
                <template v-if="opensInEditor(item, merge.flows)" #actions>
                  <el-button
                    size="small"
                    link
                    type="primary"
                    :disabled="busy"
                    data-testid="project-merge-open"
                    @click="onOpenInEditor(item)"
                  >
                    Open in editor
                  </el-button>
                </template>
              </merge-conflict-item>
            </li>
          </ul>
          <div class="merge-footer">
            <el-button
              v-if="!merge.conflicts.length"
              type="primary"
              size="small"
              @click="onConclude"
            >
              Finish the merge
            </el-button>
            <el-button size="small" type="danger" plain :disabled="busy" @click="onAbort">
              Abort the merge
            </el-button>
          </div>
        </div>
      </template>
    </div>

    <el-dialog v-model="remoteDialog" title="Remote" width="460px" append-to-body>
      <label class="field-label" for="project-remote-url">Address</label>
      <el-input
        id="project-remote-url"
        v-model="remoteUrl"
        placeholder="https://git.example.com/team/project.git"
      />
      <div v-if="githubAddress" class="github-signin" data-testid="remote-github-signin">
        <template v-if="githubSignIn">
          <p class="field-hint">Enter this code on GitHub to let Flowfile push and pull:</p>
          <div class="device-code-row">
            <code class="device-code" data-testid="remote-github-code">
              {{ githubSignIn.user_code }}
            </code>
            <el-button size="small" @click="copyGithubCode">Copy</el-button>
          </div>
          <div class="device-actions">
            <el-button type="primary" size="small" @click="openGithubPage">
              Open github.com/login/device
            </el-button>
            <span class="device-waiting">Waiting for GitHub…</span>
            <el-button link size="small" @click="stopGithubSignIn">Cancel</el-button>
          </div>
        </template>
        <template v-else>
          <el-button
            :loading="githubStarting"
            data-testid="remote-github-start"
            @click="startGithubSignIn"
          >
            <i class="fa-brands fa-github"></i>&nbsp;Sign in with GitHub
          </el-button>
          <p class="field-hint">Or paste a personal access token below.</p>
        </template>
        <p v-if="githubError" class="field-error" data-testid="remote-github-error">
          {{ githubError }}
        </p>
      </div>
      <label class="field-label" for="project-remote-user">User name (optional)</label>
      <el-input id="project-remote-user" v-model="remoteUser" placeholder="x-access-token" />
      <label class="field-label" for="project-remote-token">Token</label>
      <el-input
        id="project-remote-token"
        v-model="remoteToken"
        type="password"
        show-password
        :placeholder="
          state?.has_token ? 'Kept; type a new one to replace it' : 'Personal access token'
        "
      />
      <p class="field-hint">
        The token is kept as your secret on this server and sent only to the remote, never written
        into the address or the project folder.
      </p>
      <template #footer>
        <el-button v-if="state?.has_token" link type="danger" @click="saveRemote(true)">
          Forget the token
        </el-button>
        <el-button @click="remoteDialog = false">Cancel</el-button>
        <el-button type="primary" :disabled="!remoteUrl.trim()" @click="saveRemote(false)">
          Save
        </el-button>
      </template>
    </el-dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { desktop } from "../../../lib/desktop";
import { ProjectApi } from "../../api/project.api";
import { nextPollDelayMs } from "../../pages/nodeDesigner/publishGithub";
import { copyTextEverywhere } from "../../utils/clipboardUtils";
import { ProjectUnsavedChanges } from "../../api/project.errors";
import MergeConflictItem from "../../components/merge/MergeConflictItem.vue";
import { confirmRecoveryPoint } from "../../components/project/recoveryPoint";
import { alertIncomplete } from "../../components/project/importNotice";
import { useProjectStore } from "../../stores/project-store";
import { useFlowStore } from "../../stores/flow-store";
import {
  branchNameError,
  isGithubAddress,
  itemFlowName,
  opensInEditor,
  pullMessage,
  syncText,
  takeLabel,
} from "./branchSync";
import type {
  ProjectBranchState,
  ProjectGithubSignIn,
  ProjectMergeItem,
  ProjectMergeState,
  ProjectPullResult,
} from "../../types";

const store = useProjectStore();
const flowStore = useFlowStore();
const router = useRouter();

const state = ref<ProjectBranchState | null>(null);
const merge = ref<ProjectMergeState | null>(null);
const error = ref("");
const busy = ref(false);
const pulling = ref(false);
const pushing = ref(false);
const remoteDialog = ref(false);
const remoteUrl = ref("");
const remoteUser = ref("");
const remoteToken = ref("");

const currentBranch = computed(() => state.value?.branches.find((b) => b.current) ?? null);
const syncLine = computed(() => syncText(currentBranch.value, !!state.value?.remote));

const load = async () => {
  try {
    [state.value, merge.value] = await Promise.all([
      ProjectApi.getBranches(),
      ProjectApi.getMerge(),
    ]);
    error.value = "";
  } catch (e: any) {
    error.value = e?.message || "Could not load the branches";
  }
};

onMounted(load);

/** A settle or conclusion that failed may still have moved the merge: show what core holds now. */
const reloadOnFailure = async (work: () => Promise<void>) => {
  try {
    await work();
  } catch (e) {
    await load();
    throw e;
  }
};

/** Run `call(force)`; a refusal for unsaved changes asks once, then runs it again with a recovery point. */
const withRecovery = async <T,>(call: (force: boolean) => Promise<T>): Promise<T | null> => {
  try {
    return await call(false);
  } catch (e: any) {
    if (!(e instanceof ProjectUnsavedChanges)) throw e;
    if (!(await confirmRecoveryPoint(e))) return null;
    return await call(true);
  }
};

const run = async (flag: typeof busy, work: () => Promise<void>) => {
  busy.value = true;
  flag.value = true;
  try {
    await work();
  } catch (e: any) {
    ElMessage.error(e?.message || "That did not work");
  } finally {
    busy.value = false;
    flag.value = false;
  }
};

const switchingTo = ref<string | null>(null);

/** One switch request, shown as under way only while core works on it, not while a confirm waits on the user. */
const switchTo = async (name: string, force: boolean) => {
  switchingTo.value = name;
  try {
    return await store.switchBranch(name, force);
  } finally {
    switchingTo.value = null;
  }
};

const onSwitch = (name: string) =>
  run(busy, async () => {
    if (!name || name === state.value?.current) return;
    const result = await withRecovery((force) => switchTo(name, force));
    await load();
    if (result && (await alertIncomplete(result, "Not finished"))) return;
    if (result) ElMessage.success(`Switched to ${name}`);
  });

const onNewBranch = async () => {
  let name: string;
  try {
    ({ value: name } = await ElMessageBox.prompt(
      "It starts at the current version and is checked out.",
      "New branch",
      {
        confirmButtonText: "Create",
        inputPlaceholder: "feature/fx-rates",
        inputValidator: (v: string) => branchNameError(v) ?? true,
      },
    ));
  } catch {
    return;
  }
  await run(busy, async () => {
    await ProjectApi.createBranch(name.trim());
    await load();
    ElMessage.success(`On new branch ${name.trim()}`);
  });
};

const openRemoteDialog = () => {
  remoteUrl.value = state.value?.remote ?? "";
  remoteUser.value = "";
  remoteToken.value = "";
  remoteDialog.value = true;
};

const saveRemote = (forget: boolean) =>
  run(busy, async () => {
    const token = forget ? "" : remoteToken.value || undefined;
    state.value = await ProjectApi.setRemote(
      remoteUrl.value.trim(),
      token,
      remoteUser.value.trim() || undefined,
    );
    remoteToken.value = "";
    remoteDialog.value = false;
    ElMessage.success(forget ? "Token forgotten" : "Remote saved");
  });

const githubAddress = computed(() => isGithubAddress(remoteUrl.value));
const githubSignIn = ref<ProjectGithubSignIn | null>(null);
const githubStarting = ref(false);
const githubError = ref("");
let githubTimer: ReturnType<typeof setTimeout> | null = null;
let githubInterval = 5;

const stopGithubSignIn = () => {
  if (githubTimer) clearTimeout(githubTimer);
  githubTimer = null;
  githubSignIn.value = null;
};

/** GitHub granted the sign-in and core kept the token: save the address typed, so one sign-in is the whole setup. */
const finishGithubSignIn = (login: string) =>
  run(busy, async () => {
    state.value = await ProjectApi.setRemote(
      remoteUrl.value.trim(),
      undefined,
      remoteUser.value.trim() || undefined,
    );
    remoteDialog.value = false;
    ElMessage.success(login ? `Signed in to GitHub as @${login}` : "Signed in to GitHub");
  });

const pollGithubSignIn = async () => {
  githubTimer = null;
  const signIn = githubSignIn.value;
  if (!signIn) return;
  try {
    const answer = await ProjectApi.pollGithubSignIn(signIn.device_code);
    if (githubSignIn.value !== signIn) return;
    if (answer.status === "connected") {
      stopGithubSignIn();
      await finishGithubSignIn(answer.login ?? "");
      return;
    }
    githubInterval = answer.interval ?? githubInterval;
    githubTimer = setTimeout(pollGithubSignIn, nextPollDelayMs(githubInterval, answer.status));
  } catch (e: any) {
    if (githubSignIn.value !== signIn) return;
    stopGithubSignIn();
    githubError.value = e?.message || "The GitHub sign-in failed";
  }
};

const startGithubSignIn = async () => {
  githubError.value = "";
  githubStarting.value = true;
  try {
    const signIn = await ProjectApi.startGithubSignIn();
    githubSignIn.value = signIn;
    githubInterval = signIn.interval || 5;
    githubTimer = setTimeout(pollGithubSignIn, nextPollDelayMs(githubInterval, "pending"));
  } catch (e: any) {
    githubError.value = e?.message || "Could not start the GitHub sign-in";
  } finally {
    githubStarting.value = false;
  }
};

const copyGithubCode = async () => {
  if (githubSignIn.value && (await copyTextEverywhere(githubSignIn.value.user_code))) {
    ElMessage.success("Code copied");
  }
};

const openGithubPage = () => {
  if (githubSignIn.value) desktop.openExternal(githubSignIn.value.verification_uri);
};

watch(remoteDialog, (open) => {
  if (open) return;
  stopGithubSignIn();
  githubError.value = "";
});
onBeforeUnmount(stopGithubSignIn);

const onPush = () =>
  run(pushing, async () => {
    const answer = await ProjectApi.push();
    await load();
    if (answer.unsaved) {
      ElMessage.warning(
        `Pushed ${answer.branch}. Changes not saved as a version were not pushed: save a version first.`,
      );
    } else {
      ElMessage.success(`Pushed ${answer.branch}`);
    }
  });

/** The store already ran the post-import path for a pull that applied one. */
const reportPull = async (result: ProjectPullResult) => {
  await load();
  if (result.state === "conflicted") {
    ElMessage.warning(pullMessage(result));
    return;
  }
  if (result.result && (await alertIncomplete(result.result, "Not finished"))) return;
  ElMessage.success(pullMessage(result));
};

const onPull = () =>
  run(pulling, async () => {
    const result = await withRecovery((force) => store.pull(force));
    if (result) await reportPull(result);
  });

const onSettle = (item: ProjectMergeItem, take: "ours" | "theirs") =>
  run(busy, () =>
    reloadOnFailure(async () => {
      const answer = await withRecovery((force) =>
        store.settleMerge(item.path, item.key, take, force),
      );
      if (!answer) await load();
      else if (answer.pull) await reportPull(answer.pull);
      else merge.value = answer.merge;
    }),
  );

const onConclude = () =>
  run(busy, () =>
    reloadOnFailure(async () => {
      const result = await withRecovery((force) => store.concludeMerge(force));
      if (result) await reportPull(result);
      else await load();
    }),
  );

const onAbort = async () => {
  try {
    await ElMessageBox.confirm(
      "Drop the merge? Nothing was applied, so this project stays as it is.",
      "Abort the merge",
      { confirmButtonText: "Abort the merge", cancelButtonText: "Keep it", type: "warning" },
    );
  } catch {
    return;
  }
  await run(busy, async () => {
    await store.abortMerge();
    await load();
  });
};

const onOpenInEditor = (item: ProjectMergeItem) =>
  run(busy, async () => {
    const flowId = await store.openMergeFlow(item.path);
    flowStore.setFlowId(flowId);
    await router.push({ name: "designer" });
  });

defineExpose({ load });
</script>

<style scoped>
.field-label {
  display: block;
  margin: var(--spacing-2) 0 var(--spacing-1);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.field-hint {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.field-error {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-danger);
}

.github-signin {
  margin-top: var(--spacing-3);
}

.device-code-row,
.device-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
}

.device-code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
  letter-spacing: 0.1em;
}

.device-waiting {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.branch-row,
.remote-row,
.sync-actions,
.merge-footer {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.branch-row .el-select {
  flex: 1;
}

.sync-line,
.branch-error {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.sync-line.switching {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  color: var(--color-text-secondary);
}

.branch-error {
  color: var(--color-danger);
}

.remote-row {
  justify-content: space-between;
  margin-top: var(--spacing-3);
  font-size: var(--font-size-sm);
}

.remote-address {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono);
  color: var(--color-text-secondary);
}

.no-token {
  color: var(--color-warning-dark);
}

.link-btn {
  flex-shrink: 0;
  padding: 0;
  border: none;
  background: none;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.sync-actions {
  margin-top: var(--spacing-3);
}

.merge-panel {
  margin-top: var(--spacing-3);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.merge-title {
  margin: 0;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.merge-text {
  margin: var(--spacing-1) 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.merge-items {
  margin: 0;
  padding: 0;
  list-style: none;
}

.merge-item {
  padding: var(--spacing-2) 0;
  border-bottom: 1px solid var(--color-border-light);
}

.merge-footer {
  margin-top: var(--spacing-2);
}
</style>
