// Desktop update prompt state: what the feed offers, and how far an install got.
import { defineStore } from "pinia";

import { desktop, isDesktop, type UpdateInfo } from "../../lib/desktop";
import { createDbBackup } from "../api/system.api";
import {
  loadIgnoredVersion,
  saveIgnoredVersion,
  shouldPromptForUpdate,
} from "../components/settings/updatePrompt";

export type UpdatePhase =
  | "idle"
  | "downloading"
  | "download-failed"
  | "backing-up"
  | "backup-failed"
  | "installing"
  | "install-failed";

interface UpdateState {
  info: UpdateInfo | null;
  /** Read from localStorage once; the visibility getter reads this, never storage. */
  ignoredVersion: string | null;
  checked: boolean;
  checking: boolean;
  checkError: boolean;
  dismissed: boolean;
  /** A manual check re-offers a skipped version. */
  force: boolean;
  phase: UpdatePhase;
  progress: { downloaded: number; total: number | null };
  backupPath: string | null;
  error: string | null;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export const useUpdateStore = defineStore("update", {
  state: (): UpdateState => ({
    info: null,
    ignoredVersion: loadIgnoredVersion(),
    checked: false,
    checking: false,
    checkError: false,
    dismissed: false,
    force: false,
    phase: "idle",
    progress: { downloaded: 0, total: null },
    backupPath: null,
    error: null,
  }),

  getters: {
    promptVisible: (s): boolean =>
      shouldPromptForUpdate({
        availableVersion: s.info?.version ?? null,
        ignoredVersion: s.ignoredVersion,
        force: s.force,
      }) && !s.dismissed,
  },

  actions: {
    /**
     * Startup check. Silent by design: a 404 feed, an unsigned build or a
     * missing platform key throws, and none of that is the user's problem.
     */
    async checkOnStartup(): Promise<void> {
      if (!isDesktop || import.meta.env.DEV) return;
      if (this.checked) return;
      this.checked = true;
      this.checking = true;
      try {
        this.info = await desktop.checkForUpdate();
      } catch (error) {
        console.warn("[update] check failed:", error);
      } finally {
        this.checking = false;
      }
    },

    /** Manual check from About: re-offers a skipped version and reports failure. */
    async checkNow(): Promise<UpdateInfo | null> {
      this.checking = true;
      this.checkError = false;
      this.info = null;
      this.force = true;
      this.dismissed = false;
      try {
        this.info = await desktop.checkForUpdate();
      } catch (error) {
        console.warn("[update] check failed:", error);
        this.info = null;
        this.checkError = true;
      } finally {
        this.checked = true;
        this.checking = false;
      }
      return this.info;
    },

    /** Download, snapshot the catalog DB while the old core is alive, then install. */
    async install(): Promise<void> {
      this.error = null;
      this.backupPath = null;
      this.progress = { downloaded: 0, total: null };
      this.phase = "downloading";
      try {
        await desktop.downloadUpdate((downloaded, total) => {
          this.progress = { downloaded, total };
        });
      } catch (error) {
        this.error = messageOf(error);
        this.phase = "download-failed";
        return;
      }

      this.phase = "backing-up";
      try {
        this.backupPath = (await createDbBackup("pre_update")).path;
      } catch (error) {
        this.error = messageOf(error);
        this.phase = "backup-failed";
        return;
      }

      await this.runInstall();
    },

    async continueWithoutBackup(): Promise<void> {
      await this.runInstall();
    },

    async runInstall(): Promise<void> {
      this.error = null;
      this.phase = "installing";
      try {
        await desktop.installUpdate();
      } catch (error) {
        this.error = messageOf(error);
        this.phase = "install-failed";
      }
    },

    skipVersion(): void {
      const version = this.info?.version;
      if (version) {
        this.ignoredVersion = version;
        saveIgnoredVersion(version);
      }
      this.dismissed = true;
    },

    dismiss(): void {
      this.dismissed = true;
    },

    /** Back to the offer screen — after a cancelled backup, and on every re-open. */
    resetPhase(): void {
      this.phase = "idle";
      this.progress = { downloaded: 0, total: null };
      this.backupPath = null;
      this.error = null;
    },

    async restart(): Promise<void> {
      try {
        await desktop.restartApp();
      } catch (error) {
        this.error = messageOf(error);
      }
    },
  },
});
