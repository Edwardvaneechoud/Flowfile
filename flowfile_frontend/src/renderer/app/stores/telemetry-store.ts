import { defineStore } from "pinia";
import {
  getTelemetryStatus,
  setTelemetryConsent,
  type TelemetryStatus,
} from "../api/telemetry.api";

interface TelemetryState {
  status: TelemetryStatus | null;
  loaded: boolean;
  /** The last status read failed, so `status` is unknown or stale. */
  loadFailed: boolean;
}

export const useTelemetryStore = defineStore("telemetry", {
  state: (): TelemetryState => ({
    status: null,
    loaded: false,
    loadFailed: false,
  }),

  actions: {
    /**
     * Load-once status fetch. Failures never throw: telemetry must never nag,
     * so a broken backend just keeps the consent modal hidden (loaded stays
     * false and the visibility predicate short-circuits). `loadFailed` records
     * that the state is unknown so the settings card can say so instead of
     * rendering the previous — or default — value as fact.
     */
    async loadStatus(force = false): Promise<TelemetryStatus | null> {
      if (this.loaded && !force) {
        return this.status;
      }
      try {
        this.status = await getTelemetryStatus();
        this.loaded = true;
        this.loadFailed = false;
      } catch {
        this.loadFailed = true;
      }
      return this.status;
    },

    /**
     * Adopts the server response (never optimistic). On failure the local
     * state is left unchanged and the error rethrown so the settings card can
     * toast and the consent modal can offer a retry.
     */
    async setConsent(enabled: boolean): Promise<TelemetryStatus> {
      const next = await setTelemetryConsent(enabled);
      this.status = next;
      this.loaded = true;
      this.loadFailed = false;
      return next;
    },
  },
});
