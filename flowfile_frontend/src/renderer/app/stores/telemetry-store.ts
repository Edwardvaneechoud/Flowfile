import { defineStore } from "pinia";
import {
  getTelemetryStatus,
  setTelemetryConsent,
  type TelemetryStatus,
} from "../api/telemetry.api";

interface TelemetryState {
  status: TelemetryStatus | null;
  loaded: boolean;
}

export const useTelemetryStore = defineStore("telemetry", {
  state: (): TelemetryState => ({
    status: null,
    loaded: false,
  }),

  actions: {
    /**
     * Load-once status fetch. Failures are swallowed on purpose: telemetry
     * must never nag, so a broken backend just keeps the consent modal hidden
     * (loaded stays false and the visibility predicate short-circuits).
     */
    async loadStatus(force = false): Promise<TelemetryStatus | null> {
      if (this.loaded && !force) {
        return this.status;
      }
      try {
        this.status = await getTelemetryStatus();
        this.loaded = true;
      } catch {
        /* keep quiet — see docstring */
      }
      return this.status;
    },

    /**
     * Adopts the server response (never optimistic). On failure the local
     * state is left unchanged and the error rethrown so the settings card can
     * toast; the consent modal swallows it instead.
     */
    async setConsent(enabled: boolean): Promise<TelemetryStatus> {
      const next = await setTelemetryConsent(enabled);
      this.status = next;
      this.loaded = true;
      return next;
    },
  },
});
