// src/app/services/setup.service.ts
import axios from "axios";

export interface SetupStatus {
  setup_required: boolean;
  master_key_configured: boolean;
  mode: string;
}

export interface GeneratedKey {
  key: string;
  instructions: string;
}

class SetupService {
  private cachedStatus: SetupStatus | null = null;
  private statusPromise: Promise<SetupStatus> | null = null;

  async getSetupStatus(forceRefresh = false): Promise<SetupStatus> {
    if (!forceRefresh && this.cachedStatus) {
      return this.cachedStatus;
    }

    if (this.statusPromise) {
      return this.statusPromise;
    }

    this.statusPromise = this.fetchStatus();
    try {
      this.cachedStatus = await this.statusPromise;
      return this.cachedStatus;
    } finally {
      this.statusPromise = null;
    }
  }

  private async fetchStatus(): Promise<SetupStatus> {
    try {
      const response = await axios.get<SetupStatus>("/health/status", {
        headers: { "X-Skip-Auth-Header": "true" },
      });
      return response.data;
    } catch (error) {
      console.error("Failed to fetch setup status:", error);
      // If we can't reach the backend, assume setup is not required
      // (the backend might just be starting up)
      return {
        setup_required: false,
        master_key_configured: true,
        mode: "unknown",
      };
    }
  }

  async generateKey(): Promise<GeneratedKey> {
    const response = await axios.post<GeneratedKey>("/setup/generate-key", null, {
      headers: { "X-Skip-Auth-Header": "true" },
    });
    return response.data;
  }

  isSetupRequired(): boolean {
    return this.cachedStatus?.setup_required ?? false;
  }

  clearCache(): void {
    this.cachedStatus = null;
  }
}

export const setupService = new SetupService();
export default setupService;
