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
    // Retry logic for when backend is starting up
    const maxRetries = 5;
    const retryDelay = 1000; // 1 second

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const response = await axios.get<SetupStatus>("/health/status", {
          headers: { "X-Skip-Auth-Header": "true" },
          timeout: 5000,
        });
        return response.data;
      } catch (error) {
        console.warn(`Setup status check attempt ${attempt}/${maxRetries} failed:`, error);

        if (attempt < maxRetries) {
          // Wait before retrying
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }

    // After all retries failed, assume setup IS required (safe default)
    // This prevents bypassing the setup screen when backend is slow
    console.error("Failed to reach backend after retries, defaulting to setup required");
    return {
      setup_required: true,
      master_key_configured: false,
      mode: "unknown",
    };
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
