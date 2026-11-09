import axios from "axios";

/** What the core that answered can do for this page; absent from an older core's answer. */
export interface Capabilities {
  /** A worker executes flows here: Run Now, SQL, the notebook, visuals, schedules that fire here. */
  computes: boolean;
  /** Table data is read here: the data preview, version history. */
  previews: boolean;
  /** The page is signed in by a cookie this origin set, with no token flow. */
  cookie_session: boolean;
}

export interface SetupStatus {
  setup_required: boolean;
  master_key_configured: boolean;
  mode: string;
  projects_enabled?: boolean;
  projects_confined?: boolean;
  git_available?: boolean;
  capabilities?: Capabilities;
}

export interface GeneratedKey {
  key: string;
  instructions: string;
}

// What a core did before it said: everything but a cookie session. Read for an older core's answer and before any.
const DEFAULT_CAPABILITIES: Capabilities = {
  computes: true,
  previews: true,
  cookie_session: false,
};

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
    const maxRetries = 5;
    const retryDelay = 1000;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const response = await axios.get<SetupStatus>("/health/status", {
          headers: { "X-Skip-Auth-Header": "true" },
          timeout: 5000,
        });
        return response.data;
      } catch {
        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }

    return {
      setup_required: true,
      master_key_configured: false,
      mode: "unknown",
      projects_enabled: false,
      projects_confined: false,
      git_available: false,
    };
  }

  async generateKey(): Promise<GeneratedKey> {
    const response = await axios.post<GeneratedKey>("/setup/generate-key", null, {
      headers: { "X-Skip-Auth-Header": "true" },
    });
    return response.data;
  }

  clearCache(): void {
    this.cachedStatus = null;
  }

  /** What the core can do, from the status the app fetched once before its first route. */
  capabilities(): Capabilities {
    return this.cachedStatus?.capabilities ?? DEFAULT_CAPABILITIES;
  }

  /** A worker executes flows here. */
  computes(): boolean {
    return this.capabilities().computes;
  }

  /** Table data is read here. */
  previews(): boolean {
    return this.capabilities().previews;
  }

  /** The page is signed in by a cookie this origin set, with no token flow. */
  cookieSession(): boolean {
    return this.capabilities().cookie_session;
  }
}

export default new SetupService();
