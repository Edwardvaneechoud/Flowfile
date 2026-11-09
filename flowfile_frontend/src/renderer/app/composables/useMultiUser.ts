import { ref } from "vue";
import setupService from "../services/setup.service";

// Sharing affordances only exist in multi-user (docker) mode. Backend "electron"
// mode 404s /user-groups and /shares; the catalog is global there. We derive the
// flag from the same GET /health/status the setup flow already fetches.
const isMultiUser = ref(false);
const projectsEnabled = ref(false);
const projectsConfined = ref(false);
const gitAvailable = ref(false);
let resolved = false;

export function useMultiUser() {
  async function refresh(): Promise<boolean> {
    try {
      const status = await setupService.getSetupStatus();
      // A server's page (a hub's own page answers `server`) is multi-user too: its catalog is shared, and who
      // may manage it shows.
      isMultiUser.value = status.mode === "docker" || status.mode === "server";
      projectsEnabled.value = status.projects_enabled ?? false;
      projectsConfined.value = status.projects_confined ?? false;
      gitAvailable.value = status.git_available ?? false;
    } catch {
      isMultiUser.value = false;
      projectsEnabled.value = false;
      projectsConfined.value = false;
      gitAvailable.value = false;
    }
    resolved = true;
    return isMultiUser.value;
  }

  if (!resolved) {
    void refresh();
  }

  return { isMultiUser, projectsEnabled, projectsConfined, gitAvailable, refresh };
}
