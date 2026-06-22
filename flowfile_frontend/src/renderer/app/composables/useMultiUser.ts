import { ref } from "vue";
import setupService from "../services/setup.service";

// Sharing affordances only exist in multi-user (docker) mode. Backend "electron"
// mode 404s /user-groups and /shares; the catalog is global there. We derive the
// flag from the same GET /health/status the setup flow already fetches.
const isMultiUser = ref(false);
const projectsEnabled = ref(false);
const projectsConfined = ref(false);
let resolved = false;

export function useMultiUser() {
  async function refresh(): Promise<boolean> {
    try {
      const status = await setupService.getSetupStatus();
      isMultiUser.value = status.mode === "docker";
      projectsEnabled.value = status.projects_enabled ?? false;
      projectsConfined.value = status.projects_confined ?? false;
    } catch {
      isMultiUser.value = false;
      projectsEnabled.value = false;
      projectsConfined.value = false;
    }
    resolved = true;
    return isMultiUser.value;
  }

  if (!resolved) {
    void refresh();
  }

  return { isMultiUser, projectsEnabled, projectsConfined, refresh };
}
