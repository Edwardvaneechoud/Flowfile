import axios from "../services/axios.config";
import type { FlowTemplateMeta } from "../types/template.types";

let templatesEnsured = false;

export class TemplatesApi {
  /**
   * Ensure template YAML files are available locally (downloads from GitHub if needed).
   * Should be called before listTemplates() to handle PyPI installs without repo checkout.
   * Cached per session — only calls the backend once.
   */
  static async ensureAvailable(): Promise<void> {
    if (templatesEnsured) return;
    await axios.get("/templates/ensure_available/", {
      headers: { accept: "application/json" },
    });
    templatesEnsured = true;
  }

  /**
   * Get metadata for all available flow templates
   */
  static async listTemplates(): Promise<FlowTemplateMeta[]> {
    const response = await axios.get<FlowTemplateMeta[]>("/templates/", {
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Create a new flow from a template. Downloads CSV data if needed.
   * Returns the new flow_id.
   */
  static async createFromTemplate(templateId: string): Promise<number> {
    const response = await axios.post(
      `/templates/${templateId}/create`,
      {},
      { headers: { accept: "application/json" } },
    );
    return response.data;
  }
}
