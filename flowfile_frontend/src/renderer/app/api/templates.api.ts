import axios from "../services/axios.config";
import type { FlowTemplateMeta } from "../types/template.types";

export class TemplatesApi {
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
      "/templates/create_from_template/",
      {},
      {
        headers: { accept: "application/json" },
        params: { template_id: templateId },
      },
    );
    if (response.status === 200) {
      return response.data;
    }
    throw Error("Error creating flow from template");
  }
}
