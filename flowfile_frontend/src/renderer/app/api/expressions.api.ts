// Expressions API Service - Handles expression documentation requests
import axios from "../services/axios.config";
import type { ExpressionsOverview } from "../types";

export class ExpressionsApi {
  /**
   * Fetch all available expressions overview/documentation
   */
  static async getExpressionsOverview(): Promise<ExpressionsOverview[]> {
    const response = await axios.get<ExpressionsOverview[]>("/editor/expression_doc");
    return response.data;
  }
}
