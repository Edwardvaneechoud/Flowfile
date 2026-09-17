// Expressions API Service - Handles expression documentation requests
import axios from "../services/axios.config";
import type { ExpressionsOverview } from "../types";

/** The expression list never changes within a session; one fetch serves every editor. */
let expressionNames: Promise<string[]> | null = null;

export class ExpressionsApi {
  /**
   * Fetch all available expressions overview/documentation
   */
  static async getExpressionsOverview(): Promise<ExpressionsOverview[]> {
    const response = await axios.get<ExpressionsOverview[]>("/editor/expression_doc");
    return response.data;
  }

  /**
   * Fetch the flat list of expression names, cached for the session.
   */
  static async getExpressionNames(): Promise<string[]> {
    if (!expressionNames) {
      expressionNames = axios
        .get<string[]>("/editor/expressions")
        .then((response) => response.data)
        .catch((error) => {
          expressionNames = null;
          throw error;
        });
    }
    return expressionNames;
  }
}
