// Expressions API Service - Handles expression documentation requests
import axios from '../axios-setup';
import type { ExpressionsOverview } from '../../features/designer/baseNode/nodeInterfaces';

export class ExpressionsApi {
  /**
   * Fetch all available expressions overview/documentation
   */
  static async getExpressionsOverview(): Promise<ExpressionsOverview[]> {
    const response = await axios.get<ExpressionsOverview[]>('/editor/expression_doc');
    return response.data;
  }
}
