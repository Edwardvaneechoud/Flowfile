import axios from './axios-setup';
import type { MonitoringOverview } from '../pages/monitoring/types';

class MonitoringService {
  private readonly endpoint = 'monitoring/overview';

  /**
   * Fetch monitoring overview data from the worker service via core proxy
   */
  async getOverview(): Promise<MonitoringOverview> {
    const response = await axios.get<MonitoringOverview>(this.endpoint);
    return response.data;
  }

  /**
   * Check if monitoring service is available
   */
  async checkHealth(): Promise<boolean> {
    try {
      await this.getOverview();
      return true;
    } catch (error) {
      console.error('Monitoring service health check failed:', error);
      return false;
    }
  }
}

export default new MonitoringService();