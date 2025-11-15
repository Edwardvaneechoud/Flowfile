<template>
  <div class="monitoring-dashboard">
    <!-- Fixed Header -->
    <div class="dashboard-header">
      <div class="header-content">
        <div class="title-section">
          <h2 class="page-title">
            <i class="fa-solid fa-chart-line"></i>
            System Monitoring
          </h2>
          <p class="description-text">
            Real-time monitoring of your Flowfile Worker service
          </p>
        </div>
        <button class="btn-refresh" @click="refreshData" :disabled="isRefreshing">
          <i class="fa-solid fa-rotate-right" :class="{ 'spinning': isRefreshing }"></i>
          <span>Refresh</span>
        </button>
      </div>
    </div>

    <!-- Scrollable Content -->
    <div class="dashboard-content">
      <div v-if="isLoading" class="loading-state">
        <div class="loading-spinner"></div>
        <p>Loading monitoring data...</p>
      </div>

      <div v-else-if="error" class="error-state">
        <i class="fa-solid fa-exclamation-triangle"></i>
        <p>{{ error }}</p>
        <button class="btn-primary" @click="refreshData">Try Again</button>
      </div>

      <div v-else-if="monitoringData" class="monitoring-grid">
        <!-- Health Status Card -->
        <HealthCard :health="monitoringData.health" />

        <!-- Quick Stats -->
        <StatsGrid :processes="monitoringData.processes" />

        <!-- System Metrics -->
        <SystemMetrics :system="monitoringData.system" />

        <!-- Task Timeline -->
        <TaskTimeline :processes="monitoringData.processes" />

        <!-- Service Information -->
        <ServiceInfo :service-info="monitoringData.service_info" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import HealthCard from './monitoring/HealthCard.vue';
import StatsGrid from './monitoring/StatsGrid.vue';
import SystemMetrics from './monitoring/SystemMetrics.vue';
import TaskTimeline from './monitoring/TaskTimeline.vue';
import ServiceInfo from './monitoring/ServiceInfo.vue';
import monitoringService from '../services/monitoring.service';
import type { MonitoringOverview } from './monitoring/types';

const monitoringData = ref<MonitoringOverview | null>(null);
const isLoading = ref(true);
const isRefreshing = ref(false);
const error = ref<string | null>(null);
let refreshInterval: number | null = null;

const fetchMonitoringData = async () => {
  try {
    error.value = null;
    monitoringData.value = await monitoringService.getOverview();
  } catch (err: any) {
    console.error('Failed to fetch monitoring data:', err);
    error.value = err.response?.data?.detail || 'Failed to connect to monitoring service. Please ensure the Flowfile Worker is running.';
  } finally {
    isLoading.value = false;
    isRefreshing.value = false;
  }
};

const refreshData = async () => {
  isRefreshing.value = true;
  await fetchMonitoringData();
};

onMounted(() => {
  fetchMonitoringData();
  refreshInterval = window.setInterval(fetchMonitoringData, 5000);
});

onUnmounted(() => {
  if (refreshInterval) {
    clearInterval(refreshInterval);
  }
});
</script>

<style scoped>
.monitoring-dashboard {
  height: 100vh;
  display: flex;
  flex-direction: column;
  background: linear-gradient(135deg, #f5f7fa 0%, #e8eef5 100%);
}

.dashboard-header {
  background: white;
  border-bottom: 2px solid #e0e7ef;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  position: sticky;
  top: 0;
  z-index: 100;
}

.header-content {
  max-width: 1400px;
  margin: 0 auto;
  padding: 1.5rem 2rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-section {
  flex: 1;
}

.page-title {
  font-size: 1.75rem;
  font-weight: 700;
  color: #1a365d;
  margin: 0 0 0.5rem 0;
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.page-title i {
  color: var(--primary-blue);
}

.description-text {
  color: #64748b;
  font-size: 0.95rem;
  margin: 0;
}

.btn-refresh {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1.5rem;
  background: var(--primary-blue);
  color: white;
  border: none;
  border-radius: 8px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.btn-refresh:hover:not(:disabled) {
  background: #0056d4;
  transform: translateY(-2px);
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
}

.btn-refresh:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.spinning {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.dashboard-content {
  flex: 1;
  overflow-y: auto;
  overflow-x: hidden;
  padding: 2rem;
}

.monitoring-grid {
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

/* Loading & Error States */
.loading-state,
.error-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 4rem 2rem;
  text-align: center;
}

.loading-spinner {
  width: 60px;
  height: 60px;
  border: 4px solid #e2e8f0;
  border-top-color: var(--primary-blue);
  border-radius: 50%;
  animation: spin 1s linear infinite;
  margin-bottom: 1.5rem;
}

.error-state i {
  font-size: 4rem;
  color: #dc3545;
  margin-bottom: 1rem;
}

.error-state p {
  color: #64748b;
  margin-bottom: 1.5rem;
  max-width: 500px;
}

.btn-primary {
  padding: 0.75rem 2rem;
  background: var(--primary-blue);
  color: white;
  border: none;
  border-radius: 8px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s ease;
}

.btn-primary:hover {
  background: #0056d4;
  transform: translateY(-2px);
}

/* Responsive Design */
@media (max-width: 768px) {
  .dashboard-content {
    padding: 1rem;
  }

  .header-content {
    padding: 1rem;
    flex-direction: column;
    gap: 1rem;
  }
}
</style>
