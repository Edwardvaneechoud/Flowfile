<template>
  <div class="card card-metrics">
    <div class="card-header-inline">
      <h3 class="card-title">
        <i class="fa-solid fa-gauge-high"></i>
        System Metrics
      </h3>
    </div>
    <div class="metrics-grid">
      <div class="metric-item">
        <div class="metric-header">
          <i class="fa-solid fa-microchip"></i>
          <span class="metric-label">CPU Cores</span>
        </div>
        <div class="metric-value-large">{{ system.cpu_count }}</div>
      </div>

      <div class="metric-item">
        <div class="metric-header">
          <i class="fa-solid fa-memory"></i>
          <span class="metric-label">Memory Usage</span>
        </div>
        <div class="metric-progress">
          <div class="progress-bar">
            <div 
              class="progress-fill" 
              :class="getMemoryStatusClass(system.memory_usage_percent)"
              :style="{ width: system.memory_usage_percent + '%' }"
            ></div>
          </div>
          <div class="progress-label">
            <span class="progress-percent">{{ system.memory_usage_percent.toFixed(1) }}%</span>
            <span class="progress-text">
              {{ formatMemory(system.memory_used_mb) }} / 
              {{ formatMemory(system.memory_total_mb) }}
            </span>
          </div>
        </div>
      </div>

      <div class="metric-item" v-if="system.disk_usage_percent">
        <div class="metric-header">
          <i class="fa-solid fa-hard-drive"></i>
          <span class="metric-label">Disk Usage</span>
        </div>
        <div class="metric-progress">
          <div class="progress-bar">
            <div 
              class="progress-fill" 
              :class="getDiskStatusClass(system.disk_usage_percent)"
              :style="{ width: system.disk_usage_percent + '%' }"
            ></div>
          </div>
          <div class="progress-label">
            <span class="progress-percent">{{ system.disk_usage_percent.toFixed(1) }}%</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { type SystemMetrics } from './types';

interface Props {
  system: SystemMetrics;
}

defineProps<Props>();

const getMemoryStatusClass = (percent: number): string => {
  if (percent > 90) return 'progress-danger';
  if (percent > 75) return 'progress-warning';
  return 'progress-success';
};

const getDiskStatusClass = (percent: number): string => {
  if (percent > 90) return 'progress-danger';
  if (percent > 75) return 'progress-warning';
  return 'progress-success';
};

const formatMemory = (mb: number): string => {
  if (mb >= 1024) {
    return `${(mb / 1024).toFixed(2)} GB`;
  }
  return `${mb.toFixed(0)} MB`;
};
</script>

<style scoped>
.card-metrics {
  background: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  padding: 1.5rem;
  transition: all 0.3s ease;
}

.card-metrics:hover {
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
}

.card-header-inline {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1.5rem;
}

.card-title {
  font-size: 1.25rem;
  font-weight: 700;
  color: #1a365d;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.card-title i {
  color: var(--primary-blue);
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1.5rem;
}

.metric-item {
  background: #f8fafc;
  padding: 1.25rem;
  border-radius: 8px;
  border: 1px solid #e2e8f0;
}

.metric-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1rem;
  color: #64748b;
  font-weight: 600;
}

.metric-header i {
  color: var(--primary-blue);
}

.metric-value-large {
  font-size: 2.5rem;
  font-weight: 700;
  color: #1a365d;
}

.metric-progress {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.progress-bar {
  height: 12px;
  background: #e2e8f0;
  border-radius: 6px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  border-radius: 6px;
  transition: width 0.5s ease, background-color 0.3s ease;
}

.progress-success {
  background: linear-gradient(90deg, #28a745, #20c997);
}

.progress-warning {
  background: linear-gradient(90deg, #ffc107, #fd7e14);
}

.progress-danger {
  background: linear-gradient(90deg, #dc3545, #c82333);
}

.progress-label {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.progress-percent {
  font-size: 1.25rem;
  font-weight: 700;
  color: #1a365d;
}

.progress-text {
  font-size: 0.85rem;
  color: #64748b;
}
</style>