<template>
  <div class="card card-health">
    <div class="card-header-inline">
      <h3 class="card-title">Service Health</h3>
      <div class="health-badge" :class="getHealthStatusClass(health.status)">
        <i class="fa-solid" :class="getHealthStatusIcon(health.status)"></i>
        <span>{{ health.status.toUpperCase() }}</span>
      </div>
    </div>
    <div class="health-info">
      <div class="health-item">
        <div class="health-icon">
          <i class="fa-solid fa-server"></i>
        </div>
        <div class="health-details">
          <span class="health-label">Service</span>
          <span class="health-value">{{ health.service_name }}</span>
        </div>
      </div>
      <div class="health-item">
        <div class="health-icon">
          <i class="fa-solid fa-clock"></i>
        </div>
        <div class="health-details">
          <span class="health-label">Uptime</span>
          <span class="health-value">{{ formatUptime(health.uptime_seconds) }}</span>
        </div>
      </div>
      <div class="health-item">
        <div class="health-icon">
          <i class="fa-solid fa-code-branch"></i>
        </div>
        <div class="health-details">
          <span class="health-label">Version</span>
          <span class="health-value">{{ health.version }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { type HealthStatus } from './types';

interface Props {
  health: HealthStatus;
}

defineProps<Props>();

const getHealthStatusClass = (status: string): string => {
  const statusLower = status.toLowerCase();
  if (statusLower === 'healthy') return 'health-healthy';
  if (statusLower === 'degraded') return 'health-degraded';
  return 'health-unhealthy';
};

const getHealthStatusIcon = (status: string): string => {
  const statusLower = status.toLowerCase();
  if (statusLower === 'healthy') return 'fa-check-circle';
  if (statusLower === 'degraded') return 'fa-exclamation-circle';
  return 'fa-times-circle';
};

const formatUptime = (seconds: number): string => {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
};
</script>

<style scoped>
.card-health {
  background: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  padding: 1.5rem;
  transition: all 0.3s ease;
}

.card-health:hover {
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
}

.health-badge {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border-radius: 20px;
  font-weight: 600;
  font-size: 0.9rem;
  text-transform: uppercase;
}

.health-healthy {
  background: #d4edda;
  color: #155724;
}

.health-degraded {
  background: #fff3cd;
  color: #856404;
}

.health-unhealthy {
  background: #f8d7da;
  color: #721c24;
}

.health-info {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1.5rem;
}

.health-item {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 1rem;
  background: #f8fafc;
  border-radius: 8px;
  border-left: 4px solid var(--primary-blue);
}

.health-icon {
  width: 48px;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--primary-blue);
  color: white;
  border-radius: 10px;
  font-size: 1.5rem;
}

.health-details {
  display: flex;
  flex-direction: column;
}

.health-label {
  font-size: 0.85rem;
  color: #64748b;
  margin-bottom: 0.25rem;
}

.health-value {
  font-size: 1.1rem;
  font-weight: 700;
  color: #1a365d;
}
</style>