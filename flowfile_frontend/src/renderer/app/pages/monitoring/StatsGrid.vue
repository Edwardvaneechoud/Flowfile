<template>
  <div class="stats-container">
    <div class="stat-card stat-running">
      <div class="stat-icon">
        <i class="fa-solid fa-play-circle"></i>
      </div>
      <div class="stat-content">
        <span class="stat-value">{{ processes.running_processes }}</span>
        <span class="stat-label">Running Tasks</span>
      </div>
      <div class="stat-trend">
        <div class="pulse-dot"></div>
      </div>
    </div>

    <div class="stat-card stat-completed">
      <div class="stat-icon">
        <i class="fa-solid fa-check-circle"></i>
      </div>
      <div class="stat-content">
        <span class="stat-value">{{ processes.completed_tasks }}</span>
        <span class="stat-label">Completed</span>
      </div>
    </div>

    <div class="stat-card stat-failed">
      <div class="stat-icon">
        <i class="fa-solid fa-times-circle"></i>
      </div>
      <div class="stat-content">
        <span class="stat-value">{{ processes.failed_tasks }}</span>
        <span class="stat-label">Failed</span>
      </div>
    </div>

    <div class="stat-card stat-total">
      <div class="stat-icon">
        <i class="fa-solid fa-list-check"></i>
      </div>
      <div class="stat-content">
        <span class="stat-value">{{ processes.total_processes }}</span>
        <span class="stat-label">Total Tasks</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { type ProcessMetrics } from './types';

interface Props {
  processes: ProcessMetrics;
}

defineProps<Props>();
</script>

<style scoped>
.stats-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.stat-card {
  background: white;
  border-radius: 12px;
  padding: 1.5rem;
  display: flex;
  align-items: center;
  gap: 1rem;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
  position: relative;
  overflow: hidden;
}

.stat-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 6px 16px rgba(0, 0, 0, 0.12);
}

.stat-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  width: 4px;
  height: 100%;
  background: currentColor;
}

.stat-running {
  color: #17a2b8;
}

.stat-completed {
  color: #28a745;
}

.stat-failed {
  color: #dc3545;
}

.stat-total {
  color: #6c757d;
}

.stat-icon {
  font-size: 2.5rem;
  opacity: 0.8;
}

.stat-content {
  display: flex;
  flex-direction: column;
  flex: 1;
}

.stat-value {
  font-size: 2rem;
  font-weight: 700;
  line-height: 1;
  margin-bottom: 0.25rem;
}

.stat-label {
  font-size: 0.9rem;
  opacity: 0.8;
  font-weight: 500;
}

.stat-trend {
  display: flex;
  align-items: center;
}

.pulse-dot {
  width: 12px;
  height: 12px;
  background: currentColor;
  border-radius: 50%;
  animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.5;
    transform: scale(1.2);
  }
}
</style>