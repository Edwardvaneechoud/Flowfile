<template>
  <div class="card card-timeline">
    <div class="card-header-inline">
      <h3 class="card-title">
        <i class="fa-solid fa-timeline"></i>
        Task Timeline
      </h3>
      <span class="task-count">{{ processes.processes.length }} active</span>
    </div>

    <div v-if="processes.processes.length === 0" class="empty-timeline">
      <div class="empty-icon">
        <i class="fa-solid fa-inbox"></i>
      </div>
      <p class="empty-text">No active tasks at the moment</p>
      <p class="empty-subtext">Tasks will appear here when they start running</p>
    </div>

    <div v-else class="timeline-wrapper">
      <div class="timeline-container">
        <div class="timeline-line"></div>
        <div 
          v-for="process in sortedProcesses" 
          :key="process.task_id"
          class="timeline-item"
          :class="getTimelineItemClass(process.status)"
          @click="toggleExpanded(process.task_id)"
        >
          <div class="timeline-marker">
            <i class="fa-solid" :class="getProcessIcon(process.status)"></i>
          </div>
          <div class="timeline-content">
            <div class="timeline-header">
              <div class="timeline-title">
                <span class="task-id">{{ process.task_id }}</span>
                <span class="status-badge" :class="getProcessStatusClass(process.status)">
                  {{ process.status }}
                </span>
              </div>
              <button class="btn-expand" :class="{ 'expanded': expandedTasks.has(process.task_id) }">
                <i class="fa-solid fa-chevron-down"></i>
              </button>
            </div>
            
            <div class="timeline-meta">
              <span v-if="process.start_time" class="meta-item">
                <i class="fa-solid fa-clock"></i>
                Started {{ formatTimeAgo(process.start_time) }}
              </span>
              <span v-if="process.pid" class="meta-item">
                <i class="fa-solid fa-microchip"></i>
                PID: {{ process.pid }}
              </span>
              <span class="meta-item">
                <i class="fa-solid fa-stopwatch"></i>
                {{ getDurationText(process.start_time) }}
              </span>
            </div>

            <transition name="slide-fade">
              <div v-if="expandedTasks.has(process.task_id)" class="timeline-details">
                <div class="detail-grid">
                  <div class="detail-item">
                    <span class="detail-label">Task ID</span>
                    <span class="detail-value">{{ process.task_id }}</span>
                  </div>
                  <div class="detail-item" v-if="process.pid">
                    <span class="detail-label">Process ID</span>
                    <span class="detail-value">{{ process.pid }}</span>
                  </div>
                  <div class="detail-item">
                    <span class="detail-label">Status</span>
                    <span class="detail-value">{{ process.status }}</span>
                  </div>
                  <div class="detail-item" v-if="process.start_time">
                    <span class="detail-label">Start Time</span>
                    <span class="detail-value">{{ formatTimestamp(process.start_time) }}</span>
                  </div>
                </div>
              </div>
            </transition>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import { type ProcessMetrics, type ProcessInfo } from './types';

interface Props {
  processes: ProcessMetrics;
}

const props = defineProps<Props>();

const expandedTasks = ref(new Set<string>());

const sortedProcesses = computed(() => {
  return [...props.processes.processes].sort((a, b) => {
    if (a.start_time && b.start_time) {
      return b.start_time - a.start_time;
    }
    return 0;
  });
});

const toggleExpanded = (taskId: string) => {
  if (expandedTasks.value.has(taskId)) {
    expandedTasks.value.delete(taskId);
  } else {
    expandedTasks.value.add(taskId);
  }
};

const getTimelineItemClass = (status: string): string => {
  const statusLower = status.toLowerCase();
  if (statusLower === 'completed') return 'timeline-completed';
  if (statusLower === 'error' || statusLower === 'failed') return 'timeline-failed';
  if (statusLower === 'processing') return 'timeline-processing';
  return 'timeline-default';
};

const getProcessIcon = (status: string): string => {
  const statusLower = status.toLowerCase();
  if (statusLower === 'completed') return 'fa-check-circle';
  if (statusLower === 'error' || statusLower === 'failed') return 'fa-times-circle';
  if (statusLower === 'processing') return 'fa-spinner fa-spin';
  if (statusLower === 'starting') return 'fa-play-circle';
  return 'fa-circle';
};

const getProcessStatusClass = (status: string): string => {
  const statusLower = status.toLowerCase();
  if (statusLower === 'completed') return 'status-success';
  if (statusLower === 'error' || statusLower === 'failed') return 'status-danger';
  if (statusLower === 'processing' || statusLower === 'starting') return 'status-processing';
  return 'status-default';
};

const formatTimeAgo = (timestamp: number): string => {
  const now = Date.now();
  const diff = Math.floor((now - timestamp * 1000) / 1000);

  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
};

const getDurationText = (startTime: number | null): string => {
  if (!startTime) return 'N/A';
  const now = Date.now();
  const diff = Math.floor((now - startTime * 1000) / 1000);
  
  if (diff < 60) return `${diff}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ${diff % 60}s`;
  return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m`;
};

const formatTimestamp = (timestamp: number): string => {
  const date = new Date(timestamp * 1000);
  return date.toLocaleString();
};
</script>

<style scoped>
.card-timeline {
  background: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  padding: 1.5rem;
  transition: all 0.3s ease;
}

.card-timeline:hover {
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

.task-count {
  background: #e0e7ef;
  padding: 0.35rem 0.75rem;
  border-radius: 12px;
  font-size: 0.85rem;
  font-weight: 600;
  color: #1a365d;
}

.empty-timeline {
  text-align: center;
  padding: 3rem 1rem;
}

.empty-icon {
  font-size: 4rem;
  color: #cbd5e1;
  margin-bottom: 1rem;
}

.empty-text {
  font-size: 1.1rem;
  font-weight: 600;
  color: #64748b;
  margin: 0 0 0.5rem 0;
}

.empty-subtext {
  color: #94a3b8;
  margin: 0;
}

.timeline-wrapper {
  max-height: 600px;
  overflow-y: auto;
  overflow-x: hidden;
  padding-right: 0.5rem;
}

.timeline-wrapper::-webkit-scrollbar {
  width: 8px;
}

.timeline-wrapper::-webkit-scrollbar-track {
  background: #f1f5f9;
  border-radius: 4px;
}

.timeline-wrapper::-webkit-scrollbar-thumb {
  background: #cbd5e1;
  border-radius: 4px;
}

.timeline-wrapper::-webkit-scrollbar-thumb:hover {
  background: #94a3b8;
}

.timeline-container {
  position: relative;
  padding-left: 2rem;
}

.timeline-line {
  position: absolute;
  left: 1.5rem;
  top: 0;
  bottom: 0;
  width: 2px;
  background: linear-gradient(to bottom, var(--primary-blue), #e0e7ef);
}

.timeline-item {
  position: relative;
  margin-bottom: 1.5rem;
  cursor: pointer;
  transition: all 0.3s ease;
}

.timeline-item:hover .timeline-content {
  background: #f8fafc;
  border-color: var(--primary-blue);
}

.timeline-marker {
  position: absolute;
  left: -2rem;
  top: 0.5rem;
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: white;
  border: 3px solid;
  border-radius: 50%;
  font-size: 1rem;
  z-index: 10;
}

.timeline-processing .timeline-marker {
  border-color: #17a2b8;
  color: #17a2b8;
  animation: pulse 2s ease-in-out infinite;
}

.timeline-completed .timeline-marker {
  border-color: #28a745;
  color: #28a745;
}

.timeline-failed .timeline-marker {
  border-color: #dc3545;
  color: #dc3545;
}

.timeline-default .timeline-marker {
  border-color: #6c757d;
  color: #6c757d;
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

.timeline-content {
  background: white;
  border: 2px solid #e2e8f0;
  border-radius: 8px;
  padding: 1rem;
  margin-left: 1rem;
  transition: all 0.3s ease;
}

.timeline-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.75rem;
}

.timeline-title {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.task-id {
  font-weight: 700;
  color: #1a365d;
  font-size: 1rem;
}

.status-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 12px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.status-processing {
  background: #d1ecf1;
  color: #0c5460;
}

.status-success {
  background: #d4edda;
  color: #155724;
}

.status-danger {
  background: #f8d7da;
  color: #721c24;
}

.status-default {
  background: #e2e3e5;
  color: #383d41;
}

.btn-expand {
  background: none;
  border: none;
  color: #64748b;
  cursor: pointer;
  padding: 0.25rem;
  transition: all 0.3s ease;
  font-size: 1rem;
}

.btn-expand:hover {
  color: var(--primary-blue);
}

.btn-expand.expanded i {
  transform: rotate(180deg);
}

.btn-expand i {
  transition: transform 0.3s ease;
}

.timeline-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  font-size: 0.85rem;
  color: #64748b;
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 0.35rem;
}

.meta-item i {
  opacity: 0.6;
}

.timeline-details {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #e2e8f0;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.detail-item {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.detail-label {
  font-size: 0.75rem;
  color: #64748b;
  text-transform: uppercase;
  font-weight: 600;
  letter-spacing: 0.5px;
}

.detail-value {
  font-size: 0.95rem;
  color: #1a365d;
  font-weight: 600;
}

.slide-fade-enter-active,
.slide-fade-leave-active {
  transition: all 0.3s ease;
}

.slide-fade-enter-from {
  opacity: 0;
  transform: translateY(-10px);
}

.slide-fade-leave-to {
  opacity: 0;
  transform: translateY(-10px);
}
</style>