<template>
  <div class="card card-info">
    <div class="card-header-inline">
      <h3 class="card-title">
        <i class="fa-solid fa-info-circle"></i>
        Service Information
      </h3>
    </div>
    <div class="info-grid">
      <div v-for="(value, key) in serviceInfo" :key="key" class="info-item">
        <span class="info-label">{{ formatLabel(key) }}</span>
        <span class="info-value">{{ value }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
interface Props {
  serviceInfo: Record<string, string>;
}

defineProps<Props>();

const formatLabel = (key: string): string => {
  return key
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
};
</script>

<style scoped>
.card-info {
  background: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  padding: 1.5rem;
  transition: all 0.3s ease;
}

.card-info:hover {
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

.info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.info-item {
  display: flex;
  flex-direction: column;
  padding: 1rem;
  background: #f8fafc;
  border-radius: 8px;
  border-left: 3px solid var(--primary-blue);
}

.info-label {
  font-size: 0.8rem;
  color: #64748b;
  margin-bottom: 0.35rem;
  text-transform: uppercase;
  font-weight: 600;
  letter-spacing: 0.5px;
}

.info-value {
  font-size: 1rem;
  font-weight: 700;
  color: #1a365d;
}
</style>