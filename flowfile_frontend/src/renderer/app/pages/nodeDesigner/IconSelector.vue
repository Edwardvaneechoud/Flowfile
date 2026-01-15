<template>
  <div class="icon-selector">
    <label class="icon-label">Node Icon</label>
    <div class="icon-selector-content">
      <!-- Current icon preview -->
      <div class="current-icon" @click="toggleDropdown">
        <img
          :src="getDisplayUrl(modelValue)"
          :alt="modelValue"
          class="icon-preview"
          @error="handleImageError"
        />
        <span class="icon-name">{{ modelValue || "Select icon..." }}</span>
        <i class="fa-solid fa-chevron-down dropdown-arrow"></i>
      </div>

      <!-- Dropdown -->
      <div v-if="showDropdown" class="icon-dropdown">
        <!-- Upload section -->
        <div class="upload-section">
          <label class="upload-btn">
            <i class="fa-solid fa-upload"></i>
            Upload Icon
            <input
              type="file"
              accept=".png,.jpg,.jpeg,.svg,.gif,.webp"
              hidden
              @change="handleFileUpload"
            />
          </label>
        </div>

        <!-- Custom icons section -->
        <div v-if="customIcons.length > 0" class="icons-section">
          <div class="section-title">Custom Icons</div>
          <div class="icons-grid">
            <div
              v-for="icon in customIcons"
              :key="icon.file_name"
              class="icon-option"
              :class="{ selected: modelValue === icon.file_name }"
              @click="selectIcon(icon.file_name)"
            >
              <img
                :src="getCustomIconUrl(icon.file_name)"
                :alt="icon.file_name"
                class="icon-img"
                @error="handleImageError"
              />
              <span class="icon-filename">{{ icon.file_name }}</span>
              <button
                class="delete-icon-btn"
                title="Delete icon"
                @click.stop="deleteIcon(icon.file_name)"
              >
                <i class="fa-solid fa-times"></i>
              </button>
            </div>
          </div>
        </div>

        <!-- Default icon -->
        <div class="icons-section">
          <div class="section-title">Default</div>
          <div class="icons-grid">
            <div
              class="icon-option"
              :class="{ selected: modelValue === 'user-defined-icon.png' }"
              @click="selectIcon('user-defined-icon.png')"
            >
              <img
                :src="getBuiltinIconUrl('user-defined-icon.png')"
                alt="Default"
                class="icon-img"
              />
              <span class="icon-filename">Default</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Click outside to close -->
    <div v-if="showDropdown" class="backdrop" @click="showDropdown = false"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import axios from "axios";
import type { IconInfo } from "./types";
import {
  getImageUrl,
  getCustomIconUrl as getCustomIconUrlUtil,
  getDefaultIconUrl,
} from "../../features/designer/utils";

const props = defineProps<{
  modelValue: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();

const showDropdown = ref(false);
const customIcons = ref<IconInfo[]>([]);
const loading = ref(false);

function toggleDropdown() {
  showDropdown.value = !showDropdown.value;
  if (showDropdown.value) {
    loadIcons();
  }
}

async function loadIcons() {
  loading.value = true;
  try {
    const response = await axios.get("/user_defined_components/list-icons");
    customIcons.value = response.data;
  } catch (error) {
    console.error("Failed to load icons:", error);
  } finally {
    loading.value = false;
  }
}

function selectIcon(iconName: string) {
  emit("update:modelValue", iconName);
  showDropdown.value = false;
}

async function handleFileUpload(event: Event) {
  const target = event.target as HTMLInputElement;
  const file = target.files?.[0];
  if (!file) return;

  const formData = new FormData();
  formData.append("file", file);

  try {
    const response = await axios.post("/user_defined_components/upload-icon", formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
    });

    // Select the newly uploaded icon
    emit("update:modelValue", response.data.file_name);

    // Reload icon list
    await loadIcons();
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || "Failed to upload icon";
    alert(`Error uploading icon: ${errorMsg}`);
  }

  // Reset the input
  target.value = "";
}

async function deleteIcon(iconName: string) {
  if (!confirm(`Are you sure you want to delete "${iconName}"?`)) return;

  try {
    await axios.delete(`/user_defined_components/delete-icon/${iconName}`);

    // If the deleted icon was selected, reset to default
    if (props.modelValue === iconName) {
      emit("update:modelValue", "user-defined-icon.png");
    }

    // Reload icon list
    await loadIcons();
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || "Failed to delete icon";
    alert(`Error deleting icon: ${errorMsg}`);
  }
}

function getDisplayUrl(iconName: string): string {
  return getImageUrl(iconName);
}

function getCustomIconUrl(iconName: string): string {
  return getCustomIconUrlUtil(iconName);
}

function getBuiltinIconUrl(iconName: string): string {
  return new URL(`../../features/designer/assets/icons/${iconName}`, import.meta.url).href;
}

function handleImageError(event: Event) {
  const img = event.target as HTMLImageElement;
  img.src = getDefaultIconUrl();
}

onMounted(() => {
  loadIcons();
});
</script>

<style scoped>
.icon-selector {
  position: relative;
}

.icon-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  display: block;
  margin-bottom: var(--spacing-1);
}

.icon-selector-content {
  position: relative;
}

.current-icon {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  cursor: pointer;
  transition: border-color var(--transition-normal);
}

.current-icon:hover {
  border-color: var(--color-accent);
}

.icon-preview {
  width: 24px;
  height: 24px;
  object-fit: contain;
}

.icon-name {
  flex: 1;
  font-size: var(--font-size-base);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dropdown-arrow {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.icon-dropdown {
  position: absolute;
  top: 100%;
  left: 0;
  right: 0;
  margin-top: var(--spacing-1);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-lg);
  z-index: var(--z-index-dropdown);
  max-height: 300px;
  overflow-y: auto;
}

.upload-section {
  padding: var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
}

.upload-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  width: 100%;
  padding: var(--spacing-2);
  background: var(--color-background-secondary);
  border: 1px dashed var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-base);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-normal);
}

.upload-btn:hover {
  background: var(--color-background-tertiary);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.icons-section {
  padding: var(--spacing-2);
}

.section-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: var(--spacing-2);
  padding: 0 var(--spacing-1);
}

.icons-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(80px, 1fr));
  gap: var(--spacing-2);
}

.icon-option {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-2);
  border: 2px solid transparent;
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-normal);
}

.icon-option:hover {
  background: var(--color-background-secondary);
}

.icon-option.selected {
  border-color: var(--color-accent);
  background: var(--color-focus-ring-accent);
}

.icon-img {
  width: 32px;
  height: 32px;
  object-fit: contain;
}

.icon-filename {
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
  text-align: center;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 100%;
}

.delete-icon-btn {
  position: absolute;
  top: 2px;
  right: 2px;
  width: 16px;
  height: 16px;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-full);
  background: var(--color-danger);
  color: var(--color-text-inverse);
  font-size: var(--font-size-2xs);
  cursor: pointer;
  opacity: 0;
  transition: opacity var(--transition-normal);
  display: flex;
  align-items: center;
  justify-content: center;
}

.icon-option:hover .delete-icon-btn {
  opacity: 1;
}

.delete-icon-btn:hover {
  background: var(--color-danger-hover);
}

.backdrop {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  z-index: 999;
}
</style>