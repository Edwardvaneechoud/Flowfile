<template>
  <div class="icon-selector">
    <label class="icon-label">Node Icon</label>
    <el-popover
      v-model:visible="showDropdown"
      placement="bottom-start"
      trigger="click"
      :width="340"
      popper-class="icon-selector-popper"
      @show="loadIcons"
    >
      <template #reference>
        <div class="current-icon">
          <img :src="selectedIconUrl" :alt="modelValue" class="icon-preview" />
          <span class="icon-name">{{ selectedIconLabel }}</span>
          <i class="fa-solid fa-chevron-down dropdown-arrow"></i>
        </div>
      </template>

      <div class="icon-dropdown-content">
        <div class="upload-section">
          <label class="btn btn-secondary upload-btn">
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

        <div v-if="customIcons.length > 0" class="icons-section">
          <div class="section-title">Your Icons</div>
          <div class="icons-grid">
            <div
              v-for="icon in customIcons"
              :key="icon.file_name"
              class="icon-option"
              :class="{ selected: modelValue === icon.file_name }"
              @click="selectIcon(icon.file_name)"
            >
              <img
                :src="customIconUrls[icon.file_name] || defaultIconUrl"
                :alt="icon.file_name"
                class="icon-img"
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

        <div class="icons-section">
          <div class="section-title">Standard Icons</div>
          <div class="icons-grid">
            <div
              class="icon-option"
              :class="{ selected: modelValue === DEFAULT_ICON_NAME }"
              @click="selectIcon(DEFAULT_ICON_NAME)"
            >
              <img :src="defaultIconUrl" alt="Default" class="icon-img" />
              <span class="icon-filename">Default</span>
            </div>
            <div
              v-for="name in STANDARD_NODE_ICONS"
              :key="name"
              class="icon-option"
              :class="{ selected: modelValue === name }"
              @click="selectIcon(name)"
            >
              <img :src="getImageUrl(name)" :alt="name" class="icon-img" />
              <span class="icon-filename">{{ standardIconLabel(name) }}</span>
            </div>
          </div>
        </div>
      </div>
    </el-popover>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import axios from "axios";
import { ElPopover } from "element-plus";
import type { IconInfo } from "./types";
import {
  STANDARD_NODE_ICONS,
  getDefaultIconUrl,
  getImageUrl,
  isBuiltinIcon,
} from "../../features/designer/utils";
import {
  fetchIconObjectUrl,
  invalidateIconCache,
  useNodeIconUrl,
} from "@/composables/useCustomNodeIcon";

const DEFAULT_ICON_NAME = "user-defined-icon.png";

const props = defineProps<{
  modelValue: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();

const showDropdown = ref(false);
const customIcons = ref<IconInfo[]>([]);
const customIconUrls = reactive<Record<string, string>>({});
const defaultIconUrl = getDefaultIconUrl();

const selectedIconUrl = useNodeIconUrl(() => props.modelValue);

const selectedIconLabel = computed(() => {
  if (!props.modelValue) return "Select icon...";
  if (props.modelValue === DEFAULT_ICON_NAME) return "Default";
  if (isBuiltinIcon(props.modelValue)) return standardIconLabel(props.modelValue);
  return props.modelValue;
});

function standardIconLabel(name: string): string {
  return name.replace(/\.svg$/, "").replace(/_/g, " ");
}

async function loadIcons() {
  try {
    const response = await axios.get("/user_defined_components/list-icons");
    customIcons.value = response.data;
    for (const icon of customIcons.value) {
      fetchIconObjectUrl(icon.file_name).then((url) => {
        if (url) customIconUrls[icon.file_name] = url;
      });
    }
  } catch (error) {
    console.error("Failed to load icons:", error);
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

    invalidateIconCache(response.data.file_name);
    emit("update:modelValue", response.data.file_name);

    await loadIcons();
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || "Failed to upload icon";
    alert(`Error uploading icon: ${errorMsg}`);
  }

  target.value = "";
}

async function deleteIcon(iconName: string) {
  if (!confirm(`Are you sure you want to delete "${iconName}"?`)) return;

  try {
    await axios.delete(`/user_defined_components/delete-icon/${iconName}`);
    invalidateIconCache(iconName);
    delete customIconUrls[iconName];

    if (props.modelValue === iconName) {
      emit("update:modelValue", DEFAULT_ICON_NAME);
    }

    await loadIcons();
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || "Failed to delete icon";
    alert(`Error deleting icon: ${errorMsg}`);
  }
}

onMounted(() => {
  loadIcons();
});
</script>

<style scoped>
.icon-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  display: block;
  margin-bottom: var(--spacing-1);
}

.current-icon {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
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
  width: 20px;
  height: 20px;
  object-fit: contain;
}

.icon-name {
  flex: 1;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dropdown-arrow {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.icon-dropdown-content {
  max-height: 340px;
  overflow-y: auto;
}

.upload-section {
  padding-bottom: var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.upload-btn {
  width: 100%;
}

.icons-section {
  padding: var(--spacing-2) 0;
}

.section-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: var(--spacing-2);
}

.icons-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(72px, 1fr));
  gap: var(--spacing-1);
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
</style>
