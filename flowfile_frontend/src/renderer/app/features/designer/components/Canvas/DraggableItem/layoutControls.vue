<template>
    <div class="layout-widget-wrapper">
      <Transition name="panel-fade">
        <div v-if="isOpen" class="panel">
          <div class="panel-header">
            <span class="panel-title">Layout Controls</span>
            <button @click="isOpen = false" class="close-btn" title="Close">✕</button>
          </div>
          <div class="panel-body">
            <button @click="runAction(arrangeLayout, 'tile')" class="control-btn">
              📊 Tile Layout
            </button>
            <button @click="runAction(arrangeLayout, 'cascade')" class="control-btn">
              🗂️ Cascade
            </button>
            <button @click="runAction(syncAllGroups)" class="control-btn">
              🔗 Sync All
            </button>
            <button @click="runAction(resetLayout)" class="control-btn">
              🔄 Reset Layout
            </button>
          </div>
        </div>
      </Transition>
  
      <button @click="isOpen = !isOpen" class="trigger-btn" title="Toggle Layout Controls">
        <span class="icon">{{ isOpen ? '✕' : '⚙️' }}</span>
      </button>
    </div>
  </template>
  
  <script setup lang="ts">
  import { ref } from 'vue';
  import { useItemStore } from './stateStore'; // Assuming stateStore is in the same directory
  
  const itemStore = useItemStore();
  const isOpen = ref(false);
  
  // Helper function to run an action and then close the panel
  const runAction = (action: Function, ...args: any[]) => {
    action(...args);
    isOpen.value = false;
  };
  
  // Arrange all windows in a specific layout
  const arrangeLayout = (layout: 'tile' | 'cascade') => {
    itemStore.arrangeItems(layout);
  };
  
  // Sync all groups
  const syncAllGroups = () => {
    if (itemStore.groups) {
      Object.keys(itemStore.groups).forEach(groupName => {
        itemStore.syncGroupDimensions(groupName);
      });
    }
  };
  
  // Reset layout by clearing saved positions from localStorage
  const resetLayout = () => {
    // Assuming your itemStore can identify keys to clear
    Object.keys(localStorage).forEach(key => {
      if (key.startsWith('overlayPositionAndSize_')) {
        localStorage.removeItem(key);
      }
    });
    // Reload the page to apply the default layout
    window.location.reload();
  };
  </script>
  
  <style scoped>
  .layout-widget-wrapper {
    position: fixed;
    bottom: 20px;
    right: 20px;
    z-index: 20000;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
  }
  
  .trigger-btn {
    width: 60px;
    height: 60px;
    border-radius: 50%;
    border: none;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.3s ease;
  }
  
  .trigger-btn:hover {
    transform: scale(1.1);
    box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
  }
  
  .trigger-btn .icon {
    font-size: 28px;
    color: white;
    transition: transform 0.3s ease;
  }
  
  /* Rotate icon when opening/closing */
  .trigger-btn .icon {
    transform: rotate(0deg);
  }
  .layout-widget-wrapper .trigger-btn .icon:not(:empty) {
    transform: rotate(180deg);
  }
  
  .panel {
    width: 250px;
    background: white;
    border-radius: 12px;
    box-shadow: 0 5px 20px rgba(0, 0, 0, 0.2);
    margin-bottom: 15px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  
  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    border-bottom: 1px solid #f0f0f0;
  }
  
  .panel-title {
    font-weight: 600;
    font-size: 16px;
    color: #333;
  }
  
  .close-btn {
    background: none;
    border: none;
    font-size: 20px;
    color: #888;
    cursor: pointer;
    padding: 0;
    line-height: 1;
  }
  .close-btn:hover {
    color: #333;
  }
  
  .panel-body {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  
  .control-btn {
    background-color: #f5f5f5;
    color: #333;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 10px 16px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 8px;
    text-align: left;
  }
  
  .control-btn:hover {
    background-color: #e9e9e9;
    border-color: #ccc;
    transform: translateY(-1px);
  }
  
  /* Vue Transition Styles */
  .panel-fade-enter-active,
  .panel-fade-leave-active {
    transition: all 0.25s ease;
  }
  
  .panel-fade-enter-from,
  .panel-fade-leave-to {
    opacity: 0;
    transform: translateY(15px);
  }
  </style>