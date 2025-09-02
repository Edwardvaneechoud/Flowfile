// stateStore.ts
import { defineStore } from 'pinia';
import { ref, computed } from 'vue';

export interface ItemLayout {
  width: number;
  height: number;
  left: number;
  top: number;
  stickynessPosition: 'top' | 'bottom' | 'left' | 'right' | 'free' | 'bottom-center';
  fullWidth: boolean;
  fullHeight: boolean;
  zIndex: number;
  fullScreen: boolean;
  prevWidth?: number;
  prevHeight?: number;
  prevLeft?: number;
  prevTop?: number;
  clicked: boolean;
  group?: string;
  syncDimensions?: boolean;
}

export interface ItemInitialState {
  width?: number;
  height?: number;
  left?: number;
  top?: number;
  stickynessPosition?: 'top' | 'bottom' | 'left' | 'right' | 'free';
  group?: string;
  syncDimensions?: boolean;
  fullWidth?: boolean;
  fullHeight?: boolean;
}

export const useItemStore = defineStore('itemStore', () => {
  const items = ref<Record<string, ItemLayout>>({});
  const initialItemStates = ref<Record<string, ItemInitialState>>({});
  const groups = ref<Record<string, string[]>>({});
  const inResizing = ref(false);
  const idItemClicked = ref<string | null>(null);
  const idItemVisible = ref<string | null>(null);
  
  const layoutPresets = {
    sidePanel: { width: 400, height: '100%' },
    bottomPanel: { width: '100%', height: 300 },
    dataView: { width: 600, height: 400 },
    logView: { width: 600, height: 400 }
  };

  const getGroupItems = computed(() => (groupName: string) => {
    if (!groups.value[groupName]) return [];
    return groups.value[groupName].map(id => items.value[id]).filter(Boolean);
  });


  const registerInitialState = (id: string, initialState: ItemInitialState) => {
    // Only register if not already registered (preserve the true initial state)
    if (!initialItemStates.value[id]) {
      initialItemStates.value[id] = { ...initialState };
    }
  };

  const bringToFront = (id: string) => {
    if (!items.value[id]) {
      console.warn(`Item ${id} not found`);
      return;
    }
    
    // Don't modify if item is in fullscreen
    if (items.value[id].fullScreen) return;
    
    // Find the maximum z-index among all non-fullscreen items
    let maxZIndex = 99;
    Object.entries(items.value).forEach(([itemId, item]) => {
      if (!item.fullScreen && itemId !== id) {
        maxZIndex = Math.max(maxZIndex, item.zIndex);
      }
    });
    
    // Set this item's z-index to be above all others
    items.value[id].zIndex = maxZIndex + 1;
    
    // Optionally save the state
    saveItemState(id);
  };

  
  const setItemState = (id: string, state: Partial<ItemLayout>) => {
    if (!items.value[id]) {
      items.value[id] = {
        width: 400,
        height: 300,
        left: 100,
        top: 100,
        stickynessPosition: 'free',
        fullWidth: false,
        fullHeight: false,
        zIndex: 100,
        fullScreen: false,
        clicked: false,
      };
    }
    
    const oldGroup = items.value[id].group;
    Object.assign(items.value[id], state);
    
    if (state.group !== undefined) {
      if (oldGroup && groups.value[oldGroup]) {
        groups.value[oldGroup] = groups.value[oldGroup].filter(itemId => itemId !== id);
      }
      
      if (state.group) {
        if (!groups.value[state.group]) {
          groups.value[state.group] = [];
        }
        if (!groups.value[state.group].includes(id)) {
          groups.value[state.group].push(id);
        }
        
        if (state.syncDimensions) {
          syncGroupDimensions(state.group, id);
        }
      }
    }
  };

  const syncGroupDimensions = (groupName: string, sourceId?: string) => {
    const groupItems = groups.value[groupName];
    if (!groupItems || groupItems.length < 2) return;
    
    const referenceId = sourceId || groupItems[0];
    const reference = items.value[referenceId];
    if (!reference) return;
    
    groupItems.forEach(id => {
      if (id !== referenceId && items.value[id]?.syncDimensions) {
        items.value[id].width = reference.width;
        items.value[id].height = reference.height;
        saveItemState(id);
      }
    });
  };

  const arrangeItems = (arrangement: 'cascade' | 'tile' | 'stack') => {
    const visibleItems = Object.entries(items.value)
      .filter(([_, item]) => !item.fullScreen)
      .sort((a, b) => a[1].zIndex - b[1].zIndex);
    
    switch (arrangement) {
      case 'cascade':
        let offset = 0;
        visibleItems.forEach(([id, item]) => {
          item.left = 100 + offset;
          item.top = 100 + offset;
          item.stickynessPosition = 'free';
          offset += 30;
          saveItemState(id);
        });
        break;
        
      case 'tile':
        const screenWidth = window.innerWidth;
        const screenHeight = window.innerHeight;
        const itemCount = visibleItems.length;
        const cols = Math.ceil(Math.sqrt(itemCount));
        const rows = Math.ceil(itemCount / cols);
        const itemWidth = Math.floor(screenWidth / cols) - 20;
        const itemHeight = Math.floor(screenHeight / rows) - 20;
        
        visibleItems.forEach(([id, item], index) => {
          const col = index % cols;
          const row = Math.floor(index / cols);
          item.left = col * (itemWidth + 10) + 10;
          item.top = row * (itemHeight + 10) + 10;
          item.width = itemWidth;
          item.height = itemHeight;
          item.stickynessPosition = 'free';
          saveItemState(id);
        });
        break;
        
      case 'stack':
        visibleItems.forEach(([id, item]) => {
          item.left = 100;
          item.top = 100;
          item.stickynessPosition = 'free';
          saveItemState(id);
        });
        break;
    }
  };

  const preventOverlap = (id: string) => {
    const item = items.value[id];
    if (!item || item.stickynessPosition !== 'free') return;
    
    const threshold = 50;
    let adjusted = false;
    
    Object.entries(items.value).forEach(([otherId, otherItem]) => {
      if (otherId === id || otherItem.fullScreen) return;
      
      const horizontalOverlap = 
        item.left < otherItem.left + otherItem.width &&
        item.left + item.width > otherItem.left;
      
      const verticalOverlap = 
        item.top < otherItem.top + otherItem.height &&
        item.top + item.height > otherItem.top;
      
      if (horizontalOverlap && verticalOverlap) {
        item.left = otherItem.left + otherItem.width + threshold;
        
        if (item.left + item.width > window.innerWidth) {
          item.left = 100;
          item.top = otherItem.top + otherItem.height + threshold;
        }
        adjusted = true;
      }
    });
    
    if (adjusted) {
      saveItemState(id);
    }
  };

  const saveItemState = (id: string) => {
    const itemState = items.value[id];
    localStorage.setItem(`overlayPositionAndSize_${id}`, JSON.stringify(itemState));
    
    if (itemState.group) {
      const groupData = { groups: groups.value };
      localStorage.setItem('overlayGroups', JSON.stringify(groupData));
    }
  };

  const loadItemState = (id: string) => {
    const savedState = localStorage.getItem(`overlayPositionAndSize_${id}`);
    if (savedState) {
      const state = JSON.parse(savedState);
      setItemState(id, state);
    }
    
    const savedGroups = localStorage.getItem('overlayGroups');
    if (savedGroups) {
      const groupData = JSON.parse(savedGroups);
      groups.value = groupData.groups || {};
    }
  };

  const applyPreset = (id: string, presetName: keyof typeof layoutPresets) => {
    const preset = layoutPresets[presetName];
    const updates: Partial<ItemLayout> = {};
    
    if (preset.width === '100%') {
      updates.width = window.innerWidth;
      updates.fullWidth = true;
    } else {
      updates.width = preset.width as number;
      updates.fullWidth = false;
    }
    
    if (preset.height === '100%') {
      updates.height = window.innerHeight;
      updates.fullHeight = true;
    } else {
      updates.height = preset.height as number;
      updates.fullHeight = false;
    }
    
    setItemState(id, updates);
    saveItemState(id);
  };

  const toggleFullScreen = (id: string) => {
    if (!items.value[id]) return;
    setFullScreen(id, !items.value[id].fullScreen);
  };
  
  const setFullScreen = (id: string, fullScreen: boolean) => {
    if (!items.value[id]) return;
    
    if (items.value[id].fullScreen !== fullScreen) {
      if (fullScreen) {
        Object.keys(items.value).forEach(otherId => {
          if (otherId !== id) {
            items.value[otherId].zIndex = 1;
          }
        });
        
        items.value[id].fullScreen = true;
        items.value[id].prevWidth = items.value[id].width;
        items.value[id].prevHeight = items.value[id].height;
        items.value[id].prevLeft = items.value[id].left;
        items.value[id].prevTop = items.value[id].top;
        
        items.value[id].width = window.innerWidth;
        items.value[id].height = window.innerHeight;
        items.value[id].left = 0;
        items.value[id].top = 0;
        items.value[id].zIndex = 9999;
      } else {
        items.value[id].fullScreen = false;
        items.value[id].width = items.value[id].prevWidth || 400;
        items.value[id].height = items.value[id].prevHeight || 300;
        items.value[id].left = items.value[id].prevLeft || 100;
        items.value[id].top = items.value[id].prevTop || 100;
        items.value[id].zIndex = 1000;
        
        Object.keys(items.value).forEach(otherId => {
          if (otherId !== id) {
            items.value[otherId].zIndex = 100;
          }
        });
      }
      
      saveItemState(id);
      clickOnItem(id);
    }
  };

  const resetLayout = () => {
    // Clear all current states from localStorage
    Object.keys(items.value).forEach(id => {
      localStorage.removeItem(`overlayPositionAndSize_${id}`);
    });
    
    // Clear groups from localStorage
    localStorage.removeItem('overlayGroups');
    
    // Reset groups
    groups.value = {};
    
    // First pass: Reset all items to their initial state
    Object.keys(initialItemStates.value).forEach(id => {
      const initialState = initialItemStates.value[id];
      if (!initialState) return;
      
      // Create a fresh item state based on initial values
      const resetState: ItemLayout = {
        width: initialState.width || 400,
        height: initialState.height || 300,
        left: initialState.left || 100,
        top: initialState.top || 100,
        stickynessPosition: initialState.stickynessPosition || 'free',
        fullWidth: initialState.fullWidth || false,
        fullHeight: initialState.fullHeight || false,
        zIndex: 100,
        fullScreen: false,
        clicked: false,
        group: initialState.group,
        syncDimensions: initialState.syncDimensions
      };
      
      // Set the item state
      items.value[id] = resetState;
      
      // Re-add to groups if needed
      if (resetState.group) {
        if (!groups.value[resetState.group]) {
          groups.value[resetState.group] = [];
        }
        if (!groups.value[resetState.group].includes(id)) {
          groups.value[resetState.group].push(id);
        }
      }
    });
    
    // Emit a custom event to notify components to re-apply sticky positions
    // This event should trigger each DraggableItem to call its applyStickyPosition method
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('layout-reset', { 
        detail: { initialStates: initialItemStates.value } 
      }));
    }, 0);
  };

  const resetSingleItem = (id: string) => {
    const initialState = initialItemStates.value[id];
    if (!initialState) {
      console.warn(`No initial state found for item ${id}`);
      return;
    }
    
    // Remove from localStorage
    localStorage.removeItem(`overlayPositionAndSize_${id}`);
    
    // Reset to initial state
    const resetState: ItemLayout = {
      width: initialState.width || 400,
      height: initialState.height || 300,
      left: initialState.left || 100,
      top: initialState.top || 100,
      stickynessPosition: initialState.stickynessPosition || 'free',
      fullWidth: initialState.fullWidth || false,
      fullHeight: initialState.fullHeight || false,
      zIndex: items.value[id]?.zIndex || 100,
      fullScreen: false,
      clicked: false,
      group: initialState.group,
      syncDimensions: initialState.syncDimensions
    };
    
    items.value[id] = resetState;
  };

  const clickOnItem = (id: string) => {
    if (!items.value[id] || items.value[id].fullScreen) return;
    
    let maxZIndex = 99;
    Object.values(items.value).forEach(item => {
      if (!item.fullScreen) {
        maxZIndex = Math.max(maxZIndex, item.zIndex);
      }
    });

    if (items.value[id].zIndex <= maxZIndex) {
      items.value[id].zIndex = maxZIndex + 1;
      saveItemState(id);
    }
    
    idItemClicked.value = id;
  };

  const setResizing = (resizing: boolean) => {
    inResizing.value = resizing;
  };

  const getResizing = () => {
    return inResizing.value;
  };

  const scrollOnItem = (id: string) => {
    const itemElement = document.getElementById(id);
    if (!itemElement) return;

    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          idItemVisible.value = id;
          items.value[id].zIndex = 1000;
        } else if (idItemVisible.value === id) {
          items.value[id].zIndex = 100;
          idItemVisible.value = null;
        }
      });
    }, {
      threshold: 0.5,
    });

    observer.observe(itemElement);
  };

  return {
    inResizing,
    items,
    groups,
    layoutPresets,
    initialItemStates,
    registerInitialState,
    setItemState,
    saveItemState,
    loadItemState,
    setResizing,
    getResizing,
    clickOnItem,
    scrollOnItem,
    idItemVisible,
    toggleFullScreen,
    setFullScreen,
    arrangeItems,
    preventOverlap,
    syncGroupDimensions,
    applyPreset,
    getGroupItems,
    resetLayout,
    resetSingleItem,
    bringToFront,
  };
});