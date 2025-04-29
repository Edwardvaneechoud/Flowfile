import { defineStore } from 'pinia';
import { ref } from 'vue';

export const useItemStore = defineStore('itemStore', () => {
  const items = ref<Record<string, {
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
  }>>({});

  const setItemState = (id: string, state: Partial<typeof items.value[string]>) => {
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
    Object.assign(items.value[id], state);
  };

  const saveItemState = (id: string) => {
    const itemState = items.value[id];
    localStorage.setItem(`overlayPositionAndSize_${id}`, JSON.stringify(itemState));
  };

  const loadItemState = (id: string) => {
    const savedState = localStorage.getItem(`overlayPositionAndSize_${id}`);
    if (savedState) {
      items.value[id] = JSON.parse(savedState);
    }
  };

  const toggleFullScreen = (id: string) => {
    if (!items.value[id]) return;
    // Use the setFullScreen method with the opposite of current state
    setFullScreen(id, !items.value[id].fullScreen);
  };
  
  const setFullScreen = (id: string, fullScreen: boolean) => {
    if (!items.value[id]) return;
    
    if (items.value[id].fullScreen !== fullScreen) {
      if (fullScreen) {
        // Save current state and go fullscreen
        items.value[id].fullScreen = true;
        items.value[id].prevWidth = items.value[id].width;
        items.value[id].prevHeight = items.value[id].height;
        items.value[id].prevLeft = items.value[id].left;
        items.value[id].prevTop = items.value[id].top;
        
        // Use full window dimensions with no margins
        items.value[id].width = window.innerWidth;
        items.value[id].height = window.innerHeight;
        items.value[id].left = 0;
        items.value[id].top = 0;
      } else {
        // Return to previous state
        items.value[id].fullScreen = false;
        items.value[id].width = items.value[id].prevWidth || 400;
        items.value[id].height = items.value[id].prevHeight || 300;
        items.value[id].left = items.value[id].prevLeft || 100;
        items.value[id].top = items.value[id].prevTop || 100;
      }
      
      saveItemState(id);
      
      clickOnItem(id);
    }
  };

  const inResizing = ref(false); // Global mouseDown state
  const idItemClicked = ref<string | null>(null); // Id of the item clicked
  const idItemVisible = ref<string | null>(null); // Id of the currently visible item

  const clickOnItem = (id: string) => {
    if (idItemClicked.value === id) return;
    if (idItemClicked.value) {
      items.value[idItemClicked.value].zIndex = 100;
    }
    items.value[id].zIndex = 1000;
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

    // Create an Intersection Observer to observe when the item becomes visible
    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          idItemVisible.value = id;
          items.value[id].zIndex = 1000; // Bring item to front when visible
        } else if (idItemVisible.value === id) {
          items.value[id].zIndex = 100; // Reset zIndex when it's not visible anymore
          idItemVisible.value = null;
        }
      });
    }, {
      threshold: 0.5, // Trigger when 50% of the item is visible
    });

    observer.observe(itemElement);
  };

  return {
    inResizing,
    items,
    setItemState,
    saveItemState,
    loadItemState,
    setResizing,
    getResizing,
    clickOnItem,
    scrollOnItem,
    idItemVisible, // Exposing the visible item
    toggleFullScreen, // Toggle fullscreen state
    setFullScreen, // Set fullscreen state with boolean parameter
  };
});