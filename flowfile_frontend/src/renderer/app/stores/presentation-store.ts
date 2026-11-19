import { defineStore } from "pinia";

/**
 * Whether a page asked the app shell to hide its chrome (the sidebar, the catalog's tabs and
 * tree, floating helpers) so it fills the window. The page that sets it clears it when it leaves.
 */
export const usePresentationStore = defineStore("presentation", {
  state: () => ({ presenting: false }),
  actions: {
    enter() {
      this.presenting = true;
    },
    exit() {
      this.presenting = false;
    },
  },
});
