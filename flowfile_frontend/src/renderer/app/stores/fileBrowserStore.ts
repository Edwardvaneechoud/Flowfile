import { defineStore } from 'pinia'

export type SortByType = "name" | "size" | "last_modified" | "created_date"
export type SortDirectionType = "asc" | "desc"

export const useFileBrowserStore = defineStore('fileBrowser', {
  state: () => ({
    // Initialize state from localStorage if available, otherwise use defaults.
    sortBy: (localStorage.getItem('fileBrowser_sortBy') as SortByType) || "name",
    sortDirection: (localStorage.getItem('fileBrowser_sortDirection') as SortDirectionType) || "asc",
  }),

  actions: {
    setSortBy(newSortBy: SortByType) {
      this.sortBy = newSortBy
      localStorage.setItem('fileBrowser_sortBy', newSortBy)
    },

    setSortDirection(newSortDirection: SortDirectionType) {
      this.sortDirection = newSortDirection
      localStorage.setItem('fileBrowser_sortDirection', newSortDirection)
    },

    toggleSortDirection() {
      this.sortDirection = this.sortDirection === "asc" ? "desc" : "asc"
      localStorage.setItem('fileBrowser_sortDirection', this.sortDirection)
    },
  },
})
