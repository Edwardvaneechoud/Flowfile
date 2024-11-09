import { reactive, ref } from 'vue'
import { VizSpecStore } from '@kanaries/graphic-walker'

interface IGlobalStore {
  current: VizSpecStore | null // Allow for nullable values to prevent TypeScript errors
}

const globalStoreInstance = reactive<IGlobalStore>({
  current: null,
})

export const useGraphicWalkerStore = (): IGlobalStore => globalStoreInstance as IGlobalStore
export const resetGraphicWalkerStore = () => {
  globalStoreInstance.current = null
}
