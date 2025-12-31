import { reactive } from 'vue'

interface VizStore {
  exportViewSpec: () => any // Adapt this to your actual return type
  exportAsRaw: () => string
  importRaw: (raw: string) => void
  setVisName: (name: string) => void
  importStoInfo: (stoInfo: any) => void
}

interface Dataset {
  id: string
  name: string
  rawFields: RawField[]
  dsId: string
}

interface RawField {
  fid: string
  semanticType: string
  analyticType: string
}

interface CommonStore {
  datasets?: Dataset[]
  // Add other properties as needed
}

interface IGlobalStore {
  currentNodeId: number
  previousNodeId: number
  current: {
    commonStore: CommonStore
    vizStore: VizStore
    // Add other properties as necessary
  }
  updateNodeId: (newNodeId: number) => void
  nodeIdChanged: () => boolean
}

// Create a single instance of the global store
const globalStoreInstance = reactive<IGlobalStore>({
  currentNodeId: 0,
  previousNodeId: 0,
  current: {
    commonStore: {
      datasets: [],
    },
    vizStore: {
      exportViewSpec: () => {
        return {}
      },
      exportAsRaw: () => {
        return ''
      },
      importRaw: (_raw: string) => {
        console.log('importing raw')
      },
      setVisName: (_name: string) => {
        console.log('setting visual name')
      },
      importStoInfo: (_stoInfo: any) => {
        console.log('importing sto info')
      },
    },
  },
  updateNodeId: function (newNodeId: number) {
    this.previousNodeId = this.currentNodeId
    this.currentNodeId = newNodeId
    console.log('Updated node id to:', newNodeId)
    console.log('Previous node id:', this.previousNodeId)
  },
  nodeIdChanged: function () {
    return this.currentNodeId !== this.previousNodeId
  },
})

export const useGlobalStore = () => globalStoreInstance
