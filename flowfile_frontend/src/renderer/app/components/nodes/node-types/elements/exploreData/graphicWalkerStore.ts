import { reactive } from "vue";

interface VizStore {
  exportViewSpec: () => any;
  exportAsRaw: () => string;
  importRaw: () => void;
  setVisName: () => void;
  importStoInfo: () => void;
}

interface Dataset {
  id: string;
  name: string;
  rawFields: RawField[];
  dsId: string;
}

interface RawField {
  fid: string;
  semanticType: string;
  analyticType: string;
}

interface CommonStore {
  datasets?: Dataset[];
}

interface IGlobalStore {
  currentNodeId: number;
  previousNodeId: number;
  current: {
    commonStore: CommonStore;
    vizStore: VizStore;
  };
  updateNodeId: (newNodeId: number) => void;
  nodeIdChanged: () => boolean;
}

const globalStoreInstance = reactive<IGlobalStore>({
  currentNodeId: 0,
  previousNodeId: 0,
  current: {
    commonStore: {
      datasets: [],
    },
    vizStore: {
      exportViewSpec: () => {
        return {};
      },
      exportAsRaw: () => {
        return "";
      },
      importRaw: () => {
        console.log("importing raw");
      },
      setVisName: () => {
        console.log("setting visual name");
      },
      importStoInfo: () => {
        console.log("importing sto info");
      },
    },
  },
  updateNodeId: function (newNodeId: number) {
    this.previousNodeId = this.currentNodeId;
    this.currentNodeId = newNodeId;
    console.log("Updated node id to:", newNodeId);
    console.log("Previous node id:", this.previousNodeId);
  },
  nodeIdChanged: function () {
    return this.currentNodeId !== this.previousNodeId;
  },
});

export const useGlobalStore = () => globalStoreInstance;
