export { IMutField, IRow, VizSpecStore, IChart } from "@kanaries/graphic-walker";
import { IMutField, IRow, VizSpecStore, IChart } from "@kanaries/graphic-walker";
import { NodeBase } from "../../../../baseNode/nodeInput";

export interface DataModel {
  data: IRow[];
  fields: IMutField[];
}

export interface GraphicWalkerInput {
  is_initial: boolean;
  dataModel: DataModel;
  specList: IChart[];
}

export interface NodeGraphicWalker extends NodeBase {
  graphic_walker_input: GraphicWalkerInput;
}

export interface IGlobalStore {
  current: VizSpecStore;
}
