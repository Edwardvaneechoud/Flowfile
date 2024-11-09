declare module "drawflow" {
  export default class Drawflow {
    constructor(container: HTMLElement);
    zoom: number;
    precanvas: HTMLCanvasElement;
    node_selected: boolean;
    active_nodes: any;
    start(): void;
    import(data: any): void;
    addNode(
      name: string,
      inputs: number,
      outputs: number,
      posX: number,
      posY: number,
      className: string,
      data: object,
      html: string,
      typenode: string,
    ): void;
    destroy(): void;
  }
}
