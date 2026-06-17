export interface FlowTemplateMeta {
  template_id: string;
  name: string;
  description: string;
  category: "Beginner" | "Intermediate" | "Advanced";
  tags: string[];
  node_count: number;
  icon: string;
}
