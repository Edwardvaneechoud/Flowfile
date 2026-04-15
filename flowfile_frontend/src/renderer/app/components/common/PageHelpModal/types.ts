export interface HelpFeature {
  icon: string;
  title: string;
  description: string;
}

export interface HelpTip {
  type: "success" | "warning";
  title: string;
  description: string;
}

export interface HelpSection {
  title: string;
  icon?: string;
  description?: string;
  features?: HelpFeature[];
  tips?: HelpTip[];
}

export interface PageHelpContent {
  title: string;
  icon: string;
  sections: HelpSection[];
}
