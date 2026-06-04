import { SampleUsers } from "../../../baseNode/nodeInput";

type ConfigType = "SAMPLE_USERS" | "GOOGLE_SHEET";

export function get_template_source_type(type: ConfigType, options?: any): SampleUsers {
  switch (type) {
    case "SAMPLE_USERS":
      return {
        SAMPLE_USERS: true,
        size: options?.size || 100,
        orientation: options?.orientation || "row",
        fields: [],
      } as SampleUsers;
    default:
      throw new Error("Unsupported configuration type");
  }
}
