// User groups + resource sharing types (mirror flowfile_core/schemas/sharing_schema.py).

export type ResourceType =
  | "secret"
  | "database_connection"
  | "cloud_connection"
  | "ga_connection"
  | "kafka_connection"
  | "catalog_namespace"
  | "catalog_table"
  | "flow"
  | "visualization"
  | "dashboard"
  | "global_artifact";

export type PermissionLevel = "use" | "manage";
export type GroupRole = "owner" | "manager" | "member";

export interface AccessInfo {
  is_owner: boolean;
  access_level: "owner" | "manage" | "use";
  shared_by?: string | null;
}

export interface UserGroup {
  id: number;
  name: string;
  description?: string | null;
  created_by: number;
  member_count: number;
  my_role?: GroupRole | null;
  created_at?: string | null;
}

export interface GroupMember {
  user_id: number;
  username: string;
  full_name?: string | null;
  role: GroupRole;
}

export interface UserGroupDetail extends UserGroup {
  members: GroupMember[];
}

export interface UserGroupCreate {
  name: string;
  description?: string | null;
}

export interface UserGroupUpdate {
  name?: string | null;
  description?: string | null;
}

export interface ShareCreate {
  resource_type: ResourceType;
  resource_id: number;
  group_id: number;
  permission: PermissionLevel;
}

export interface Share {
  id: number;
  resource_type: ResourceType;
  resource_id: number;
  group_id: number;
  group_name: string;
  permission: PermissionLevel;
  granted_by?: number | null;
  granted_by_username?: string | null;
  created_at?: string | null;
}
