import axios from "../services/axios.config";
import type {
  GroupMember,
  GroupRole,
  UserGroup,
  UserGroupCreate,
  UserGroupDetail,
  UserGroupUpdate,
} from "../types/sharing.types";

const BASE = "/user-groups";

export class UserGroupsApi {
  static async list(all = false): Promise<UserGroup[]> {
    const response = await axios.get<UserGroup[]>(BASE, { params: all ? { all: true } : {} });
    return response.data;
  }

  static async get(groupId: number): Promise<UserGroupDetail> {
    const response = await axios.get<UserGroupDetail>(`${BASE}/${groupId}`);
    return response.data;
  }

  static async create(data: UserGroupCreate): Promise<UserGroup> {
    const response = await axios.post<UserGroup>(BASE, data);
    return response.data;
  }

  static async update(groupId: number, data: UserGroupUpdate): Promise<UserGroup> {
    const response = await axios.patch<UserGroup>(`${BASE}/${groupId}`, data);
    return response.data;
  }

  static async remove(groupId: number): Promise<void> {
    await axios.delete(`${BASE}/${groupId}`);
  }

  static async addMember(groupId: number, userId: number, role: GroupRole): Promise<GroupMember> {
    const response = await axios.post<GroupMember>(`${BASE}/${groupId}/members`, {
      user_id: userId,
      role,
    });
    return response.data;
  }

  static async updateMember(
    groupId: number,
    userId: number,
    role: GroupRole,
  ): Promise<GroupMember> {
    const response = await axios.patch<GroupMember>(`${BASE}/${groupId}/members/${userId}`, {
      role,
    });
    return response.data;
  }

  static async removeMember(groupId: number, userId: number): Promise<void> {
    await axios.delete(`${BASE}/${groupId}/members/${userId}`);
  }
}
