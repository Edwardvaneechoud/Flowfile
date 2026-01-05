// src/app/services/user.service.ts
import axios from 'axios';

export interface User {
  id: number;
  username: string;
  email?: string;
  full_name?: string;
  disabled: boolean;
  is_admin: boolean;
  must_change_password: boolean;
}

export interface UserCreate {
  username: string;
  password: string;
  email?: string;
  full_name?: string;
  is_admin?: boolean;
}

export interface UserUpdate {
  email?: string;
  full_name?: string;
  disabled?: boolean;
  is_admin?: boolean;
  password?: string;
  must_change_password?: boolean;
}

class UserService {
  /**
   * Get all users (admin only)
   */
  async getUsers(): Promise<User[]> {
    const response = await axios.get<User[]>('/auth/users');
    return response.data;
  }

  /**
   * Create a new user (admin only)
   */
  async createUser(userData: UserCreate): Promise<User> {
    const response = await axios.post<User>('/auth/users', userData);
    return response.data;
  }

  /**
   * Update a user (admin only)
   */
  async updateUser(userId: number, userData: UserUpdate): Promise<User> {
    const response = await axios.put<User>(`/auth/users/${userId}`, userData);
    return response.data;
  }

  /**
   * Delete a user (admin only)
   */
  async deleteUser(userId: number): Promise<void> {
    await axios.delete(`/auth/users/${userId}`);
  }
}

export const userService = new UserService();
export default userService;
