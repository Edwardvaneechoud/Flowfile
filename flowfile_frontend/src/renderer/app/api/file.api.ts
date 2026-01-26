// File API Service - Handles file system operations
// Consolidated from features/designer/components/fileBrowser/fileSystemApi.ts
import axios from "../services/axios.config";
import type { FileInfo, DirectoryContentsParams } from "../types";
import path from "path-browserify";

const handleApiError = (error: any): never => {
  throw {
    message: error.response?.data?.detail || "An unknown error occurred",
    status: error.response?.status || 500,
  };
};

export class FileApi {
  /**
   * Get the list of files in the current directory (legacy - uses backend state)
   * @deprecated Use getDirectoryContents with explicit directory path instead
   */
  static async getCurrentDirectoryContents(params?: DirectoryContentsParams): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>("files/current_directory_contents/", { params });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Get the contents of a specific directory (stateless - preferred method)
   */
  static async getDirectoryContents(
    directory: string,
    params?: DirectoryContentsParams,
  ): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>("files/directory_contents/", {
        params: { directory, ...params },
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Navigate up one directory level (legacy - uses backend state)
   * @deprecated Use getParentPath and getDirectoryContents instead
   */
  static async navigateUp(): Promise<string> {
    try {
      const response = await axios.post<string>("files/navigate_up/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Navigate into a subdirectory (legacy - uses backend state)
   * @deprecated Use path.join and getDirectoryContents instead
   */
  static async navigateInto(directoryName: string): Promise<string> {
    try {
      const response = await axios.post<string>("files/navigate_into/", null, {
        params: { directory_name: directoryName },
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Navigate to a specific directory path (legacy - uses backend state)
   * @deprecated Use getDirectoryContents with explicit path instead
   */
  static async navigateTo(directoryPath: string): Promise<string> {
    try {
      const response = await axios.post<string>("files/navigate_to/", null, {
        params: { directory_name: directoryPath },
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Get the current directory path (legacy - uses backend state)
   * @deprecated Use stored path from fileBrowserStore instead
   */
  static async getCurrentPath(): Promise<string> {
    try {
      const response = await axios.get<string>("files/current_path/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Get the default/home directory path from the backend
   * This is useful for initializing a new browser context
   */
  static async getDefaultPath(): Promise<string> {
    try {
      const response = await axios.get<string>("files/current_path/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Create a new directory
   */
  static async createDirectory(directoryName: string): Promise<boolean> {
    try {
      const response = await axios.post<boolean>("files/create_directory", {
        name: directoryName,
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Get the full directory tree
   */
  static async getFileTree(): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>("files/tree/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Get files from a local directory
   */
  static async getLocalFiles(directory: string): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>("files/files_in_local_directory/", {
        params: { directory },
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  // ============================================================================
  // Stateless path helper methods (computed locally, no backend state)
  // ============================================================================

  /**
   * Get the parent directory path (computed locally)
   */
  static getParentPath(currentPath: string): string {
    if (!currentPath) return "";
    const parent = path.dirname(currentPath);
    // Prevent going above root
    if (parent === currentPath || parent === ".") {
      return currentPath;
    }
    return parent;
  }

  /**
   * Join current path with a subdirectory name (computed locally)
   */
  static joinPath(currentPath: string, subdir: string): string {
    if (!currentPath) return subdir;
    return path.join(currentPath, subdir);
  }

  /**
   * Check if a path is a root path
   */
  static isRootPath(pathStr: string): boolean {
    if (!pathStr) return true;
    // Handle Unix root
    if (pathStr === "/" || pathStr === "~") return true;
    // Handle Windows root (e.g., "C:\")
    if (/^[A-Za-z]:[/\\]?$/.test(pathStr)) return true;
    return false;
  }
}

// ============================================================================
// Legacy function exports for backward compatibility
// ============================================================================

export const getCurrentDirectoryContents = FileApi.getCurrentDirectoryContents;
export const getDirectoryContents = FileApi.getDirectoryContents;
export const navigateUp = FileApi.navigateUp;
export const navigateInto = FileApi.navigateInto;
export const navigateTo = FileApi.navigateTo;
export const getCurrentPath = FileApi.getCurrentPath;
export const getDefaultPath = FileApi.getDefaultPath;
export const createDirectory = FileApi.createDirectory;
export const getFileTree = FileApi.getFileTree;
export const getLocalFiles = FileApi.getLocalFiles;
export const getParentPath = FileApi.getParentPath;
export const joinPath = FileApi.joinPath;
export const isRootPath = FileApi.isRootPath;
