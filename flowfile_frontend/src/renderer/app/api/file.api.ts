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

  static async getDefaultPath(): Promise<string> {
    try {
      const response = await axios.get<string>("files/default_path/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

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

  static getParentPath(currentPath: string): string {
    if (!currentPath) return "";
    const parent = path.dirname(currentPath);
    if (parent === currentPath || parent === ".") {
      return currentPath;
    }
    return parent;
  }

  static joinPath(currentPath: string, subdir: string): string {
    if (!currentPath) return subdir;
    return path.join(currentPath, subdir);
  }

  static isRootPath(pathStr: string): boolean {
    if (!pathStr) return true;
    if (pathStr === "/" || pathStr === "~") return true;
    if (/^[A-Za-z]:[/\\]?$/.test(pathStr)) return true;
    return false;
  }
}

export const getDirectoryContents = FileApi.getDirectoryContents;
export const getDefaultPath = FileApi.getDefaultPath;
export const createDirectory = FileApi.createDirectory;
export const getLocalFiles = FileApi.getLocalFiles;
export const getParentPath = FileApi.getParentPath;
export const joinPath = FileApi.joinPath;
export const isRootPath = FileApi.isRootPath;
