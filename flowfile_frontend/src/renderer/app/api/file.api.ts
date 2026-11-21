import axios from "../services/axios.config";
import type { FileInfo, DirectoryContentsParams } from "../types";
import {
  isRootPath as isStorageRootPath,
  joinPath as joinStoragePath,
  parentPath as storageParentPath,
} from "../utils/storagePath";

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

  /**
   * Returns the managed flows directory used for catalog-tab saves.
   * Resolves to ``~/.flowfile/flows`` on local or ``/data/user/flows`` in Docker.
   */
  static async getCatalogFlowsDirectory(): Promise<string> {
    try {
      const response = await axios.get<string>("files/catalog_flows_directory/");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /** Creates the folder `dirName` inside `sourcePath`; a taken name rejects with status 409. */
  static async createDirectory(sourcePath: string, dirName: string): Promise<boolean> {
    try {
      const response = await axios.post<boolean>("files/create_directory", {
        source_path: sourcePath,
        dir_name: dirName,
      });
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /** The file browser's top folders on a server (the person's Workspace, then Shared); empty on a desktop. */
  static async getBrowseRoots(): Promise<{ name: string; path: string }[]> {
    try {
      const response = await axios.get<{ name: string; path: string }[]>("files/roots");
      return response.data;
    } catch (error) {
      return handleApiError(error);
    }
  }

  /** A file of a folder the browser shows, saved through the browser's own download. */
  static async downloadFile(path: string, fileName: string): Promise<void> {
    try {
      const response = await axios.get<Blob>("files/download", {
        params: { path },
        responseType: "blob",
      });
      const url = URL.createObjectURL(response.data);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = fileName;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      return handleApiError(error);
    }
  }

  /**
   * Deletes a file, or a folder with what is in it, on a server; `items` is how many files and folders went
   * with it. With `dryRun` they are only counted and nothing is deleted.
   */
  static async deletePath(
    path: string,
    opts: { dryRun?: boolean } = {},
  ): Promise<{ path: string; items: number }> {
    try {
      const response = await axios.delete<{ path: string; items: number }>("files/delete", {
        params: { path, dry_run: opts.dryRun ? true : undefined },
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
    return currentPath ? storageParentPath(currentPath) : "";
  }

  static joinPath(currentPath: string, subdir: string): string {
    return joinStoragePath(currentPath, subdir);
  }

  static isRootPath(pathStr: string): boolean {
    return isStorageRootPath(pathStr);
  }
}

export const getDirectoryContents = FileApi.getDirectoryContents;
export const getDefaultPath = FileApi.getDefaultPath;
export const getCatalogFlowsDirectory = FileApi.getCatalogFlowsDirectory;
export const createDirectory = FileApi.createDirectory;
export const getLocalFiles = FileApi.getLocalFiles;
export const getParentPath = FileApi.getParentPath;
export const joinPath = FileApi.joinPath;
export const isRootPath = FileApi.isRootPath;
