// fileSystemApi.ts
import axios from 'axios';
import { FileInfo } from './types';

interface DirectoryContentsParams {
  file_types?: string[];
  include_hidden?: boolean;
}

const handleApiError = (error: any): never => {
  throw {
    message: error.response?.data?.detail || 'An unknown error occurred',
    status: error.response?.status || 500
  };
};

/**
 * Get the list of files in the current directory
 */
export const getCurrentDirectoryContents = async (params?: DirectoryContentsParams): Promise<FileInfo[]> => {
  try {
    const response = await axios.get<FileInfo[]>('files/current_directory_contents/', { params });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Get the contents of a specific directory
 */
export const getDirectoryContents = async (
  directory: string,
  params?: DirectoryContentsParams
): Promise<FileInfo[]> => {
  try {
    const response = await axios.get<FileInfo[]>('files/directory_contents/', {
      params: { directory, ...params }
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Navigate up one directory level
 */
export const navigateUp = async (): Promise<string> => {
  try {
    const response = await axios.post<string>('files/navigate_up/');
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Navigate into a subdirectory
 */
export const navigateInto = async (directoryName: string): Promise<string> => {
  try {
    const response = await axios.post<string>('files/navigate_into/', null, {
      params: { directory_name: directoryName }
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Navigate to a specific directory path
 */
export const navigateTo = async (directoryPath: string): Promise<string> => {
  try {
    const response = await axios.post<string>('files/navigate_to/', null, {
      params: { directory_name: directoryPath }
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Get the current directory path
 */
export const getCurrentPath = async (): Promise<string> => {
  try {
    const response = await axios.get<string>('files/current_path/');
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Create a new directory
 */
export const createDirectory = async (directoryName: string): Promise<boolean> => {
  try {
    const response = await axios.post<boolean>('files/create_directory', {
      name: directoryName
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Get the full directory tree
 */
export const getFileTree = async (): Promise<FileInfo[]> => {
  try {
    const response = await axios.get<FileInfo[]>('files/tree/');
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};

/**
 * Get files from a local directory
 */
export const getLocalFiles = async (directory: string): Promise<FileInfo[]> => {
  try {
    const response = await axios.get<FileInfo[]>('files/files_in_local_directory/', {
      params: { directory }
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};