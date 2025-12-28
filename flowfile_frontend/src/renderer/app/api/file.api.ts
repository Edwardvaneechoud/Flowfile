// File API Service - Handles file system operations
// Consolidated from features/designer/components/fileBrowser/fileSystemApi.ts
import axios from '../services/axios-setup'
import type { FileInfo, DirectoryContentsParams } from '../types'

const handleApiError = (error: any): never => {
  throw {
    message: error.response?.data?.detail || 'An unknown error occurred',
    status: error.response?.status || 500,
  }
}

export class FileApi {
  /**
   * Get the list of files in the current directory
   */
  static async getCurrentDirectoryContents(params?: DirectoryContentsParams): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>('files/current_directory_contents/', { params })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Get the contents of a specific directory
   */
  static async getDirectoryContents(
    directory: string,
    params?: DirectoryContentsParams
  ): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>('files/directory_contents/', {
        params: { directory, ...params },
      })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Navigate up one directory level
   */
  static async navigateUp(): Promise<string> {
    try {
      const response = await axios.post<string>('files/navigate_up/')
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Navigate into a subdirectory
   */
  static async navigateInto(directoryName: string): Promise<string> {
    try {
      const response = await axios.post<string>('files/navigate_into/', null, {
        params: { directory_name: directoryName },
      })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Navigate to a specific directory path
   */
  static async navigateTo(directoryPath: string): Promise<string> {
    try {
      const response = await axios.post<string>('files/navigate_to/', null, {
        params: { directory_name: directoryPath },
      })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Get the current directory path
   */
  static async getCurrentPath(): Promise<string> {
    try {
      const response = await axios.get<string>('files/current_path/')
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Create a new directory
   */
  static async createDirectory(directoryName: string): Promise<boolean> {
    try {
      const response = await axios.post<boolean>('files/create_directory', {
        name: directoryName,
      })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Get the full directory tree
   */
  static async getFileTree(): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>('files/tree/')
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }

  /**
   * Get files from a local directory
   */
  static async getLocalFiles(directory: string): Promise<FileInfo[]> {
    try {
      const response = await axios.get<FileInfo[]>('files/files_in_local_directory/', {
        params: { directory },
      })
      return response.data
    } catch (error) {
      return handleApiError(error)
    }
  }
}

// ============================================================================
// Legacy function exports for backward compatibility
// ============================================================================

export const getCurrentDirectoryContents = FileApi.getCurrentDirectoryContents
export const getDirectoryContents = FileApi.getDirectoryContents
export const navigateUp = FileApi.navigateUp
export const navigateInto = FileApi.navigateInto
export const navigateTo = FileApi.navigateTo
export const getCurrentPath = FileApi.getCurrentPath
export const createDirectory = FileApi.createDirectory
export const getFileTree = FileApi.getFileTree
export const getLocalFiles = FileApi.getLocalFiles
