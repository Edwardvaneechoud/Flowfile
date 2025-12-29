// File system related TypeScript interfaces and types
// Consolidated from features/designer/components/fileBrowser/types.ts

// ============================================================================
// File Info Types
// ============================================================================

export interface FileInfo {
  name: string
  path: string
  is_directory: boolean
  size: number
  file_type: string
  last_modified: Date
  created_date: Date
  is_hidden: boolean
  exists?: boolean
}

// ============================================================================
// Directory Contents Params
// ============================================================================

export interface DirectoryContentsParams {
  file_types?: string[]
  include_hidden?: boolean
}
