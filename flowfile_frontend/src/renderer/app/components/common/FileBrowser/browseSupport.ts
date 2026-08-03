// Client-side mirror of shared/cloud_storage/browse.py::browse_support, so Browse can
// be disabled with an explanation before a round trip. The backend stays the enforcer
// and returns BROWSE_UNSUPPORTED_AUTH regardless of what this says.
//
// Deliberately not a field on FullCloudStorageConnectionInterface: that type is filled
// by a hand-written mapper which silently drops anything it does not enumerate.

const BROWSABLE_STORAGE_TYPES = ["s3", "adls", "gcs"];

const ADLS_UNSUPPORTED_AUTH: Record<string, string> = {
  service_principal:
    "Browsing Azure storage with service-principal auth is not supported. Use an account key or SAS token, or enter the path manually.",
  managed_identity:
    "Browsing Azure storage with managed-identity auth is not supported. Use an account key or SAS token, or enter the path manually.",
};

/** Why this connection cannot be browsed, or null when it can. */
export function browseUnsupportedReason(
  storageType: string | null | undefined,
  authMethod: string | null | undefined,
): string | null {
  if (!storageType) return null;
  if (!BROWSABLE_STORAGE_TYPES.includes(storageType)) {
    return `Browsing is not supported for ${storageType} storage.`;
  }
  if (storageType === "adls" && authMethod) {
    return ADLS_UNSUPPORTED_AUTH[authMethod] ?? null;
  }
  return null;
}

export function canBrowseConnection(
  storageType: string | null | undefined,
  authMethod: string | null | undefined,
): boolean {
  return browseUnsupportedReason(storageType, authMethod) === null;
}
