export function getStorageTypeLabel(storageType: string): string {
  switch (storageType) {
    case "s3":
      return "AWS S3";
    case "adls":
      return "Azure ADLS";
    case "gcs":
      return "Google Cloud Storage";
    default:
      return storageType.toUpperCase();
  }
}

export function getAuthMethodLabel(authMethod: string): string {
  switch (authMethod) {
    case "access_key":
      return "Access Key";
    case "iam_role":
      return "IAM Role";
    case "service_principal":
      return "Service Principal";
    case "managed_identity":
      return "Managed Identity";
    case "sas_token":
      return "SAS Token";
    case "aws-cli":
      return "AWS CLI";
    case "auto":
      return "Auto";
    default:
      return authMethod;
  }
}
