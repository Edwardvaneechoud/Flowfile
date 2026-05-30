// Kernel management related TypeScript interfaces and types

export type KernelState = "stopped" | "starting" | "idle" | "executing" | "error";

export type ImageFlavour = "base" | "ml" | "lite" | "custom";

export interface KernelFlavourMeta {
  value: ImageFlavour;
  label: string;
  description: string;
}

export const KERNEL_FLAVOURS: KernelFlavourMeta[] = [
  {
    value: "base",
    label: "Base",
    description: "Polars, PyArrow, NumPy. Best for plain data work.",
  },
  {
    value: "ml",
    label: "ML",
    description: "Base + scikit-learn, XGBoost, LightGBM, statsmodels.",
  },
  {
    value: "lite",
    label: "Lite",
    description:
      "Same image as Base, but only Polars and the kernel-runtime libs are pinned — " +
      "numpy, pyarrow and other transitives float. Best for installing large libraries " +
      "(e.g. flowfile) whose own dep trees need room to resolve. " +
      "flowfile_ctx is always available, regardless of flavour.",
  },
  {
    value: "custom",
    label: "Custom image",
    description: "Use your own published Docker image URI.",
  },
];

export interface FlavourPackage {
  name: string;
  version: string;
}

export interface FlavourInfo {
  flavour: ImageFlavour;
  image: string | null;
  packages: FlavourPackage[];
}

export interface KernelConfig {
  id: string;
  name: string;
  packages: string[];
  cpu_cores: number;
  memory_gb: number;
  gpu: boolean;
  image_flavour: ImageFlavour;
  custom_image: string | null;
}

export interface KernelImageStatus {
  flavour: ImageFlavour;
  image: string;
  available: boolean;
  // Tag actually picked by the resolver when ``image`` itself isn't present
  // but a locally-built variant was found. Null = the registry default is in
  // use (or nothing is). Drives the "Found locally" info banner.
  resolved_image: string | null;
  // "pulling" while a background install is running, "error:<msg>" if the
  // last attempt failed, null otherwise.
  pull_state: string | null;
}

export interface DockerStatus {
  available: boolean;
  image_available: boolean;
  images: KernelImageStatus[];
  error: string | null;
}

export interface ResolvedPackage {
  name: string;
  version: string;
}

export interface KernelInfo {
  id: string;
  name: string;
  state: KernelState;
  container_id: string | null;
  port: number | null;
  packages: string[];
  resolved_packages: ResolvedPackage[];
  memory_gb: number;
  cpu_cores: number;
  gpu: boolean;
  image_flavour: ImageFlavour;
  custom_image: string | null;
  image: string | null;
  created_at: string;
  error_message: string | null;
  kernel_version: string | null;
}

export interface DisplayOutput {
  mime_type: string;
  data: string;
  title: string;
}

export interface ExecuteResult {
  success: boolean;
  output_paths: string[];
  artifacts_published: string[];
  artifacts_deleted: string[];
  display_outputs: DisplayOutput[];
  stdout: string;
  stderr: string;
  error: string | null;
  execution_time_ms: number;
}

export interface ExecuteCellRequest {
  node_id: number;
  code: string;
  flow_id: number;
}

export interface KernelMemoryInfo {
  used_bytes: number;
  limit_bytes: number;
  usage_percent: number;
}

/** A single introspected symbol from the kernel API, used for editor type hints. */
export interface ApiSymbol {
  name: string;
  kind: string; // "function" | "class" | "property" | "variable"
  namespace: string; // "flowfile_ctx" | "pl" | "LazyFrame" | "Expr" | ...
  signature: string;
  return_type: string;
  doc: string;
}
