# Flowfile Kubernetes Deployment

Deploy Flowfile (frontend, core, and worker) on a Kubernetes cluster.

## Architecture

```
                ┌──────────┐
                │ Ingress  │
                └────┬─────┘
                     │
          ┌──────────┴──────────┐
          │                     │
  ┌───────▼───────┐    ┌───────▼───────┐
  │   Frontend    │    │     Core      │
  │  (nginx:8080) │    │ (FastAPI:63578│
  └───────────────┘    └───────┬───────┘
                               │
                       ┌───────▼───────┐
                       │    Worker     │
                       │(FastAPI:63579)│
                       └───────────────┘

  Core and Worker share:
    - /app/internal_storage (PVC)
    - /app/user_data (PVC)
```

## Prerequisites

- Kubernetes cluster (v1.24+)
- `kubectl` configured for your cluster
- Container images pushed to a registry accessible by the cluster
- A **ReadWriteMany** StorageClass for shared volumes (e.g. NFS, EFS, CephFS, Longhorn)

## Quick Start

### 1. Build and push container images

From the repository root, build and push the three images to your container registry:

```bash
# Set your registry prefix
export REGISTRY=your-registry.example.com/flowfile

docker build -f flowfile_core/Dockerfile -t $REGISTRY/flowfile-core:latest .
docker build -f flowfile_worker/Dockerfile -t $REGISTRY/flowfile-worker:latest .
docker build -f flowfile_frontend/Dockerfile -t $REGISTRY/flowfile-frontend:latest .

docker push $REGISTRY/flowfile-core:latest
docker push $REGISTRY/flowfile-worker:latest
docker push $REGISTRY/flowfile-frontend:latest
```

### 2. Configure secrets

Edit `k8s/secrets.yaml` with production-safe values **before** deploying:

```bash
# Generate a JWT secret
openssl rand -hex 32

# Generate a Fernet master key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Update the `stringData` fields in `secrets.yaml` with these generated values and a strong admin password.

### 3. Configure storage

Edit `k8s/storage.yaml` and uncomment `storageClassName` lines to match your cluster's ReadWriteMany-capable StorageClass:

```yaml
storageClassName: nfs-client   # or efs-sc, cephfs, longhorn, etc.
```

### 4. Update image references

If using a private registry, update the `image:` fields in each deployment manifest:

- `k8s/flowfile-core.yaml` — `image: your-registry/flowfile-core:latest`
- `k8s/flowfile-worker.yaml` — `image: your-registry/flowfile-worker:latest`
- `k8s/flowfile-frontend.yaml` — `image: your-registry/flowfile-frontend:latest`

### 5. Deploy with Kustomize

```bash
kubectl apply -k k8s/
```

Or apply manifests individually:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/storage.yaml
kubectl apply -f k8s/flowfile-core.yaml
kubectl apply -f k8s/flowfile-worker.yaml
kubectl apply -f k8s/flowfile-frontend.yaml
```

### 6. Verify the deployment

```bash
kubectl -n flowfile get pods
kubectl -n flowfile get svc
```

Wait for all pods to reach `Running` status with `READY 1/1`.

### 7. Access the UI

**Port-forward** (quick local access):

```bash
kubectl -n flowfile port-forward svc/flowfile-frontend 8080:8080
# Open http://localhost:8080
```

**Ingress** (production access):

1. Edit `k8s/ingress.yaml` — set `host` to your domain and configure your ingress class
2. Uncomment `- ingress.yaml` in `k8s/kustomization.yaml`
3. Re-apply: `kubectl apply -k k8s/`

## Configuration Reference

### Environment Variables

| Variable | Service | Description |
|---|---|---|
| `FLOWFILE_MODE` | core, worker | Runtime mode (set to `docker`) |
| `FLOWFILE_ADMIN_USER` | core | Admin username |
| `FLOWFILE_ADMIN_PASSWORD` | core | Admin password |
| `JWT_SECRET_KEY` | core | JWT signing key (min 32 chars) |
| `FLOWFILE_MASTER_KEY` | core, worker | Fernet key for secret encryption |
| `WORKER_HOST` | core | Hostname of the worker service |
| `CORE_HOST` | worker | Hostname of the core service |
| `CORE_PORT` | worker | Port of the core service |
| `FLOWFILE_STORAGE_DIR` | core, worker | Path to internal storage |
| `FLOWFILE_USER_DATA_DIR` | core, worker | Path to user data |

### Volumes

| PVC | Mount Path | Access Mode | Used By |
|---|---|---|---|
| `flowfile-internal-storage` | `/app/internal_storage` | ReadWriteMany | core, worker |
| `flowfile-user-data` | `/app/user_data` | ReadWriteMany | core, worker |
| `flowfile-saved-flows` | `/app/flowfile_core/saved_flows` | ReadWriteOnce | core |

Both core and worker also mount an `emptyDir` with `medium: Memory` at `/dev/shm` (2Gi) for shared memory used by Polars and multiprocessing.

### Ports

| Service | Port | Description |
|---|---|---|
| `flowfile-frontend` | 8080 | Web UI (nginx) |
| `flowfile-core` | 63578 | Core API (FastAPI) |
| `flowfile-worker` | 63579 | Worker API (FastAPI) |

## Scaling Considerations

- **Frontend**: Stateless — can scale replicas freely.
- **Core**: Manages flow state in the filesystem. Scaling beyond 1 replica requires a shared database or external state store (not yet supported).
- **Worker**: Computation-heavy. Scaling beyond 1 replica is possible if your shared storage supports concurrent writes. Each worker pod needs sufficient CPU and memory for data processing.

## Shared Storage Alternatives

If your cluster does not support ReadWriteMany volumes:

1. **Single-node deployment**: Change `accessModes` to `ReadWriteOnce` in `storage.yaml` and ensure core + worker pods schedule on the same node (use `nodeAffinity` or `podAffinity`).
2. **NFS server**: Deploy an in-cluster NFS server (e.g. `nfs-ganesha`) and use an NFS StorageClass.
3. **Cloud-managed**: Use AWS EFS, GCP Filestore, or Azure Files for managed ReadWriteMany support.

## Troubleshooting

```bash
# Check pod status
kubectl -n flowfile get pods

# View logs
kubectl -n flowfile logs deployment/flowfile-core
kubectl -n flowfile logs deployment/flowfile-worker
kubectl -n flowfile logs deployment/flowfile-frontend

# Describe a pod for events and errors
kubectl -n flowfile describe pod <pod-name>

# Check if PVCs are bound
kubectl -n flowfile get pvc
```
