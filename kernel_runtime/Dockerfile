FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    polars>=1.0.0 pyarrow>=14.0.0 numpy>=1.24.0 \
    fastapi>=0.115.0 uvicorn>=0.32.0 httpx>=0.24.0 \
    cloudpickle>=3.0.0 joblib>=1.3.0

COPY kernel_runtime /app/kernel_runtime
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV KERNEL_PACKAGES=""
# Note: /shared is mounted at runtime via docker.types.Mount — do NOT use
# a VOLUME directive here as it can create an anonymous volume that shadows
# the explicit named-volume mount in Docker-in-Docker setups.
EXPOSE 9999

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s \
    CMD curl -f http://localhost:9999/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
