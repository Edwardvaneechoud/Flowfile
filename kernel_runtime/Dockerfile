# syntax=docker/dockerfile:1.6
# Build stage
FROM python:3.12-slim AS builder

WORKDIR /build

ARG EXTRAS=""
# SLIM_CONSTRAINTS=true filters /opt/constraints.txt down to just the packages
# the kernel runtime directly imports (fastapi, uvicorn, httpx, cloudpickle,
# polars + its version-coupled runtime, joblib for sklearn/numpy artifact
# serialisation). pyarrow and numpy are deliberately left out: they come in as
# polars transitives, and pinning them blocks users from installing packages
# (e.g. flowfile) whose own dep trees need different versions. Used to build
# the "lite" flavour. Keep this whitelist in sync with _LITE_PACKAGE_NAMES in
# flowfile_core/flowfile_core/kernel/flavours.py.
ARG SLIM_CONSTRAINTS="false"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    build-essential \
    binutils \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.8.4 python3 - \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Optimize Rust builds - strip symbols and enable LTO
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=thin \
    RUSTFLAGS="-C strip=symbols"

COPY pyproject.toml poetry.lock README.md ./

# Install dependencies (with optional extras), export constraints for KERNEL_PACKAGES,
# strip and clean in one layer
RUN poetry install --only=main --no-root ${EXTRAS:+--extras "$EXTRAS"} && \
    poetry export --without-hashes --only=main \
        ${EXTRAS:+--extras "$EXTRAS"} \
        -o /opt/constraints.txt && \
    if [ "$SLIM_CONSTRAINTS" = "true" ]; then \
        grep -E '^(polars|polars-runtime-32|fastapi|uvicorn|httpx|cloudpickle|joblib)==' \
            /opt/constraints.txt > /opt/constraints.slim.txt && \
        mv /opt/constraints.slim.txt /opt/constraints.txt ; \
    fi && \
    rm -rf $POETRY_CACHE_DIR && \
    find /usr/local/lib/python3.12/site-packages -name "*.so" -exec strip --strip-debug {} \; 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -type d -name "test" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -name "*.pyc" -delete 2>/dev/null || true && \
    rm -rf /root/.cache /root/.cargo/registry /root/.cargo/git

# Runtime stage
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy already-stripped site-packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /opt/constraints.txt /opt/constraints.txt

COPY kernel_runtime /app/kernel_runtime
COPY entrypoint.sh /app/entrypoint.sh
# Strip CRs so images built from a Windows checkout (CRLF) still exec
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KERNEL_PACKAGES="" \
    KERNEL_CONSTRAINTS_FILE=/opt/constraints.txt

# Note: /shared is mounted at runtime via docker.types.Mount — do NOT use
# a VOLUME directive here as it can create an anonymous volume that shadows
# the explicit named-volume mount in Docker-in-Docker setups.
EXPOSE 9999

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s \
    CMD curl -f http://localhost:9999/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
