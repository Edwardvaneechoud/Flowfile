# Build stage
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    build-essential \
    binutils \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Optimize Rust builds - strip symbols and enable LTO
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=thin \
    RUSTFLAGS="-C strip=symbols"

COPY pyproject.toml poetry.lock* ./

# Install dependencies AND strip in the same layer (in builder)
RUN poetry install --only=main --no-root && \
    rm -rf $POETRY_CACHE_DIR && \
    # Strip debug symbols from compiled libraries
    find /usr/local/lib/python3.12/site-packages -name "*.so" -exec strip --strip-debug {} \; 2>/dev/null || true && \
    # Remove unnecessary files
    find /usr/local/lib/python3.12/site-packages -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -type d -name "test" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.12/site-packages -name "*.pyc" -delete 2>/dev/null || true && \
    # Remove pip cache and other unnecessary files
    rm -rf /root/.cache /root/.cargo/registry /root/.cargo/git

# Runtime stage
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy already-stripped site-packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY shared /app/shared
COPY tools /app/tools
COPY flowfile_core/flowfile_core /app/flowfile_core

RUN mkdir -p /app/user_data /app/internal_storage /app/flowfile_core/saved_flows

ENV PYTHONPATH=/app \
    FLOWFILE_MODE=docker \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 63578

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:63578/docs || exit 1

CMD ["python", "-m", "flowfile_core.main"]