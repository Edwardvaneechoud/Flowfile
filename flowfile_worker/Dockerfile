# =============================================================================
# Stage 1: Builder - Install dependencies with build tools
# =============================================================================
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    build-essential \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && rm -rf /var/lib/apt/lists/*

# Add cargo and poetry to PATH
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

# Configure Poetry
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Copy poetry files and install dependencies
COPY pyproject.toml poetry.lock* ./
RUN poetry install --only=main --no-root && rm -rf $POETRY_CACHE_DIR

# =============================================================================
# Stage 2: Runtime - Minimal image with only runtime dependencies
# =============================================================================
FROM python:3.12-slim AS runtime

# Create non-root user for security
RUN groupadd --gid 1000 flowfile \
    && useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home flowfile

WORKDIR /app

# Install only runtime dependencies (if any needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=flowfile:flowfile shared /app/shared
COPY --chown=flowfile:flowfile flowfile_worker/flowfile_worker /app/flowfile_worker

# Create directories for volumes
RUN mkdir -p /app/user_data /app/internal_storage \
    && chown -R flowfile:flowfile /app

# Set environment
ENV PYTHONPATH=/app \
    FLOWFILE_MODE=docker \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 63579

# Health check - verify server is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:63579/docs || exit 1

# Switch to non-root user
USER flowfile

CMD ["python", "-m", "flowfile_worker.main"]
