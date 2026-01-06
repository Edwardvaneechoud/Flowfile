FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Poetry
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    build-essential \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && rm -rf /var/lib/apt/lists/*

# Add cargo and poetry to PATH
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

# Configure Poetry: Don't create a virtual environment (we're already in a container)
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Copy poetry files from the root directory (build context)
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
RUN poetry install --only=main --no-root && rm -rf $POETRY_CACHE_DIR

# Install additional package that wasn't in requirements.txt
#RUN pip install --no-cache-dir --only-binary=:all: connectorx==0.4.3a1

# Copy the shared package
COPY shared /app/shared

# Copy the flowfile_worker package contents into the container
COPY flowfile_worker/flowfile_worker /app/flowfile_worker

# Expose the port the app runs on
EXPOSE 63579

# Set Python path to recognize the packages
ENV PYTHONPATH=/app
ENV FLOWFILE_MODE=docker

# Change working directory to where the module is located
WORKDIR /app

# Command to run the application
CMD ["python", "-m", "flowfile_worker.main"]