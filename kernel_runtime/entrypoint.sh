#!/bin/bash
set -e

if [ -n "$KERNEL_PACKAGES" ]; then
    echo "Installing packages: $KERNEL_PACKAGES"
    if [ -f "${KERNEL_CONSTRAINTS_FILE:-/opt/constraints.txt}" ]; then
        pip install --no-cache-dir \
            --constraint "${KERNEL_CONSTRAINTS_FILE:-/opt/constraints.txt}" \
            $KERNEL_PACKAGES
    else
        echo "Warning: constraints file not found, installing without version constraints" >&2
        pip install --no-cache-dir $KERNEL_PACKAGES
    fi
fi

exec uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999
