#!/usr/bin/env python3
"""
Script to generate Python gRPC stubs from proto files.

Usage:
    python generate_stubs.py

This will generate:
    - worker_service_pb2.py (message classes)
    - worker_service_pb2_grpc.py (service stubs and servicers)
"""

import subprocess
import sys
from pathlib import Path


def generate_stubs():
    """Generate Python gRPC stubs from proto files."""
    # Get the directory containing this script
    proto_dir = Path(__file__).parent.resolve()
    proto_file = proto_dir / "worker_service.proto"

    if not proto_file.exists():
        print(f"Error: Proto file not found at {proto_file}")
        sys.exit(1)

    # Generate Python stubs using grpc_tools.protoc
    try:
        from grpc_tools import protoc

        # Generate both message and gRPC service stubs
        result = protoc.main([
            "grpc_tools.protoc",
            f"--proto_path={proto_dir}",
            f"--python_out={proto_dir}",
            f"--grpc_python_out={proto_dir}",
            str(proto_file),
        ])

        if result != 0:
            print(f"Error: protoc returned exit code {result}")
            sys.exit(result)

        # Fix the import in the generated grpc file
        grpc_file = proto_dir / "worker_service_pb2_grpc.py"
        if grpc_file.exists():
            content = grpc_file.read_text()
            # Fix relative import
            content = content.replace(
                "import worker_service_pb2 as worker__service__pb2",
                "from . import worker_service_pb2 as worker__service__pb2",
            )
            grpc_file.write_text(content)

        print("Successfully generated gRPC stubs:")
        print(f"  - {proto_dir / 'worker_service_pb2.py'}")
        print(f"  - {proto_dir / 'worker_service_pb2_grpc.py'}")

    except ImportError:
        print("Error: grpcio-tools not installed. Install it with: pip install grpcio-tools")
        # Fall back to using protoc command line tool
        try:
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "grpc_tools.protoc",
                    f"--proto_path={proto_dir}",
                    f"--python_out={proto_dir}",
                    f"--grpc_python_out={proto_dir}",
                    str(proto_file),
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                print(f"Error: {result.stderr}")
                sys.exit(result.returncode)

            print("Successfully generated gRPC stubs")

        except FileNotFoundError:
            print("Error: protoc not found. Please install grpcio-tools")
            sys.exit(1)


if __name__ == "__main__":
    generate_stubs()
