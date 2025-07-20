def start_minio():
    """Start MinIO container for S3 testing"""
    from . import fixtures

    if fixtures.start_minio_container():
        print(f"MinIO started at http://localhost:{fixtures.MINIO_PORT}")
        print(f"Access Key: {fixtures.MINIO_ACCESS_KEY}")
        print(f"Secret Key: {fixtures.MINIO_SECRET_KEY}")
        return 0
    return 1


def stop_minio():
    """Stop MinIO container"""
    from . import fixtures

    if fixtures.stop_minio_container():
        print("MinIO stopped successfully")
        return 0
    return 1