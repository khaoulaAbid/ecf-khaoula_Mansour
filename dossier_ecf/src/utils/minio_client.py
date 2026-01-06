# src/utils/minio_client.py

from minio import Minio

def get_minio_client():
    return Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
