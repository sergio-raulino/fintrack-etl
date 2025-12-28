from __future__ import annotations

import os
from typing import Tuple, Optional

from minio import Minio

from fintrack_etl.config import settings


def _parse_s3a_uri(uri: str) -> Tuple[str, str]:
    """
    Converte algo como:
      s3a://sdl-lnd-fin
      s3a://sdl-lnd-fin/fintrack/landing
    em (bucket, prefix).

    Se não houver barra após o bucket, prefix = "".
    """
    if not uri:
        raise ValueError("URI S3A vazia para MinIO")

    if uri.startswith("s3a://"):
        uri = uri[len("s3a://") :]

    parts = uri.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def get_minio_client() -> Minio:
    """
    Cria um cliente MinIO usando as configs do settings.
    """
    # Aqui assumo que o config.py expõe algo como:
    # settings.minio.host_api, settings.minio.access_key, settings.minio.secret_key, settings.minio.secure
    host = getattr(settings.minio, "host_api", None) if hasattr(settings, "minio") else None
    access_key = getattr(settings.minio, "access_key", None) if hasattr(settings, "minio") else None
    secret_key = getattr(settings.minio, "secret_key", None) if hasattr(settings, "minio") else None
    secure = getattr(settings.minio, "secure", False) if hasattr(settings, "minio") else False

    if not host or not access_key or not secret_key:
        raise RuntimeError("Configuração MinIO incompleta em settings.minio")

    return Minio(
        endpoint=host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def upload_file_to_minio(
    local_path: str,
    bucket_uri: str,
    object_name: Optional[str] = None,
) -> str:
    """
    Envia um arquivo local para o MinIO.

    - bucket_uri: algo como "s3a://sdl-lnd-fin" ou "s3a://sdl-lnd-fin/fintrack/landing"
    - object_name: chave do objeto relativa ao prefixo do bucket_uri.
                   Se None, usa apenas o basename do arquivo.
    Retorna a URI completa em formato s3a://bucket/key
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Arquivo local não encontrado: {local_path}")

    bucket, prefix = _parse_s3a_uri(bucket_uri)

    if object_name is None:
        object_name = os.path.basename(local_path)

    # Junta prefixo do bucket (se houver) com o object_name
    if prefix:
        key = f"{prefix.rstrip('/')}/{object_name.lstrip('/')}"
    else:
        key = object_name.lstrip("/")

    client = get_minio_client()

    # Garante existência do bucket (idempotente)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.fput_object(bucket, key, local_path)

    return f"s3a://{bucket}/{key}"
