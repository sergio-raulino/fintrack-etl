from __future__ import annotations

import os
import logging

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.integrations.gdrive.client import download_file
from fintrack_etl.integrations.minio.client import upload_file_to_minio


def main():
    # Logging básico
    setup_logging()
    logger = logging.getLogger(__name__)

    # -----------------------------
    # 1) Baixar o PDF do GDrive
    # -----------------------------
    logger.info("Iniciando download da fatura via Google Drive...")

    # Se você ainda estiver com o config antigo (flat), troque para:
    # file_id=settings.gdrive_file_id, out_path=settings.gdrive_out_path, etc.
    path = download_file(
        file_id=settings.gdrive.file_id,
        out_path=settings.gdrive.out_path,
        credentials_path=settings.gdrive.credentials,
        token_path=settings.gdrive.token,
    )

    logger.info("Arquivo baixado localmente em: %s", path)

    # -----------------------------
    # 2) Enviar PDF para MinIO (landing)
    # -----------------------------
    # Bucket da camada landing (ex: s3a://sdl-lnd-fin)
    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket

    # Prefixo "lógico" para faturas do GDrive
    base_name = os.path.basename(path)
    object_name = f"fintrack/gdrive_faturas/{base_name}"

    logger.info(
        "Enviando para MinIO | bucket_uri=%s | object_name=%s",
        lnd_bucket_uri,
        object_name,
    )

    s3a_uri = upload_file_to_minio(
        local_path=path,
        bucket_uri=lnd_bucket_uri,
        object_name=object_name,
    )

    logger.info("Upload concluído. Objeto disponível em: %s", s3a_uri)

    print("Arquivo baixado em:", path)
    print("Arquivo enviado para MinIO em:", s3a_uri)


if __name__ == "__main__":
    main()

