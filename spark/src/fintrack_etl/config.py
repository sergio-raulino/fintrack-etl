from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field

from dotenv import load_dotenv

# --------------------------------------------------
# Logging (centralizado)
# --------------------------------------------------
from fintrack_etl.logging_conf import setup_logging

# Nível pode vir do env futuramente (DEBUG em dev, INFO em prod)
setup_logging(level=os.getenv("FINTRACK_LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Carregamento explícito dos envs
# cwd esperado: /opt/spark-jobs
# --------------------------------------------------
# 1) GDrive (já existia)
load_dotenv("connections/gdrive.env", override=True)

# 2) MinIO
load_dotenv("connections/minio.env", override=False)

# 3) Catálogos Hive (fintrack)
load_dotenv("connections/hivecatalog_fin.env", override=False)

logger.info("FinTrack config carregado via connections/*.env")

# --------------------------------------------------
# Sub-settings
# --------------------------------------------------


@dataclass(frozen=True)
class GDriveSettings:
    credentials: str = os.getenv("GDRIVE_CREDENTIALS", "")
    token: str = os.getenv("GDRIVE_TOKEN", "")
    folder_id: str = os.getenv("GDRIVE_FOLDER_ID", "")
    file_id: str = os.getenv("GDRIVE_FILE_ID", "")
    out_path: str = os.getenv("GDRIVE_OUT_PATH", "")


@dataclass(frozen=True)
class MinioSettings:
    """
    Configuração de acesso ao MinIO / S3 compatível.
    """

    host_api: str = os.getenv("HOST_API", "localhost:9000")
    access_key: str = os.getenv("ACCESS_KEY", "")
    secret_key: str = os.getenv("SECRET_KEY", "")

    # Opcional – pode ser útil no futuro
    secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    region: str = os.getenv("MINIO_REGION", "us-east-1")


@dataclass(frozen=True)
class HiveFinSettings:
    """
    Configuração dos metastores e buckets das camadas do FinTrack.
    (lnd/raw/trt/rfn/sbx)
    """

    # URIs dos Hive Metastores
    sdl_lnd: str = os.getenv("SDL_LND", "")
    sdl_raw: str = os.getenv("SDL_RAW", "")
    sdl_trt: str = os.getenv("SDL_TRT", "")
    sdl_rfn: str = os.getenv("SDL_RFN", "")
    sdl_sbx: str = os.getenv("SDL_SBX", "")

    # Buckets S3/MinIO
    sdl_lnd_bucket: str = os.getenv("SDL_LND_BUCKET", "")
    sdl_raw_bucket: str = os.getenv("SDL_RAW_BUCKET", "")
    sdl_trt_bucket: str = os.getenv("SDL_TRT_BUCKET", "")
    sdl_rfn_bucket: str = os.getenv("SDL_RFN_BUCKET", "")
    sdl_sbx_bucket: str = os.getenv("SDL_SBX_BUCKET", "")

    # Catálogos (Trino/Iceberg)
    sdl_lnd_catalog: str = os.getenv("SDL_LND_CATALOG", "")
    sdl_raw_catalog: str = os.getenv("SDL_RAW_CATALOG", "")
    sdl_trt_catalog: str = os.getenv("SDL_TRT_CATALOG", "")
    sdl_rfn_catalog: str = os.getenv("SDL_RFN_CATALOG", "")
    sdl_sbx_catalog: str = os.getenv("SDL_SBX_CATALOG", "")


# --------------------------------------------------
# Settings "raiz"
# --------------------------------------------------


@dataclass(frozen=True)
class Settings:
    # Diretório base (pode usar pra montar caminhos locais temporários, etc.)
    data_dir: str = os.getenv("FINTRACK_DATA_DIR", "/data")

    # Blocos de configuração
    gdrive: GDriveSettings = field(default_factory=GDriveSettings)
    minio: MinioSettings = field(default_factory=MinioSettings)
    hive_fin: HiveFinSettings = field(default_factory=HiveFinSettings)


settings = Settings()

# --------------------------------------------------
# Log de sanity-check (sem vazar segredos)
# --------------------------------------------------
logger.info(
    "FinTrack settings OK | "
    "gdrive_creds=%s | gdrive_token=%s | gdrive_out=%s | "
    "minio_host=%s | lnd_bucket=%s | raw_bucket=%s",
    bool(settings.gdrive.credentials),
    bool(settings.gdrive.token),
    settings.gdrive.out_path,
    settings.minio.host_api,
    settings.hive_fin.sdl_lnd_bucket,
    settings.hive_fin.sdl_raw_bucket,
)
