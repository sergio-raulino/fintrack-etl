from __future__ import annotations

import os
import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from fintrack_etl.config import settings
from fintrack_etl.integrations.minio.client import get_minio_client, _parse_s3a_uri
from fintrack_etl.extractors.bb_bill import (
    extract_text as extract_text_bb_fatura,
    parse_lancamentos as parse_lancamentos_bb_fatura,
)
from fintrack_etl.extractors.bb_statement import extract_bb_statement
from fintrack_etl.extractors.bradesco_bill import extract_bradesco_bill
from fintrack_etl.logging_conf import setup_logging

logger = logging.getLogger(__name__)

# =========================================================
# Constantes / Metadados da RAW (Iceberg)
# =========================================================

# Catálogo Iceberg RAW (ex.: sdl_raw_adm) e schema para as tabelas do FinTrack
ICEBERG_CATALOG = settings.hive_fin.sdl_raw_catalog or "sdl_raw_fin"
ICEBERG_SCHEMA = "fintrack_bank"   # você pode trocar depois se quiser


# =========================================================
# Namespace Iceberg (fintrack_bank)
# =========================================================

def ensure_bank_namespace(spark: SparkSession) -> None:
    """
    Garante que o namespace (database) fintrack_bank exista no catálogo Iceberg.
    Usa o warehouse padrão configurado em settings.hive_fin.sdl_raw_bucket.
    """
    from pyspark.sql.utils import AnalysisException

    try:
        # Spark 3.5 + Iceberg (HiveCatalog) – sintaxe compatível:
        # CREATE NAMESPACE [IF NOT EXISTS] catalog.db
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}"
        )
        logger.info(
            "Namespace Iceberg garantido: %s.%s",
            ICEBERG_CATALOG,
            ICEBERG_SCHEMA,
        )
    except AnalysisException as e:
        # Se já existir ou der algum erro menor, loga e segue
        logger.warning(
            "Falha ao garantir namespace Iceberg (pode já existir): %s", e
        )

# =========================================================
# Spark / Config
# =========================================================

def make_spark(app_name: str | None = None) -> SparkSession:
    """
    Cria uma SparkSession configurada para acessar o MinIO via S3A
    e um catálogo Iceberg no RAW, no estilo DataJust.
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    builder = SparkSession.builder
    if app_name:
        builder = builder.appName(app_name)

    # Endpoint S3A (MinIO) – ex.: minio:9000 -> http://minio:9000
    endpoint_host = settings.minio.host_api  # ex.: "minio:9000"
    endpoint_url = f"http://{endpoint_host}"

    access_key = settings.minio.access_key
    secret_key = settings.minio.secret_key

    hive_uri = settings.hive_fin.sdl_raw  # thrift://hive-metastore-raw-fin:9083
    warehouse_raw = settings.hive_fin.sdl_raw_bucket  # ex.: s3a://sdl-raw-fin

    spark = (
        builder
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Iceberg (Hive catalog RAW)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hive")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", hive_uri)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", warehouse_raw)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    # Menos “lixo” de _SUCCESS
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    logger.info("SparkSession criada. AppName=%s", spark.sparkContext.appName)
    logger.info("Iceberg RAW catalog=%s schema=%s", ICEBERG_CATALOG, ICEBERG_SCHEMA)

    # 🔹 Aqui garantimos que o namespace fintrack_bank existe
    ensure_bank_namespace(spark)

    return spark


# =========================================================
# Helpers MinIO / Landing
# =========================================================

@dataclass
class LandingObject:
    bucket: str
    key: str
    name: str         # basename
    kind: str         # fatura_bb | extrato_bb | fatura_bradesco | desconhecido


def slugify(name: str) -> str:
    import re
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9\-_\.]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def classify_pdf(filename: str) -> str:
    fn = filename.lower()
    if "fatura" in fn and "bradesco" in fn:
        return "fatura_bradesco"
    if "fatura" in fn and "bb" in fn:
        return "fatura_bb"
    if "extrato" in fn and "bb" in fn:
        return "extrato_bb"
    return "desconhecido"


def list_bank_pdfs_from_landing(prefix_suffix: str = "fintrack/movimentacoes_25_11/") -> List[LandingObject]:
    """
    Lista os PDFs de faturas/extratos bancários na landing do FinTrack.

    prefix_suffix: sufixo relativo dentro do bucket landing,
                   ex.: "fintrack/movimentacoes_25_11/"
    """
    client = get_minio_client()

    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket  # ex.: s3a://sdl-lnd-fin
    bucket, base_prefix = _parse_s3a_uri(lnd_bucket_uri)

    # Prefixo completo dentro do bucket
    if base_prefix:
        full_prefix = f"{base_prefix.rstrip('/')}/{prefix_suffix.lstrip('/')}"
    else:
        full_prefix = prefix_suffix.lstrip("/")

    logger.info("Listando objetos de landing: bucket=%s prefix=%s", bucket, full_prefix)

    objs: List[LandingObject] = []
    for obj in client.list_objects(bucket, prefix=full_prefix, recursive=True):
        if not obj.object_name.lower().endswith(".pdf"):
            continue
        name = os.path.basename(obj.object_name)
        kind = classify_pdf(name)
        objs.append(
            LandingObject(
                bucket=bucket,
                key=obj.object_name,
                name=name,
                kind=kind,
            )
        )

    logger.info("Total de PDFs encontrados na landing: %d", len(objs))
    return objs


# =========================================================
# Parsing de PDFs (BB, Bradesco)
# =========================================================

def parse_pdf_to_dfs(local_pdf: str, kind: str) -> Dict[str, pd.DataFrame]:
    """
    Converte o PDF local em um ou mais DataFrames pandas, dependendo do tipo:

      - fatura_bb:        {"lancamentos": df_lanc}
      - extrato_bb:       {"lancamentos": df_lanc}
      - fatura_bradesco:  {"lancamentos": df_lanc}
      - desconhecido:     {}

    Você pode estender para incluir df_resumo/df_header, etc.
    """
    if kind == "fatura_bb":
        full_text = extract_text_bb_fatura(local_pdf)
        df_lanc = parse_lancamentos_bb_fatura(full_text)
        return {"lancamentos": df_lanc}

    if kind == "extrato_bb":
        payload = extract_bb_statement(local_pdf)
        df_lanc = payload["lancamentos"]
        return {"lancamentos": df_lanc}

    if kind == "fatura_bradesco":
        payload = extract_bradesco_bill(local_pdf)
        df_lanc = payload["lancamentos"]
        if df_lanc is None or not isinstance(df_lanc, pd.DataFrame):
            df_lanc = pd.DataFrame([])
        return {"lancamentos": df_lanc}

    # desconhecido
    logger.warning("Tipo de PDF desconhecido, sem parsing: %s", local_pdf)
    return {}


# =========================================================
# Helpers Iceberg RAW (tabelas + partição)
# =========================================================

def table_for_kind(kind: str) -> str:
    """
    Mapeia o tipo do PDF para o nome lógico da tabela RAW (Iceberg).
    """
    # Ex.: sdl_raw_fin.fintrack_bank.fatura_bb_lancamentos
    suffix = "lancamentos"
    return f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{kind}_{suffix}"


def add_ingestion_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de ingestão no padrão Medallion: 
    ingestao_date, ingestao_year, ingestao_month, ingestao_day
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    df = df.withColumn("ingestao_date", F.lit(now.date().isoformat()))
    df = df.withColumn("ingestao_year", F.lit(year).cast("int"))
    df = df.withColumn("ingestao_month", F.lit(month).cast("int"))
    df = df.withColumn("ingestao_day", F.lit(day).cast("int"))

    return df


def upsert_iceberg_table_partitioned(df: DataFrame, table_id: str) -> None:
    """
    Cria (se não existir) ou atualiza uma tabela Iceberg particionada por
    ingestao_year/ingestao_month/ingestao_day, usando a API do Iceberg.
    """
    spark = df.sparkSession

    # Garante colunas de partição presentes
    if not all(c in df.columns for c in ["ingestao_year", "ingestao_month", "ingestao_day"]):
        df = add_ingestion_columns(df)

    # Se a tabela não existe, cria com schema + partição inferidos do DF
    # e Iceberg v2 (padrão moderno)
    tables = spark.sql(f"SHOW TABLES IN {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")
    table_name = table_id.split(".")[-1]
    exists = tables.filter(F.col("tableName") == table_name).count() > 0

    writer = df.writeTo(table_id).tableProperty("format-version", "2")

    if not exists:
        # Cria com partição explícita por (year, month, day)
        writer = writer.partitionedBy(
            "ingestao_year", "ingestao_month", "ingestao_day"
        )
        writer.create()
    else:
        # Append na mesma tabela/partições
        writer.append()


# =========================================================
# LND -> RAW (bank financial PDFs)
# =========================================================

def bank_lnd_to_raw(
    spark: SparkSession,
    landing_prefix_suffix: str = "fintrack/movimentacoes_25_11/",
) -> None:
    """
    Lê os PDFs na landing (MinIO) e grava dataframes de lançamentos
    em tabelas Iceberg na camada RAW, particionadas por
    ingestao_year/ingestao_month/ingestao_day.
    """
    logger.info("Iniciando LND -> RAW (Iceberg) para PDFs bancários.")
    client = get_minio_client()

    objs = list_bank_pdfs_from_landing(prefix_suffix=landing_prefix_suffix)
    if not objs:
        logger.warning("Nenhum PDF de fatura/extrato encontrado na landing. Nada a fazer.")
        return

    tmp_root = Path(tempfile.mkdtemp(prefix="fintrack_bank_lnd_raw_"))
    logger.info("Diretório temporário para PDFs: %s", tmp_root)

    for obj in objs:
        logger.info("Processando PDF: bucket=%s key=%s kind=%s", obj.bucket, obj.key, obj.kind)
        local_pdf = tmp_root / obj.name

        # 1) download do PDF da landing para /tmp
        client.fget_object(obj.bucket, obj.key, str(local_pdf))

        # 2) parsing -> DataFrames pandas
        dfs = parse_pdf_to_dfs(str(local_pdf), obj.kind)
        if not dfs:
            logger.warning("Nenhum DataFrame gerado para %s (%s). Pulando RAW.", obj.name, obj.kind)
            continue

        # No momento só usamos "lancamentos", mas deixo iterável
        for _, df_pd in dfs.items():
            if df_pd is None or df_pd.empty:
                logger.warning("DataFrame vazio para %s. Pulando.", obj.name)
                continue

            # 3) pandas -> Spark DataFrame
            df_spark: DataFrame = spark.createDataFrame(df_pd)

            # 4) adiciona colunas de ingestão (Medallion)
            df_spark = add_ingestion_columns(df_spark)

            # 5) tabela Iceberg específica para o tipo de PDF
            table_id = table_for_kind(obj.kind)
            logger.info("Gravando em tabela Iceberg RAW: %s", table_id)

            upsert_iceberg_table_partitioned(df_spark, table_id)

    logger.info("✅ LND -> RAW (bank PDFs, Iceberg) concluído com sucesso.")
