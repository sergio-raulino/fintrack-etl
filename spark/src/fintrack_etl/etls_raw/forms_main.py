from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from fintrack_etl.config import settings
from fintrack_etl.integrations.minio.client import get_minio_client, _parse_s3a_uri
from fintrack_etl.logging_conf import setup_logging

logger = logging.getLogger(__name__)

# =========================================================
# Constantes / Metadados da RAW (Iceberg)
# =========================================================

ICEBERG_CATALOG = settings.hive_fin.sdl_raw_catalog or "sdl_raw_fin"
ICEBERG_SCHEMA = "fintrack_forms"  # schema lógico dos forms (já criado no Hive/Iceberg)


# =========================================================
# Namespace Iceberg (fintrack_forms)
# =========================================================

def ensure_forms_namespace(spark: SparkSession) -> None:
    """
    Garante que o namespace (database) fintrack_forms exista
    no catálogo Iceberg RAW.
    """
    try:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}"
        )
        logger.info(
            "Namespace Iceberg garantido: %s.%s",
            ICEBERG_CATALOG,
            ICEBERG_SCHEMA,
        )
    except AnalysisException as e:
        logger.warning(
            "Falha ao garantir namespace Iceberg (pode já existir): %s",
            e,
        )


# =========================================================
# Spark / Config
# =========================================================

def make_spark_forms(app_name: str | None = None) -> SparkSession:
    """
    Cria uma SparkSession configurada para:
      - acessar MinIO via S3A
      - usar catálogo Iceberg do RAW (sdl_raw_fin)
      - trabalhar com o schema fintrack_forms
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    builder = SparkSession.builder
    if app_name:
        builder = builder.appName(app_name)

    endpoint_host = settings.minio.host_api  # ex.: "minio:9000"
    endpoint_url = f"http://{endpoint_host}"

    access_key = settings.minio.access_key
    secret_key = settings.minio.secret_key

    hive_uri = settings.hive_fin.sdl_raw        # ex.: thrift://hive-metastore-raw-fin:9083
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
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )

    # evita criação de arquivos _SUCCESS
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    logger.info("SparkSession criada. AppName=%s", spark.sparkContext.appName)
    logger.info("Iceberg RAW catalog=%s schema=%s", ICEBERG_CATALOG, ICEBERG_SCHEMA)

    ensure_forms_namespace(spark)
    return spark


# =========================================================
# Helpers MinIO / Landing
# =========================================================

@dataclass
class FormsLandingObject:
    bucket: str
    key: str
    name: str        # nome do arquivo
    form_slug: str   # ex.: "gastos_compartilhados"


def list_forms_csv_from_landing(
    client_slug: str,
    form_slug: str = "gastos_compartilhados",
) -> List[FormsLandingObject]:
    """
    Lista os CSVs do Google Forms na landing do FinTrack, assumindo estrutura:

      s3a://<sdl_lnd_bucket>/
        fintrack/01_clientes/<client_slug>/02_forms/<form_slug>/...

    Exemplo:
      fintrack/01_clientes/cruz_raulino_familia/02_forms/gastos_compartilhados/forms_....csv
    """
    client = get_minio_client()

    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket  # ex.: s3a://sdl-lnd-fin
    bucket, base_prefix = _parse_s3a_uri(lnd_bucket_uri)

    # prefix relativo dentro do bucket MinIO
    relative_prefix = (
        f"fintrack/01_clientes/{client_slug}/02_forms/{form_slug}/"
    )

    if base_prefix:
        full_prefix = f"{base_prefix.rstrip('/')}/{relative_prefix}"
    else:
        full_prefix = relative_prefix

    logger.info(
        "Listando CSVs de forms na landing: bucket=%s prefix=%s (client_slug=%s, form_slug=%s)",
        bucket,
        full_prefix,
        client_slug,
        form_slug,
    )

    objs: List[FormsLandingObject] = []
    for obj in client.list_objects(bucket, prefix=full_prefix, recursive=True):
        if not obj.object_name.lower().endswith(".csv"):
            continue

        name = os.path.basename(obj.object_name)
        objs.append(
            FormsLandingObject(
                bucket=bucket,
                key=obj.object_name,
                name=name,
                form_slug=form_slug,
            )
        )

    logger.info("Total de CSVs de forms encontrados na landing: %d", len(objs))
    return objs

# =========================================================
# Helpers de parsing / normalização
# =========================================================

def _brl_to_float(val) -> float | None:
    """
    Converte strings no padrão brasileiro para float.
      - '237,63' -> 237.63
      - 'R$ 40,00' -> 40.0
      - NaN/None -> None
    """
    if val is None:
        return None

    if pd.isna(val):
        return None

    s = str(val).strip()
    if not s:
        return None

    s = s.replace("R$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_forms_csv_to_pandas(
    local_csv: str,
    client_slug: str,
) -> pd.DataFrame:
    """
    Lê o CSV bruto do Google Forms e normaliza colunas para o layout RAW:

    Mantém:
      - carimbo de data/hora
      - lançamento feito por:
      - data do pagamento
      - vencimento
      - descricao
      - valor
      - tipo_de_custo
      - categoria
      - client_slug
    """
    df = pd.read_csv(local_csv)

    # Mapeamento de colunas do Google Forms -> nomes internos
    col_map = {
        "Carimbo de data/hora": "carimbo de data/hora",
        "LANÇAMENTO FEITO POR:": "lançamento feito por:",
        "DATA DO PAGAMENTO": "data do pagamento",
        "VENCIMENTO\nColocar sempre o mês da prestação de conta": "vencimento",
        "DESCRIÇÃO": "descricao",
        "Valor:\nExemplo: R$40,00": "valor",
        "TIPO DE CUSTO": "tipo_de_custo",
        "CATEGORIA": "categoria",
    }

    # Renomeia somente as colunas que existem no CSV
    cols_present = {c: c for c in df.columns}
    effective_map = {src: dst for src, dst in col_map.items() if src in cols_present}
    df = df.rename(columns=effective_map)

    # Garante colunas mínimas, mesmo que vazias (evita erro ao criar DataFrame Spark)
    for required in [
        "carimbo de data/hora",
        "lançamento feito por:",
        "data do pagamento",
        "vencimento",
        "descricao",
        "valor",
        "tipo_de_custo",
        "categoria",
    ]:
        if required not in df.columns:
            df[required] = None

    # Converte valor (BRL string) para float
    df["valor"] = df["valor"].apply(_brl_to_float)

    # Adiciona client_slug (constante por ingestão)
    df["client_slug"] = client_slug

    return df[
        [
            "carimbo de data/hora",
            "lançamento feito por:",
            "data do pagamento",
            "vencimento",
            "descricao",
            "valor",
            "tipo_de_custo",
            "categoria",
            "client_slug",
        ]
    ]


# =========================================================
# Helpers Iceberg RAW (tabelas + partição)
# =========================================================

def add_ingestion_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de ingestão no padrão Medallion:
      ingestao_timestamp, ingestao_date, ingestao_year, ingestao_month, ingestao_day
    """
    now = datetime.now()
    iso_date = now.date().isoformat()

    df = df.withColumn("ingestao_timestamp", F.current_timestamp())
    df = df.withColumn("ingestao_date", F.lit(iso_date))
    df = df.withColumn("ingestao_year", F.lit(now.year).cast("int"))
    df = df.withColumn("ingestao_month", F.lit(now.month).cast("int"))
    df = df.withColumn("ingestao_day", F.lit(now.day).cast("int"))

    return df


def upsert_iceberg_table_partitioned(df: DataFrame, table_id: str) -> None:
    """
    Cria (se não existir) ou faz append em uma tabela Iceberg
    particionada por ingestao_year/ingestao_month/ingestao_day.
    """
    spark = df.sparkSession

    # Garante colunas de partição presentes
    if not all(
        c in df.columns
        for c in ["ingestao_year", "ingestao_month", "ingestao_day"]
    ):
        df = add_ingestion_columns(df)

    # Verifica se a tabela existe
    tables = spark.sql(f"SHOW TABLES IN {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")
    table_name = table_id.split(".")[-1]
    exists = tables.filter(F.col("tableName") == table_name).count() > 0

    writer = df.writeTo(table_id).tableProperty("format-version", "2")

    if not exists:
        writer = writer.partitionedBy(
            "ingestao_year", "ingestao_month", "ingestao_day"
        )
        writer.create()
    else:
        writer.append()


# =========================================================
# LND -> RAW (Forms CSV) – gastos_compartilhados
# =========================================================

def forms_lnd_to_raw(
    spark: SparkSession,
    client_slug: str = "cruz_raulino_familia",
    form_slug: str = "gastos_compartilhados",
) -> None:
    """
    Lê o(s) CSV(s) do Google Forms na landing (MinIO) e escreve na camada RAW
    em uma tabela Iceberg específica para o form (gastos_compartilhados).

    Para cada arquivo na landing:
      - lê e normaliza o CSV
      - adiciona coluna técnica landing_object_key (path do objeto no MinIO)
      - remove previamente, na RAW, as linhas desse mesmo arquivo
      - faz append dos dados novos

    Isso torna a carga idempotente por arquivo: reprocessar o mesmo CSV não
    gera duplicidade na RAW.
    """
    logger.info(
        "Iniciando LND -> RAW (Forms) | client_slug=%s form_slug=%s",
        client_slug,
        form_slug,
    )

    client = get_minio_client()
    objs = list_forms_csv_from_landing(
        client_slug=client_slug,
        form_slug=form_slug,
    )

    if not objs:
        logger.warning(
            "Nenhum CSV de forms encontrado na landing para form_slug=%s. Nada a fazer.",
            form_slug,
        )
        return

    # Tabela Iceberg alvo (fixa para este form)
    table_id = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.gastos_compartilhados"
    logger.info("Tabela Iceberg alvo (RAW): %s", table_id)

    for obj in objs:
        logger.info(
            "Processando CSV de forms: bucket=%s key=%s",
            obj.bucket,
            obj.key,
        )

        # 1) Baixa o CSV da landing para /tmp
        local_tmp = Path("/tmp") / f"{form_slug}_{os.path.basename(obj.key)}"
        client.fget_object(obj.bucket, obj.key, str(local_tmp))

        # 2) Parsing / normalização em pandas
        df_pd = parse_forms_csv_to_pandas(str(local_tmp), client_slug=client_slug)
        if df_pd is None or df_pd.empty:
            logger.warning("DataFrame vazio para %s. Pulando.", obj.key)
            continue

        # 3) pandas -> Spark
        df_spark: DataFrame = spark.createDataFrame(df_pd)

        # 4) Coluna técnica com o arquivo de origem (path completo no MinIO)
        df_spark = df_spark.withColumn("landing_object_key", F.lit(obj.key))

        # 5) Colunas de ingestão (timestamp, year, month, day)
        df_spark = add_ingestion_columns(df_spark)

        # 6) Remove linhas anteriores desse mesmo arquivo (idempotência)
        landing_key_escaped = obj.key.replace("'", "''")
        try:
            logger.info(
                "Removendo dados antigos na RAW para landing_object_key=%s",
                obj.key,
            )
            spark.sql(
                f"""
                DELETE FROM {table_id}
                WHERE landing_object_key = '{landing_key_escaped}'
                """
            )
        except AnalysisException as e:
            # Se a tabela ainda não existir, apenas loga e segue.
            logger.warning(
                "Falha ao deletar linhas existentes (tabela pode não existir ainda): %s",
                e,
            )

        # 7) Cria (se preciso) ou faz append na tabela Iceberg particionada
        logger.info("Gravando dados do arquivo em tabela Iceberg RAW: %s", table_id)
        upsert_iceberg_table_partitioned(df_spark, table_id)

    logger.info("✅ LND -> RAW (Forms, %s) concluído com sucesso.", form_slug)

