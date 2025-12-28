from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, List

import yaml
from pyspark.sql import SparkSession

from spark.src.fintrack_etl.datatrack_utils.scripts.datatrack_spark_utils import DataTrackSparkUtils
from datatrack_utils.scripts.logger_utils import LoggerUtils

# 🔁 novo client SharePoint
from tjce_lnd_adm.extract_sp_gestaoadmti.sp_list_client import fetch_all

# writer reutilizado
from tjce_lnd_adm.extract_sp_gestaoadmti.json_writer import salvar_jsonl_batch

# config específico das listas SharePoint
from tjce_lnd_adm.extract_sp_gestaoadmti.config import (  
    caminho_base,
    minio_parameters,
    lists_config,
)

# --------------------------- Logging ---------------------------
NOME_SISTEMA_CATALOGO = "sharepoint_gestaoadmti"
log_filename = f"{NOME_SISTEMA_CATALOGO}_ingestion_log"

logger = LoggerUtils(
    nome_logger=log_filename,
    pasta_logs="logs/tjce_lnd_adm",
    backup_days=10,
).get_logger()

dsu = DataTrackSparkUtils(logger)


# ---------------- YAML de entidades (listas) ----------------
def carregar_listas(yaml_path: str) -> List[str]:
    """
    Espera um YAML assim:

    entidades:
      - orcamento_financas
      - contratos
    """
    path = Path(yaml_path)
    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("entidades", [])


# ---------------- Helpers de paginação/batching locais ----------------
def _chunks(seq: List[dict], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _as_pages(obj) -> Iterable[List[dict]]:
    """
    Tenta tratar 'obj' como:

      1) um iterável que já yielde páginas (listas de dicts), OU
      2) uma lista gigante de registros (que será chunkada).

    Aqui o SharePoint via sp_list_client.fetch_all retorna uma lista
    direta de registros (fields), então normalmente vamos cair no caso 2.
    """
    # Caso 1: já vem paginado (iterável de páginas)
    if hasattr(obj, "__iter__") and not isinstance(obj, (list, dict, str, bytes)):
        for page in obj:
            if not page:
                continue
            if isinstance(page, dict):
                yield [page]
            else:
                yield list(page)
        return

    # Caso 2: lista única
    if obj is None:
        return
    if isinstance(obj, dict):
        obj = [obj]
    if isinstance(obj, list):
        BATCH_SIZE = int((Path.cwd() / ".batch_size").read_text().strip()) \
            if (Path.cwd() / ".batch_size").exists() else int(20000)
        for batch in _chunks(obj, BATCH_SIZE):
            yield batch


# ---------------- Spark ----------------
def make_spark(app_name: str, minio) -> SparkSession:
    builder = SparkSession.builder
    if app_name:
        builder = builder.appName(app_name)

    spark = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", minio["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


# ---------------- Main ----------------
if __name__ == "__main__":

    # YAML específico para listas SharePoint
    entidades_a_ingerir = carregar_listas("sources/entidades_sp_gestaoadmti.yaml")
    if not entidades_a_ingerir:
        logger.info("⚠️ Nenhuma lista definida no arquivo entidades_sp_gestaoadmti.yaml")
        sys.exit(1)

    # Validação simples: verifica se a chave existe no lists_config
    for ent in entidades_a_ingerir:
        if ent not in lists_config:
            logger.error(
                f"❌ Lista inválida no YAML: '{ent}'. "
                f"Listas válidas com config: {list(lists_config.keys())}"
            )
            sys.exit(1)

    spark = make_spark(app_name=NOME_SISTEMA_CATALOGO, minio=minio_parameters)
    logger.info(f"AppName efetivo: {spark.sparkContext.appName}")

    try:
        for entidade in entidades_a_ingerir:
            # entidade aqui será "orcamento_financas" ou "contratos"
            logger.info(f"\n🚀 Iniciando ingestão da lista SharePoint: {entidade}")

            dt_ingestao_dados = datetime.now()

            # Coleta via SharePoint / Microsoft Graph
            # sp_list_client.resolve a lista pelo displayName configurado
            registros = fetch_all(entidade)

            total_registros = 0
            page_idx = 0
            for page in _as_pages(registros) or []:
                page_idx += 1
                total_registros += len(page)

                # nome_tabela = entidade → cria as duas pastas:
                # <base>/orcamento_financas/ingestao_*/...
                # <base>/contratos/ingestao_*/...
                salvar_jsonl_batch(
                    spark=spark,
                    nome_tabela=entidade,
                    registros=page,
                    caminho_base=caminho_base,
                    dt_ingestao_dados=dt_ingestao_dados,
                )

                logger.info(f"Página/batch {page_idx} gravada: {len(page)} registros")

            logger.info(
                f"🏁 Lista '{entidade}' finalizada. Registros totais: {total_registros}"
            )

    finally:
        spark.stop()
