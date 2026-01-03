#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from fintrack_etl.etls_raw.main import make_spark, bank_lnd_to_raw
from fintrack_etl.logging_conf import setup_logging


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    ap = argparse.ArgumentParser(
        description=(
            "Lê PDFs bancários na landing do FinTrack (MinIO) em "
            "fintrack/01_clientes/<client>/01_bancos/<bank>/<doc_type>/<ano>/<mes>/ "
            "e grava em tabelas Iceberg na camada RAW, particionadas por "
            "ingestao_year/month/day."
        )
    )
    ap.add_argument(
        "--client",
        default="cruz_raulino_familia",
        help="Slug do cliente (ex.: cruz_raulino_familia).",
    )
    ap.add_argument(
        "--bank",
        default=None,
        help="Código do banco (ex.: bb, bradesco). Se omitido, processa todos os bancos.",
    )
    ap.add_argument(
        "--doc-type",
        default=None,
        help="Tipo de documento (ex.: extratos, faturas). Se omitido, processa ambos.",
    )
    ap.add_argument(
        "--year",
        type=int,
        default=None,
        help="Ano específico (YYYY). Se omitido, processa todos os anos disponíveis.",
    )
    ap.add_argument(
        "--month",
        type=int,
        default=None,
        help="Mês específico (1-12). Se omitido, processa todos os meses disponíveis.",
    )
    args = ap.parse_args()

    spark = make_spark(app_name="fintrack_bank_lnd_raw_finantial_files_load")
    logger.info("AppName efetivo: %s", spark.sparkContext.appName)

    try:
        bank_lnd_to_raw(
            spark,
            client_slug=args.client,
            bank_code=args.bank,
            doc_type=args.doc_type,
            year=args.year,
            month=args.month,
        )
    finally:
        spark.stop()
        logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    main()
