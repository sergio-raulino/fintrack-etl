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
            "Lê PDFs bancários na landing do FinTrack (MinIO) e grava "
            "em tabelas Iceberg na camada RAW, particionadas por ingestao_year/month/day."
        )
    )
    ap.add_argument(
        "--landing-prefix",
        default="fintrack/movimentacoes_25_11/",
        help="Prefixo relativo dentro do bucket landing (ex.: fintrack/movimentacoes_25_11/).",
    )
    args = ap.parse_args()

    spark = make_spark(app_name="fintrack_bank_lnd_raw_finantial_files_load")
    logger.info("AppName efetivo: %s", spark.sparkContext.appName)

    try:
        bank_lnd_to_raw(
            spark,
            landing_prefix_suffix=args.landing_prefix,
        )
    finally:
        spark.stop()
        logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    main()
