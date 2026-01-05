#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from fintrack_etl.etls_raw.forms_main import make_spark_forms, forms_lnd_to_raw
from fintrack_etl.logging_conf import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lê CSVs do Google Forms na landing do FinTrack em "
            "fintrack/01_clientes/<client_slug>/02_forms/<form_slug>/ "
            "e grava em tabela Iceberg na camada RAW (fintrack_forms)."
        )
    )
    parser.add_argument(
        "--client",
        dest="client_slug",
        default="cruz_raulino_familia",
        help="Slug do cliente (ex.: cruz_raulino_familia).",
    )
    parser.add_argument(
        "--form-slug",
        dest="form_slug",
        default="gastos_compartilhados",
        help="Slug do form (ex.: gastos_compartilhados).",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    args = parse_args()

    app_name = f"fintrack_forms_{args.form_slug}_lnd_raw"
    spark = make_spark_forms(app_name=app_name)
    logger.info("AppName efetivo: %s", spark.sparkContext.appName)

    try:
        forms_lnd_to_raw(
            spark,
            client_slug=args.client_slug,
            form_slug=args.form_slug,
        )
    finally:
        spark.stop()
        logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    main()