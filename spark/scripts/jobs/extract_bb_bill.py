#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.extractors.bb_bill import run_bb_bill


def main() -> None:
    # --------------------------------------------------
    # Logging básico
    # --------------------------------------------------
    setup_logging(level="INFO")
    logger = logging.getLogger(__name__)

    # --------------------------------------------------
    # CLI
    # --------------------------------------------------
    ap = argparse.ArgumentParser(
        description="Extrai fatura BB (Drive ou local) e grava em Parquet/CSV/JSON."
    )
    ap.add_argument(
        "--source",
        choices=["drive", "local"],
        default="drive",
        help="Origem do PDF: 'drive' (Google Drive) ou 'local' (path no container).",
    )
    ap.add_argument(
        "--local-pdf",
        default=None,
        help="Path do PDF quando --source=local.",
    )
    # Mantemos --out-dir apenas para uso em source=local
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Diretório de saída (usado apenas quando --source=local).",
    )
    ap.add_argument(
        "--also-json",
        action="store_true",
        help="Se informado, grava JSON consolidado da fatura.",
    )
    ap.add_argument(
        "--write-csv",
        action="store_true",
        help="Se informado, também grava CSV (útil para debug/local).",
    )
    args = ap.parse_args()

    # --------------------------------------------------
    # Calcula o out_dir efetivo
    # --------------------------------------------------
    if args.source == "drive":
        # gdrive_out_path é o ARQUIVO PDF (ex: ./data/raw/drive/fatura_drive.pdf)
        pdf_out_path = settings.gdrive_out_path
        # Para saída (parquet/csv/json) usamos o diretório pai
        effective_out_dir = os.path.dirname(pdf_out_path) or "."
        logger.info(
            "source=drive | PDF será salvo em %s | out_dir (parquet/csv/json) = %s",
            pdf_out_path,
            effective_out_dir,
        )
    else:
        # source = local
        if args.out_dir:
            effective_out_dir = args.out_dir
        elif args.local_pdf:
            effective_out_dir = os.path.dirname(args.local_pdf) or "."
        else:
            # fallback razoável
            effective_out_dir = "./data/raw/drive"

        logger.info(
            "source=local | local_pdf=%s | out_dir (parquet/csv/json) = %s",
            args.local_pdf,
            effective_out_dir,
        )

    logger.info(
        "Iniciando run_bb_bill source=%s local_pdf=%s also_json=%s write_csv=%s",
        args.source,
        args.local_pdf,
        args.also_json,
        args.write_csv,
    )

    if args.source == "drive":
        logger.info(
            "Lendo PDF do Google Drive (file_id=%s, out_path=%s)",
            getattr(settings, "gdrive_file_id", None),
            getattr(settings, "gdrive_out_path", None),
        )
    else:
        logger.info("Lendo PDF local em %s", args.local_pdf)

    # --------------------------------------------------
    # Execução principal
    # --------------------------------------------------
    result = run_bb_bill(
        source=args.source,
        local_pdf=args.local_pdf,
        out_dir=effective_out_dir,
        also_json=args.also_json,
        write_csv=args.write_csv,
    )

    logger.info("Extração concluída com sucesso.")
    logger.info("PDF utilizado: %s", result.get("pdf_path"))
    logger.info("Arquivos gerados: %s", result.get("files"))
    logger.info("Contagens: %s", result.get("counts"))

    totals = result.get("totals_by_categoria") or {}
    if totals:
        logger.info("Totais por categoria: %s", totals)

    # Print estruturado (facilita debug / logs do Airflow)
    print("OK ✅")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
