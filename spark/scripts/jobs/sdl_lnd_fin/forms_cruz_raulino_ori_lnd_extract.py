#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, date

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.integrations.gdrive.client import download_file
from fintrack_etl.integrations.minio.client import upload_file_to_minio

# raiz do projeto dentro do contêiner (/opt/spark-jobs)
ROOT_DIR = Path(__file__).resolve().parents[3]  # ajusta se necessário


# =========================================================
# Utils de filesystem / estado (pode reutilizar de outro módulo se quiser)
# =========================================================
def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}

    try:
        raw = state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        bad_path = state_path.with_suffix(state_path.suffix + ".bad")
        try:
            state_path.replace(bad_path)
        except Exception:
            pass
        return {}


def save_state(state_path: Path, state: dict) -> None:
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(state_path)


# =========================================================
# Main
# =========================================================
def main() -> None:
    """
    Baixa a planilha de respostas do Forms de gastos compartilhados
    (FORM_RESPONSES_SHEET_ID) como CSV e envia para o MinIO (landing),
    preservando uma estrutura consistente com o restante do FinTrack.
    """
    ap = argparse.ArgumentParser(
        description=(
            "Baixa a planilha de respostas do Forms de gastos compartilhados "
            "da família cruz_raulino_familia no GDrive e envia para MinIO (landing)."
        )
    )
    ap.add_argument(
        "--client",
        default="cruz_raulino_familia",
        help="Slug do cliente (apenas para compor o path no MinIO).",
    )
    ap.add_argument(
        "--local-dir",
        default="./data/raw/drive",
        help="Diretório local base para armazenar temporariamente o CSV.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Força re-download/upload mesmo se já houver um CSV para hoje.",
    )
    args = ap.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    local_base = Path(args.local_dir)
    safe_mkdir(local_base)

    # ------------------------------------------------------------------
    # 1) Lê o ID da planilha de respostas do Forms do .env (connections/gdrive.env)
    # ------------------------------------------------------------------
    sheet_id = os.getenv("FORM_RESPONSES_SHEET_ID")
    if not sheet_id:
        raise RuntimeError(
            "Variável de ambiente FORM_RESPONSES_SHEET_ID não encontrada.\n"
            "Certifique-se de que ela está definida em connections/gdrive.env "
            "e que o Settings está carregando esse .env."
        )

    # Bucket da camada landing (ex: s3a://sdl-lnd-fin)
    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket

    # Estado simples: último upload bem-sucedido por data
    state_path = local_base / f"_state_forms_{args.client}.json"
    state = load_state(state_path)

    today = date.today()
    today_str = today.isoformat()  # YYYY-MM-DD

    if (not args.force) and state.get("last_upload_date") == today_str:
        logger.info(
            "⏭️  Já houve upload de Forms para o cliente %s hoje (%s). Use --force para reenviar.",
            args.client,
            today_str,
        )
        print(
            f"⏭️  Já houve upload de Forms para {args.client} hoje ({today_str}). "
            f"Use --force para reenviar."
        )
        return

    # ------------------------------------------------------------------
    # 2) Define caminho local do CSV
    # ------------------------------------------------------------------
    # Ex.: ./data/raw/drive/02_forms/cruz_raulino_familia/forms_gastos_compartilhados_20260103.csv
    relative_path = (
        Path("02_forms")
        / args.client
    )
    local_dir_for_file = local_base / relative_path
    safe_mkdir(local_dir_for_file)

    csv_name = f"forms_gastos_compartilhados_{today.strftime('%Y%m%d')}.csv"
    local_csv_path = local_dir_for_file / csv_name

    # ------------------------------------------------------------------
    # 3) Download da planilha do Forms como CSV
    # ------------------------------------------------------------------
    logger.info(
        "Baixando respostas do Forms (sheet_id=%s) -> %s",
        sheet_id,
        local_csv_path,
    )

    # IMPORTANTE:
    # - garanta que a função download_file, ao receber um file_id de planilha (Google Sheets),
    #   esteja usando export_media do Drive para gerar CSV.
    downloaded_path = download_file(
        file_id=sheet_id,
        out_path=str(local_csv_path),
        credentials_path=settings.gdrive.credentials,
        token_path=settings.gdrive.token,
        export_mime_type="text/csv",
    )

    # ------------------------------------------------------------------
    # 4) Upload para MinIO (landing)
    # ------------------------------------------------------------------

    # Ex.:
    # fintrack/01_clientes/cruz_raulino_familia/02_forms/forms_gastos_compartilhados/ano=2026/mes=01/forms_gastos_compartilhados_20260103.csv
    object_name = (
        f"fintrack/01_clientes/{args.client}/02_forms/forms_gastos_compartilhados/{csv_name}"
    )

    logger.info(
        "Enviando CSV do Forms para MinIO | bucket_uri=%s | object_name=%s",
        lnd_bucket_uri,
        object_name,
    )

    s3a_uri = upload_file_to_minio(
        local_path=str(downloaded_path),
        bucket_uri=lnd_bucket_uri,
        object_name=object_name,
    )

    logger.info("Upload concluído. Objeto disponível em: %s", s3a_uri)
    print(f"✅ Forms CSV -> {s3a_uri}")

    # ------------------------------------------------------------------
    # 5) Atualiza estado
    # ------------------------------------------------------------------
    state["last_upload_date"] = today_str
    state["last_s3a_uri"] = s3a_uri
    state["sheet_id"] = sheet_id
    state["client"] = args.client
    state["last_success_upload"] = datetime.now().isoformat(timespec="seconds")

    save_state(state_path, state)

    print("\nOK ✅ Respostas do Forms enviadas para MinIO (landing).")
    print(" - CSV local:", local_csv_path)
    print(" - Estado:   ", state_path)


if __name__ == "__main__":
    main()
