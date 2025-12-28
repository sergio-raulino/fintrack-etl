#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.integrations.gdrive.client import (
    list_pdfs_in_folder,
    download_file,
)
from fintrack_etl.integrations.minio.client import upload_file_to_minio

# =========================================================
# Utils
# =========================================================
def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_state(state_path: Path) -> dict:
    """
    Carrega o _state.json de forma tolerante:
      - se não existir: {}
      - se estiver vazio: {}
      - se estiver inválido/corrompido: renomeia para .bad e retorna {}
    """
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
    """
    Escrita atômica: escreve em arquivo temporário e faz replace.
    Evita deixar _state.json vazio/corrompido se o processo cair.
    """
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(state_path)


def should_skip(file_meta: dict, state: dict) -> bool:
    """
    Pula download/upload se o arquivo não mudou no Drive.
    Critério: modifiedTime igual ao último processado com sucesso
    (e opcionalmente size).
    """
    file_id = file_meta["id"]
    mt = file_meta.get("modifiedTime")
    sz = file_meta.get("size")

    prev = state.get(file_id)
    if not prev:
        return False

    if mt and prev.get("modifiedTime") == mt:
        if sz is None or prev.get("size") == sz:
            return True

    return False


# =========================================================
# Main
# =========================================================
def main() -> None:
    """
    Lista todos os PDFs da pasta GDrive (movimentações 11/2025),
    baixa localmente e envia *apenas os PDFs* para o MinIO
    na camada landing, sem qualquer processamento de conteúdo.
    """
    ap = argparse.ArgumentParser(
        description="Baixa PDFs de movimentações (11/2025) do GDrive e envia para MinIO (landing)."
    )
    ap.add_argument(
        "--local-dir",
        default="./data/raw/drive/movimentacoes_25_11",
        help="Diretório local para armazenar temporariamente os PDFs.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Força re-download/upload mesmo se não houver mudanças no Drive.",
    )
    args = ap.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    # Sanidade GDrive
    if not settings.gdrive.folder_id:
        raise ValueError(
            "GDRIVE_FOLDER_ID não definido no .env (settings.gdrive.folder_id vazio)."
        )

    local_base = Path(args.local_dir)
    safe_mkdir(local_base)

    state_path = local_base / "_state_upload.json"
    state = load_state(state_path)

    # Bucket da camada landing (ex: s3a://sdl-lnd-fin)
    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket

    logger.info(
        "Listando PDFs na pasta GDrive (movimentações 11/2025). folder_id=%s",
        settings.gdrive.folder_id,
    )
    files = list_pdfs_in_folder(
        folder_id=settings.gdrive.folder_id,
        credentials_path=settings.gdrive.credentials,
        token_path=settings.gdrive.token,
    )

    if not files:
        logger.warning("Nenhum PDF encontrado na pasta do Drive.")
        print("Nenhum PDF encontrado na pasta do Drive.")
        return

    summary: List[Dict[str, Any]] = []

    for f in files:
        file_id = f["id"]
        name = f["name"]

        if (not args.force) and should_skip(f, state):
            msg = f"⏭️  SKIP (sem mudanças no Drive): {name}"
            logger.info(msg)
            print(msg)
            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    "status": "skip",
                }
            )
            continue

        local_pdf = str(local_base / name)

        try:
            # 1) Download do PDF do Drive
            logger.info("Baixando PDF do Drive: %s (%s) -> %s", name, file_id, local_pdf)
            pdf_path = download_file(
                file_id=file_id,
                out_path=local_pdf,
                credentials_path=settings.gdrive.credentials,
                token_path=settings.gdrive.token,
            )

            # 2) Upload para MinIO (landing)
            # Prefixo lógico para mês 11/2025
            object_name = f"fintrack/movimentacoes_25_11/{name}"

            logger.info(
                "Enviando para MinIO | bucket_uri=%s | object_name=%s",
                lnd_bucket_uri,
                object_name,
            )

            s3a_uri = upload_file_to_minio(
                local_path=pdf_path,
                bucket_uri=lnd_bucket_uri,
                object_name=object_name,
            )

            logger.info("Upload concluído. Objeto disponível em: %s", s3a_uri)
            print(f"✅ {name} -> {s3a_uri}")

            # atualiza estado apenas se upload OK
            state[file_id] = {
                "name": name,
                "modifiedTime": f.get("modifiedTime"),
                "size": f.get("size"),
                "last_success_upload": datetime.now().isoformat(timespec="seconds"),
                "s3a_uri": s3a_uri,
            }

            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    "status": "uploaded",
                    "s3a_uri": s3a_uri,
                }
            )

        except Exception as e:
            logger.exception("Erro ao enviar %s para MinIO: %s", name, e)
            print(f"❌ ERRO em {name}: {e}")

            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    "status": "error",
                    "error": repr(e),
                }
            )

    # Salva estado e resumo do lote (apenas upload, sem parsing)
    save_state(state_path, state)

    with open(local_base / "_upload_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\nOK ✅ Lote de PDFs enviado para MinIO (landing).")
    print(" -", local_base / "_upload_summary.json")
    print(" -", state_path)


if __name__ == "__main__":
    main()
