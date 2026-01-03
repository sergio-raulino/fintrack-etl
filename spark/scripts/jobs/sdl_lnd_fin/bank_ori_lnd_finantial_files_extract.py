#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import logging
import argparse
from pathlib import Path

from datetime import datetime, date
from typing import Dict, Any, List, Optional

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.integrations.gdrive.client import (
    list_pdfs_in_folder,
    download_file,
)
from fintrack_etl.integrations.minio.client import upload_file_to_minio

# raiz do projeto dentro do contêiner (/opt/spark-jobs)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Caminho padrão do mapa de pastas do GDrive
GDRIVE_MAP_DEFAULT_PATH = ROOT_DIR / "sources" / "gdrive_folders_map.json"
GDRIVE_MAP_ENV_VAR = "GDRIVE_FOLDERS_MAP_PATH"


# =========================================================
# Utils de filesystem / estado
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
# Utils de GDrive (mapa e folders)
# =========================================================
def load_gdrive_map(path: Optional[Path] = None) -> dict:
    """
    Carrega o gdrive_folders_map.json.
    Ordem de resolução:
      1) Variável de ambiente GDRIVE_FOLDERS_MAP_PATH (se existir)
      2) Caminho passado como parâmetro
      3) GDRIVE_MAP_DEFAULT_PATH (spark/scripts/sources/gdrive_folders_map.json)
    """
    env_path = os.getenv(GDRIVE_MAP_ENV_VAR)
    if env_path:
        map_path = Path(env_path)
    elif path is not None:
        map_path = path
    else:
        map_path = GDRIVE_MAP_DEFAULT_PATH

    if not map_path.exists():
        raise FileNotFoundError(
            f"Mapa de pastas do GDrive não encontrado: {map_path}\n"
            "Certifique-se de ter rodado o setup_gdrive_structure.py "
            "e gerado o gdrive_folders_map.json."
        )

    with map_path.open("r", encoding="utf-8") as f:
        return json.load(f)


from datetime import date
from typing import Optional, List, Dict, Any

def iter_client_month_folders(
    gdrive_map: dict,
    client_slug: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Gera uma lista de dicts com:
      {
        "client_slug": ...,
        "bank_code": ...,
        "doc_type": "extratos" | "faturas",
        "year_str": "YYYY",
        "month_str": "MM",
        "folder_id": "xxxx"
      }
    com base no gdrive_folders_map.json.
    Se year e/ou month forem informados, filtra.
    Também evita varrer meses no futuro em relação à data atual.
    """
    if client_slug not in gdrive_map:
        raise KeyError(
            f"Cliente '{client_slug}' não encontrado no gdrive_folders_map.json."
        )

    client_entry = gdrive_map[client_slug]
    banks = client_entry.get("banks", {})

    year_filter = str(year) if year is not None else None
    month_filter = f"{month:02d}" if month is not None else None

    # 👉 corte automático para não varrer meses no futuro
    today = date.today()
    current_ym = today.year * 100 + today.month

    results: List[Dict[str, Any]] = []

    for bank_code, bank_entry in banks.items():
        doc_types = bank_entry.get("doc_types", {})
        for doc_type, doc_entry in doc_types.items():
            years_dict = doc_entry.get("years", {})
            for year_str, months_dict in years_dict.items():
                if year_filter and year_str != year_filter:
                    continue

                for month_str, folder_id in months_dict.items():
                    if month_filter and month_str != month_filter:
                        continue

                    # yyyy-mm da pasta
                    ym = int(year_str) * 100 + int(month_str)

                    # pula meses no futuro
                    if ym > current_ym:
                        continue

                    results.append(
                        {
                            "client_slug": client_slug,
                            "bank_code": bank_code,
                            "doc_type": doc_type,
                            "year_str": year_str,
                            "month_str": month_str,
                            "folder_id": folder_id,
                        }
                    )

    return results


# =========================================================
# Main
# =========================================================
def main() -> None:
    """
    Lista todos os PDFs/CSVs das pastas de extratos/faturas da família
    'cruz_raulino_familia' no GDrive (conforme gdrive_folders_map.json),
    baixa localmente e envia para o MinIO na camada landing, mantendo
    a estrutura de pastas da origem.
    """
    ap = argparse.ArgumentParser(
        description=(
            "Baixa PDFs/CSVs de extratos/faturas da família cruz_raulino_familia "
            "no GDrive e envia para MinIO (landing), preservando a estrutura."
        )
    )
    ap.add_argument(
        "--client",
        default="cruz_raulino_familia",
        help="Slug do cliente no gdrive_folders_map.json (default: cruz_raulino_familia).",
    )
    ap.add_argument(
        "--local-dir",
        default="./data/raw/drive",
        help="Diretório local base para armazenar temporariamente os arquivos.",
    )
    ap.add_argument(
        "--year",
        type=int,
        default=None,
        help="Ano específico a processar (YYYY). Se omitido, processa todos os anos do mapa.",
    )
    ap.add_argument(
        "--month",
        type=int,
        default=None,
        help="Mês específico (1-12). Se omitido, processa todos os meses do mapa.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Força re-download/upload mesmo se não houver mudanças no Drive.",
    )
    ap.add_argument(
        "--gdrive-map-path",
        default=None,
        help=(
            "Caminho explícito para gdrive_folders_map.json. "
            "Se omitido, usa env GDRIVE_FOLDERS_MAP_PATH ou "
            "spark/scripts/sources/gdrive_folders_map.json."
        ),
    )
    args = ap.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    local_base = Path(args.local_dir)
    safe_mkdir(local_base)

    # Carrega mapa de pastas do GDrive
    gdrive_map_path = Path(args.gdrive_map_path) if args.gdrive_map_path else None
    gdrive_map = load_gdrive_map(gdrive_map_path)

    # Descobre todas as pastas (por banco/doc_type/ano/mês) do cliente alvo
    target_folders = iter_client_month_folders(
        gdrive_map=gdrive_map,
        client_slug=args.client,
        year=args.year,
        month=args.month,
    )

    if not target_folders:
        logger.warning(
            "Nenhuma pasta encontrada para client=%s com os filtros year=%s, month=%s.",
            args.client,
            args.year,
            args.month,
        )
        print("Nenhuma pasta encontrada no mapa para os filtros informados.")
        return

    # Bucket da camada landing (ex: s3a://sdl-lnd-fin)
    lnd_bucket_uri = settings.hive_fin.sdl_lnd_bucket

    # Estado global (por file_id) – um único _state para o job todo
    state_path = local_base / f"_state_upload_{args.client}.json"
    state = load_state(state_path)

    summary: List[Dict[str, Any]] = []

    logger.info(
        "Iniciando carga de arquivos GDrive -> MinIO para client=%s | total de pastas=%d",
        args.client,
        len(target_folders),
    )

    for folder_info in target_folders:
        client_slug = folder_info["client_slug"]
        bank_code = folder_info["bank_code"]
        doc_type = folder_info["doc_type"]  # "extratos" / "faturas"
        year_str = folder_info["year_str"]
        month_str = folder_info["month_str"]
        folder_id = folder_info["folder_id"]

        logger.info(
            "Listando arquivos em client=%s | banco=%s | tipo=%s | ano=%s | mes=%s | folder_id=%s",
            client_slug,
            bank_code,
            doc_type,
            year_str,
            month_str,
            folder_id,
        )

        # OBS: hoje a função se chama list_pdfs_in_folder.
        # Para incluir CSV também, você pode:
        #  - alterar a implementação para listar todos os arquivos da pasta
        #  - ou criar uma nova função list_files_in_folder e usá-la aqui.
        files = list_pdfs_in_folder(
            folder_id=folder_id,
            credentials_path=settings.gdrive.credentials,
            token_path=settings.gdrive.token,
        )

        if not files:
            logger.warning(
                "Nenhum arquivo encontrado em client=%s | banco=%s | tipo=%s | ano=%s | mes=%s.",
                client_slug,
                bank_code,
                doc_type,
                year_str,
                month_str,
            )
            continue

        for fmeta in files:
            file_id = fmeta["id"]
            name = fmeta["name"]

            # Filtra por extensão (mantendo PDF e CSV)
            lower_name = name.lower()
            if not (lower_name.endswith(".pdf") or lower_name.endswith(".csv")):
                logger.debug("Ignorando arquivo não .pdf/.csv: %s", name)
                continue

            if (not args.force) and should_skip(fmeta, state):
                msg = (
                    f"⏭️  SKIP (sem mudanças no Drive): {name} | "
                    f"{client_slug}/{bank_code}/{doc_type}/{year_str}/{month_str}"
                )
                logger.info(msg)
                print(msg)
                summary.append(
                    {
                        "file_id": file_id,
                        "file_name": name,
                        "client_slug": client_slug,
                        "bank_code": bank_code,
                        "doc_type": doc_type,
                        "year": year_str,
                        "month": month_str,
                        "drive_modifiedTime": fmeta.get("modifiedTime"),
                        "drive_size": fmeta.get("size"),
                        "status": "skip",
                    }
                )
                continue

            # Diretório local preservando estrutura
            # Ex.: ./data/raw/drive/01_clientes/cruz_raulino_familia/01_bancos/bb/extratos/2025/11/arquivo.pdf
            relative_path = (
                Path("01_clientes")
                / client_slug
                / "01_bancos"
                / bank_code
                / doc_type
                / year_str
                / month_str
            )
            local_dir_for_file = local_base / relative_path
            safe_mkdir(local_dir_for_file)

            local_path = local_dir_for_file / name

            try:
                # 1) Download do arquivo do Drive
                logger.info(
                    "Baixando arquivo do Drive: %s (%s) -> %s",
                    name,
                    file_id,
                    local_path,
                )
                downloaded_path = download_file(
                    file_id=file_id,
                    out_path=str(local_path),
                    credentials_path=settings.gdrive.credentials,
                    token_path=settings.gdrive.token,
                )

                # 2) Upload para MinIO (landing), preservando a estrutura
                # Ex.: fintrack/01_clientes/cruz_raulino_familia/01_bancos/bb/extratos/2025/11/arquivo.pdf
                object_name = (
                    f"fintrack/01_clientes/{client_slug}/01_bancos/"
                    f"{bank_code}/{doc_type}/{year_str}/{month_str}/{name}"
                )

                logger.info(
                    "Enviando para MinIO | bucket_uri=%s | object_name=%s",
                    lnd_bucket_uri,
                    object_name,
                )

                s3a_uri = upload_file_to_minio(
                    local_path=str(downloaded_path),
                    bucket_uri=lnd_bucket_uri,
                    object_name=object_name,
                )

                logger.info("Upload concluído. Objeto disponível em: %s", s3a_uri)
                print(f"✅ {name} -> {s3a_uri}")

                # Atualiza estado apenas se upload OK
                state[file_id] = {
                    "name": name,
                    "client_slug": client_slug,
                    "bank_code": bank_code,
                    "doc_type": doc_type,
                    "year": year_str,
                    "month": month_str,
                    "modifiedTime": fmeta.get("modifiedTime"),
                    "size": fmeta.get("size"),
                    "last_success_upload": datetime.now().isoformat(timespec="seconds"),
                    "s3a_uri": s3a_uri,
                }

                summary.append(
                    {
                        "file_id": file_id,
                        "file_name": name,
                        "client_slug": client_slug,
                        "bank_code": bank_code,
                        "doc_type": doc_type,
                        "year": year_str,
                        "month": month_str,
                        "drive_modifiedTime": fmeta.get("modifiedTime"),
                        "drive_size": fmeta.get("size"),
                        "status": "uploaded",
                        "s3a_uri": s3a_uri,
                    }
                )

            except Exception as e:
                logger.exception(
                    "Erro ao enviar %s para MinIO (client=%s | banco=%s | tipo=%s | ano=%s | mes=%s): %s",
                    name,
                    client_slug,
                    bank_code,
                    doc_type,
                    year_str,
                    month_str,
                    e,
                )
                print(f"❌ ERRO em {name}: {e}")

                summary.append(
                    {
                        "file_id": file_id,
                        "file_name": name,
                        "client_slug": client_slug,
                        "bank_code": bank_code,
                        "doc_type": doc_type,
                        "year": year_str,
                        "month": month_str,
                        "drive_modifiedTime": fmeta.get("modifiedTime"),
                        "drive_size": fmeta.get("size"),
                        "status": "error",
                        "error": repr(e),
                    }
                )

    # Salva estado e resumo do lote (apenas upload, sem parsing de conteúdo)
    save_state(state_path, state)

    summary_path = local_base / f"_upload_summary_{args.client}.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\nOK ✅ Lote de arquivos enviado para MinIO (landing).")
    print(" -", summary_path)
    print(" -", state_path)


if __name__ == "__main__":
    main()
