#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import yaml
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================================================
# Caminhos e constantes
# ============================================================
ROOT_DIR = Path(__file__).resolve().parent
SECRETS_DIR = ROOT_DIR / "spark" / "secrets"
TOKEN_PATH = SECRETS_DIR / "token.json"

# Arquivo de env com o ID da pasta FinTrack
GDRIVE_ENV_PATH = ROOT_DIR / "spark" / "scripts" / "connections" / "gdrive.env"

# Arquivo yaml com a estrutura de clientes a ser criada no FinTrack
DEFAULT_CLIENTS_CONFIG = ROOT_DIR / "spark" / "scripts" / "sources" / "clients.yaml"

# Escopo recomendado para criar/editar pastas no Drive
SCOPES = ["https://www.googleapis.com/auth/drive"]


# ============================================================
# Helpers para carregar env
# ============================================================
def load_env_file(path: Path) -> Dict[str, str]:
    """
    Carrega um arquivo .env simples em dict.
    Ignora linhas em branco e comentários começando com '#'.
    Suporta 'KEY=VAL' simples.
    """
    env: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Arquivo .env não encontrado: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value
    return env


def get_fintrack_root_id_from_env() -> str:
    env_vars = load_env_file(GDRIVE_ENV_PATH)
    root_id = env_vars.get("GDRIVE_FINTRACK_FOLDER_ROOT_ID")
    if not root_id:
        raise KeyError(
            "Variável GDRIVE_FINTRACK_FOLDER_ROOT_ID não encontrada em "
            f"{GDRIVE_ENV_PATH}. Defina algo como:\n\n"
            "GDRIVE_FINTRACK_FOLDER_ROOT_ID=XXXXXXXXXXXX\n"
        )
    return root_id


# ============================================================
# Auth e construção do serviço Drive
# ============================================================
def build_drive_service() -> "googleapiclient.discovery.Resource":
    if not TOKEN_PATH.exists():
        raise FileNotFoundError(
            f"token.json não encontrado em {TOKEN_PATH}\n"
            "Rode primeiro o script de regeneração de token (OAuth)."
        )

    # Usa os scopes contidos no token.json.
    # Se quiser forçar, passe SCOPES como segundo argumento.
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH))

    # Opcional: garantir que o escopo está adequado
    if SCOPES[0] not in (creds.scopes or []):
        print(
            "⚠️ Aviso: o token atual não parece ter o escopo completo de Drive.\n"
            "   Se ocorrer erro 403 ao criar pastas, regenere o token com SCOPES="
            "'https://www.googleapis.com/auth/drive'."
        )

    service = build("drive", "v3", credentials=creds)
    return service


# ============================================================
# Funções de Drive (folders)
# ============================================================
def get_or_create_folder(service, name: str, parent_id: str) -> str:
    """
    Retorna o ID de uma pasta com nome e parent_id informados.
    Se não existir, cria.
    Compatível com unidades compartilhadas (supportsAllDrives=True).
    """
    query = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{name}' "
        f"and '{parent_id}' in parents "
        "and trashed=false"
    )

    results = (
        service.files()
        .list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = (
        service.files()
        .create(
            body=file_metadata,
            fields="id, name, parents",
            supportsAllDrives=True,
        )
        .execute()
    )
    print(f"Criada pasta: {name} (id={folder['id']})")
    return folder["id"]


def ensure_year_month_tree(
    service, root_folder_id: str, year_start: int, year_end: int
) -> Dict[str, Dict[str, str]]:
    """
    Garante estrutura:
      root_folder_id/
        YYYY/
          MM/

    Retorna um dict { "YYYY": { "MM": folder_id, ... }, ... }.
    """
    year_month_map: Dict[str, Dict[str, str]] = {}

    for year in range(year_start, year_end + 1):
        year_str = str(year)
        year_id = get_or_create_folder(service, year_str, root_folder_id)
        year_month_map[year_str] = {}

        for month in range(1, 13):
            month_str = f"{month:02d}"
            month_id = get_or_create_folder(service, month_str, year_id)
            year_month_map[year_str][month_str] = month_id

    return year_month_map


# ============================================================
# Main
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria estrutura multi-cliente do FinTrack no Google Drive (usando OAuth/token.json)."
    )
    parser.add_argument(
        "--root-folder-id",
        help=(
            "ID da pasta raiz 'FinTrack'. "
            "Se não informado, será lido de GDRIVE_FINTRACK_FOLDER_ROOT_ID em "
            "spark/scripts/connections/gdrive.env."
        ),
    )
    parser.add_argument(
        "--clients-config",
        default=str(DEFAULT_CLIENTS_CONFIG),
        help=(
            "YAML com definição de clientes/bancos/doc_types. "
            "Padrão: spark/scripts/sources/clients.yaml"
        ),
    )
    parser.add_argument(
        "--year-start",
        type=int,
        default=2025,
        help="Ano inicial para criar pastas (inclusive).",
    )
    parser.add_argument(
        "--year-end",
        type=int,
        default=2027,
        help="Ano final para criar pastas (inclusive).",
    )
    parser.add_argument(
        "--output-map",
        default="config/gdrive_folders_map.json",
        help="Arquivo JSON de saída com o mapeamento de pastas.",
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # Descobre root_id
    # --------------------------------------------------------
    if args.root_folder_id:
        root_id = args.root_folder_id
    else:
        root_id = get_fintrack_root_id_from_env()

    # --------------------------------------------------------
    # Carrega YAML de clientes
    # --------------------------------------------------------
    clients_config_path = Path(args.clients_config)
    if not clients_config_path.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração de clientes não encontrado: {clients_config_path}"
        )

    with clients_config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    clients: List[dict] = config.get("clients", [])

    # --------------------------------------------------------
    # Inicializa Drive
    # --------------------------------------------------------
    service = build_drive_service()

    # --------------------------------------------------------
    # Garante pasta 01_clientes dentro de FinTrack
    # --------------------------------------------------------
    clientes_root_id = get_or_create_folder(service, "01_clientes", root_id)

    # Mapeamento final para salvar em JSON
    mapping: Dict[str, dict] = {}

    try:
        for client in clients:
            slug = client["slug"]
            banks = client.get("banks", [])

            print(f"\n=== Cliente: {slug} ===")

            # Pasta do cliente
            client_folder_id = get_or_create_folder(
                service, slug, clientes_root_id
            )

            # 01_bancos
            bancos_root_id = get_or_create_folder(
                service, "01_bancos", client_folder_id
            )

            mapping[slug] = {
                "client_folder_id": client_folder_id,
                "bancos_root_id": bancos_root_id,
                "banks": {},
            }

            for bank in banks:
                bank_code = bank["code"]
                doc_types = bank.get("doc_types", ["extratos", "faturas"])

                print(f"  - Banco: {bank_code}")

                bank_folder_id = get_or_create_folder(
                    service, bank_code, bancos_root_id
                )

                mapping[slug]["banks"][bank_code] = {
                    "bank_folder_id": bank_folder_id,
                    "doc_types": {},
                }

                for doc_type in doc_types:
                    print(f"    * Tipo: {doc_type}")
                    doc_type_folder_id = get_or_create_folder(
                        service, doc_type, bank_folder_id
                    )

                    # Cria/garante anos/meses
                    year_month_map = ensure_year_month_tree(
                        service,
                        doc_type_folder_id,
                        args.year_start,
                        args.year_end,
                    )

                    mapping[slug]["banks"][bank_code]["doc_types"][doc_type] = {
                        "root_folder_id": doc_type_folder_id,
                        "years": year_month_map,
                    }

    except HttpError as e:
        print("❌ Erro ao acessar o Google Drive.")
        print(e)
        return

    # --------------------------------------------------------
    # Salva mapeamento em JSON para o ETL usar
    # --------------------------------------------------------
    output_map_path = ROOT_DIR / args.output_map
    output_map_path.parent.mkdir(parents=True, exist_ok=True)

    with output_map_path.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Mapeamento salvo em: {output_map_path}")


if __name__ == "__main__":
    main()
