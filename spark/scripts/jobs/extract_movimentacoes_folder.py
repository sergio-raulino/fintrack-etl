#!/usr/bin/env python3
from __future__ import annotations

import re
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd

from fintrack_etl.config import settings
from fintrack_etl.integrations.gdrive.client import list_pdfs_in_folder, download_file

# Parsers
from fintrack_etl.extractors.bb_bill import extract_text as extract_text_bb_fatura
from fintrack_etl.extractors.bb_bill import parse_resumo as parse_resumo_bb_fatura
from fintrack_etl.extractors.bb_bill import parse_lancamentos as parse_lancamentos_bb_fatura

from fintrack_etl.extractors.bb_statement import extract_bb_statement
from fintrack_etl.extractors.bradesco_bill import extract_bradesco_bill


# =========================================================
# Utils
# =========================================================
def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def slugify(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9\-_\.]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def classify_pdf(filename: str) -> str:
    fn = filename.lower()
    if "fatura" in fn and "bradesco" in fn:
        return "fatura_bradesco"
    if "fatura" in fn and "bb" in fn:
        return "fatura_bb"
    if "extrato" in fn and "bb" in fn:
        return "extrato_bb"
    return "desconhecido"


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
        # backup do estado ruim para não quebrar o pipeline
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
    Pula download/processamento se o arquivo não mudou no Drive.
    Critério: modifiedTime igual ao último processado com sucesso.
    (Opcionalmente também confere size quando disponível.)
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
# Processors
# =========================================================
def process_fatura_bb(pdf_path: str, out_dir: Path, also_json: bool) -> Dict[str, Any]:
    full_text = extract_text_bb_fatura(pdf_path)
    resumo = parse_resumo_bb_fatura(full_text)
    df_resumo = pd.DataFrame([asdict_like(resumo)])

    df_lanc = parse_lancamentos_bb_fatura(full_text)

    df_resumo.to_csv(out_dir / "fatura_resumo.csv", index=False)
    df_lanc.to_csv(out_dir / "fatura_lancamentos.csv", index=False)

    df_resumo.to_parquet(out_dir / "fatura_resumo.parquet", index=False)
    df_lanc.to_parquet(out_dir / "fatura_lancamentos.parquet", index=False)

    if also_json:
        with open(out_dir / "fatura_consolidada.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pdf_path": pdf_path,
                    "resumo": asdict_like(resumo),
                    "lancamentos": df_lanc.to_dict(orient="records"),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    return {"status": "ok", "lancamentos_count": int(len(df_lanc))}


def process_extrato_bb(pdf_path: str, out_dir: Path, also_json: bool) -> Dict[str, Any]:
    payload = extract_bb_statement(pdf_path)
    header = payload["header"]
    df = payload["lancamentos"]

    pd.DataFrame([asdict_like(header)]).to_csv(out_dir / "extrato_header.csv", index=False)
    df.to_csv(out_dir / "extrato_lancamentos.csv", index=False)

    pd.DataFrame([asdict_like(header)]).to_parquet(out_dir / "extrato_header.parquet", index=False)
    df.to_parquet(out_dir / "extrato_lancamentos.parquet", index=False)

    if also_json:
        with open(out_dir / "extrato_consolidado.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pdf_path": pdf_path,
                    "header": asdict_like(header),
                    "lancamentos": df.to_dict(orient="records"),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    return {"status": "ok", "lancamentos_count": int(len(df))}


def process_fatura_bradesco(pdf_path: str, out_dir: Path, also_json: bool) -> Dict[str, Any]:
    payload = extract_bradesco_bill(pdf_path)
    resumo = payload["resumo"]
    df = payload["lancamentos"]

    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame([])

    # enriquece sem quebrar (mesmo vazio)
    df = df.copy()
    df["vencimento"] = getattr(resumo, "vencimento", None)
    df["total_fatura"] = getattr(resumo, "total_fatura", None)
    df["titular"] = getattr(resumo, "titular", None)
    df["produto"] = getattr(resumo, "produto", None)

    pd.DataFrame([asdict_like(resumo)]).to_csv(out_dir / "fatura_resumo.csv", index=False)
    df.to_csv(out_dir / "fatura_lancamentos.csv", index=False)

    pd.DataFrame([asdict_like(resumo)]).to_parquet(out_dir / "fatura_resumo.parquet", index=False)
    df.to_parquet(out_dir / "fatura_lancamentos.parquet", index=False)

    if also_json:
        with open(out_dir / "fatura_consolidada.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pdf_path": pdf_path,
                    "resumo": asdict_like(resumo),
                    "lancamentos": df.to_dict(orient="records"),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    return {"status": "ok", "lancamentos_count": int(len(df))}


def asdict_like(obj: object) -> Dict[str, Any]:
    """
    Converte dataclass ou objeto simples em dict.
    (Sem depender de dataclasses.asdict para evitar erros se não for dataclass.)
    """
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    # fallback: tenta pandas-friendly
    return {"value": str(obj)}


# =========================================================
# Main
# =========================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="./data/processed/movimentacoes_25_11")
    ap.add_argument("--also-json", action="store_true")
    ap.add_argument("--only", default=None, help="fatura_bb | extrato_bb | fatura_bradesco")
    ap.add_argument("--force", action="store_true", help="força reprocessamento mesmo sem mudanças no Drive")
    args = ap.parse_args()

    if not settings.gdrive_folder_id:
        raise ValueError("GDRIVE_FOLDER_ID não definido no .env (settings.gdrive_folder_id vazio).")

    out_base = Path(args.out_dir)
    safe_mkdir(out_base)

    state_path = out_base / "_state.json"
    state = load_state(state_path)

    files = list_pdfs_in_folder(
        folder_id=settings.gdrive_folder_id,
        credentials_path=settings.gdrive_credentials,
        token_path=settings.gdrive_token,
    )

    if not files:
        print("Nenhum PDF encontrado na pasta.")
        return

    summary: List[Dict[str, Any]] = []

    for f in files:
        file_id = f["id"]
        name = f["name"]
        kind = classify_pdf(name)

        if args.only and kind != args.only:
            continue

        if (not args.force) and should_skip(f, state):
            print(f"⏭️  SKIP (sem mudanças): {name}")
            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "kind": kind,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    "status": "skip",
                }
            )
            continue

        file_out_dir = out_base / f"{kind}__{slugify(Path(name).stem)}"
        safe_mkdir(file_out_dir)

        local_pdf = str(file_out_dir / name)

        try:
            pdf_path = download_file(
                file_id=file_id,
                out_path=local_pdf,
                credentials_path=settings.gdrive_credentials,
                token_path=settings.gdrive_token,
            )

            if kind == "fatura_bb":
                result = process_fatura_bb(pdf_path, file_out_dir, also_json=args.also_json)
            elif kind == "extrato_bb":
                result = process_extrato_bb(pdf_path, file_out_dir, also_json=args.also_json)
            elif kind == "fatura_bradesco":
                result = process_fatura_bradesco(pdf_path, file_out_dir, also_json=args.also_json)
            else:
                (file_out_dir / "_IGNORADO.txt").write_text(
                    f"Arquivo não reconhecido pelo classificador.\nArquivo: {name}\n",
                    encoding="utf-8",
                )
                result = {"status": "ignorado"}

            # Atualiza state apenas se processamento OK
            if result.get("status") == "ok":
                state[file_id] = {
                    "name": name,
                    "kind": kind,
                    "modifiedTime": f.get("modifiedTime"),
                    "size": f.get("size"),
                    "last_success_at": datetime.now().isoformat(timespec="seconds"),
                }

            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "kind": kind,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    **result,
                }
            )

            print(f"✅ {name} -> {file_out_dir}")

        except Exception as e:
            # não derruba o lote
            err_path = file_out_dir / "_ERROR.txt"
            err_path.write_text(f"Erro ao processar {name}\n{repr(e)}\n", encoding="utf-8")

            summary.append(
                {
                    "file_id": file_id,
                    "file_name": name,
                    "kind": kind,
                    "drive_modifiedTime": f.get("modifiedTime"),
                    "drive_size": f.get("size"),
                    "status": "error",
                    "error": repr(e),
                }
            )

            print(f"❌ ERRO em {name}: {e}")

    # salva estado e sumários
    save_state(state_path, state)

    with open(out_base / "_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    pd.DataFrame(summary).to_csv(out_base / "_summary.csv", index=False)

    print("\nOK ✅ Lote processado em:", out_base)
    print(" -", out_base / "_summary.csv")
    print(" -", out_base / "_summary.json")
    print(" -", out_base / "_state.json")


if __name__ == "__main__":
    main()