from __future__ import annotations

import os
import re
import json
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Any

import pandas as pd
import pdfplumber

from fintrack_etl.config import settings
from fintrack_etl.integrations.gdrive.client import download_file
from fintrack_etl.rules.categories_bb import categorize_bb


# =========================================================
# Helpers
# =========================================================
def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_space(text: str) -> str:
    return re.sub(r"[ \t]+", " ", (text or "")).strip()


def brl_to_float(s: str) -> Optional[float]:
    """
    'R$ 5.899,51' ou '5.899,51' ou '-6.797,51' -> float
    """
    if not s:
        return None
    s = s.strip()
    s = s.replace("R$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def find_first(pattern: str, text: str, flags=re.IGNORECASE | re.MULTILINE) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def find_date_after_keyword(text: str, keyword: str, window: int = 160) -> Optional[str]:
    """
    Encontra uma data dd/mm/aaaa que apareça logo após uma palavra-chave.
    Útil quando o PDF quebra linha e o label fica separado do valor.
    """
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return None
    snippet = text[idx : idx + window]
    m = re.search(r"(\d{2}/\d{2}/\d{4})", snippet)
    return m.group(1) if m else None


# =========================================================
# Modelos / parsing do PDF BB
# =========================================================
@dataclass
class FaturaResumo:
    titular: Optional[str] = None
    endereco: Optional[str] = None
    mes_referencia: Optional[str] = None
    vencimento: Optional[str] = None
    valor_total: Optional[float] = None
    limite_unico: Optional[float] = None

    limite_utilizado: Optional[float] = None
    limite_disponivel: Optional[float] = None

    saldo_anterior: Optional[float] = None
    pagamentos_creditos: Optional[float] = None
    compras_nacionais: Optional[float] = None
    compras_internacionais: Optional[float] = None
    tarifas_encargos_multas: Optional[float] = None
    saldo_parcelado_futuro: Optional[float] = None
    pagamento_minimo: Optional[float] = None

    fatura_fechada_em: Optional[str] = None
    fechamento_proxima_fatura: Optional[str] = None
    melhor_data_compra: Optional[str] = None


def extract_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    return "\n".join(pages)


def parse_resumo(full_text: str) -> FaturaResumo:
    t = full_text

    titular = find_first(r"^([A-Za-zÀ-ÿ ]+)\s+RUA", t)
    endereco = find_first(r"^.*?\n([A-Z0-9À-ÿ ,\-]+CE\s*-\s*\d{8})", t)

    mes = find_first(r"fatura de\s+([A-ZÀ-ÿa-z]+)", t)

    venc = (
        find_first(r"Vencimento\s*(\d{2}/\d{2}/\d{4})", t)
        or find_first(r"Vencimento\s*\n\s*(\d{2}/\d{2}/\d{4})", t)
        or find_date_after_keyword(t, "Vencimento", window=250)
    )

    valor = (
        find_first(r"\bValor\b\s*R\$\s*([\d\.\,]+)", t)
        or find_first(r"\bValor\b\s*\n\s*R\$\s*([\d\.\,]+)", t)
        or find_first(r"\bTotal\b\s*R\$\s*([\d\.\,]+)", t)
        or find_first(r"\bTotal da Fatura\b\s*R\$\s*([\d\.\,]+)", t)
    )

    limite = find_first(r"Limite único\s+R\$\s*([\d\.\,]+)", t)

    limite_util = find_first(r"Limite único utilizado\s+R\$\s*([\d\.\,]+)", t)
    limite_disp = find_first(r"Limite único disponível\s+R\$\s*([\d\.\,]+)", t)

    saldo_ant = find_first(r"Saldo fatura anterior\s+R\$\s*([\d\.\,]+)", t)
    pag_cred = find_first(r"Pagamentos/Créditos\s+R\$\s*([-\d\.\,]+)", t)
    comp_nac = find_first(r"Compras nacionais\s+R\$\s*([\d\.\,]+)", t)
    comp_int = find_first(r"Compras internacionais\s+R\$\s*([\d\.\,]+)", t)
    tarifas = find_first(r"Tarifas, encargos e multas\s+R\$\s*([\d\.\,]+)", t)
    saldo_parc = find_first(r"Saldo parcelado em faturas\s+futuras\s+R\$\s*([\d\.\,]+)", t)

    pag_min = (
        find_first(r"Pagamento mínimo\s*R\$\s*([\d\.\,]+)", t)
        or find_first(r"Pagamento mínimo\s*\n\s*R\$\s*([\d\.\,]+)", t)
        or find_first(r"\bvalor mínimo\b.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)
    )

    # Alguns PDFs retornam "OUROCARD" no lugar do mês; tenta corrigir
    if mes and mes.upper() == "OUROCARD":
        mes2 = find_first(
            r"\b(JANEIRO|FEVEREIRO|MARÇO|MARCO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\b",
            t,
            flags=re.IGNORECASE,
        )
        mes = mes2

    fechada = find_first(r"Fatura fechada em\s+(\d{2}/\d{2}/\d{4})", t)
    prox = find_first(r"Fechamento da próxima fatura\s+(\d{2}/\d{2}/\d{4})", t)
    melhor = find_first(r"Melhor data de compra\s+(\d{2}/\d{2}/\d{4})", t)

    return FaturaResumo(
        titular=normalize_space(titular) if titular else None,
        endereco=normalize_space(endereco) if endereco else None,
        mes_referencia=mes,
        vencimento=venc,
        valor_total=brl_to_float(valor) if valor else None,
        limite_unico=brl_to_float(limite) if limite else None,
        limite_utilizado=brl_to_float(limite_util) if limite_util else None,
        limite_disponivel=brl_to_float(limite_disp) if limite_disp else None,
        saldo_anterior=brl_to_float(saldo_ant) if saldo_ant else None,
        pagamentos_creditos=brl_to_float(pag_cred) if pag_cred else None,
        compras_nacionais=brl_to_float(comp_nac) if comp_nac else None,
        compras_internacionais=brl_to_float(comp_int) if comp_int else None,
        tarifas_encargos_multas=brl_to_float(tarifas) if tarifas else None,
        saldo_parcelado_futuro=brl_to_float(saldo_parc) if saldo_parc else None,
        pagamento_minimo=brl_to_float(pag_min) if pag_min else None,
        fatura_fechada_em=fechada,
        fechamento_proxima_fatura=prox,
        melhor_data_compra=melhor,
    )


def parse_lancamentos(full_text: str) -> pd.DataFrame:
    lines = [normalize_space(x) for x in full_text.splitlines() if normalize_space(x)]

    card_re = re.compile(r"^(.*)\s+\(Cartão\s+(\d{4})\)\s*$", re.IGNORECASE)

    txn_re = re.compile(
        r"^(?P<data>\d{2}/\d{2})\s+(?P<desc>.+?)\s+(?P<pais>[A-Z]{2})\s+R\$\s*(?P<valor>-?[\d\.\,]+)\s*$"
    )

    txn_re_no_country = re.compile(
        r"^(?P<data>\d{2}/\d{2})\s+(?P<desc>.+?)\s+R\$\s*(?P<valor>-?[\d\.\,]+)\s*$"
    )

    current_holder = None
    current_card = None
    rows: List[Dict[str, Any]] = []

    for ln in lines:
        mcard = card_re.match(ln)
        if mcard:
            current_holder = normalize_space(mcard.group(1))
            current_card = mcard.group(2)
            continue

        m = txn_re.match(ln)
        if m:
            rows.append(
                {
                    "titular_cartao": current_holder,
                    "final_cartao": current_card,
                    "data": m.group("data"),
                    "pais": m.group("pais"),
                    "descricao": normalize_space(m.group("desc")),
                    "valor": brl_to_float(m.group("valor")),
                }
            )
            continue

        m2 = txn_re_no_country.match(ln)
        if m2:
            rows.append(
                {
                    "titular_cartao": current_holder,
                    "final_cartao": current_card,
                    "data": m2.group("data"),
                    "pais": None,
                    "descricao": normalize_space(m2.group("desc")),
                    "valor": brl_to_float(m2.group("valor")),
                }
            )
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Regras de negócio (categoria/subcategoria/etc.)
    cats = df["descricao"].apply(categorize_bb).apply(pd.Series)
    df = pd.concat([df, cats], axis=1)

    return df.reset_index(drop=True)


# =========================================================
# Pipeline "cluster-friendly" (para entrypoint Spark)
# =========================================================
def run_bb_bill(
    *,
    source: str,
    out_dir: str,
    local_pdf: Optional[str] = None,
    also_json: bool = False,
    write_csv: bool = False,
) -> Dict[str, Any]:
    """
    Executa extração BB fatura (PDF), aplica parsing e regras de negócio, e salva datasets.

    - source: "drive" ou "local"
    - out_dir: diretório de saída (no container/volume)
    - local_pdf: path do PDF quando source="local"
    - also_json: salva JSON consolidado
    - write_csv: opcional (para debug/local), mantém compatibilidade com seu fluxo antigo
    """
    safe_mkdir(out_dir)

    # 1) obter PDF
    if source == "drive":
        pdf_path = download_file(
            file_id=settings.gdrive_file_id,
            out_path=settings.gdrive_out_path,
            credentials_path=settings.gdrive_credentials,
            token_path=settings.gdrive_token,
        )
    elif source == "local":
        if not local_pdf:
            raise ValueError("Informe local_pdf quando source=local")
        pdf_path = local_pdf
    else:
        raise ValueError(f"source inválido: {source}")

    # 2) extrair e parsear
    full_text = extract_text(pdf_path)
    resumo = parse_resumo(full_text)
    df_resumo = pd.DataFrame([asdict(resumo)])
    df_lanc = parse_lancamentos(full_text)

    # 3) persistência
    resumo_parquet = os.path.join(out_dir, "fatura_resumo.parquet")
    lanc_parquet = os.path.join(out_dir, "fatura_lancamentos.parquet")

    df_resumo.to_parquet(resumo_parquet, index=False)
    df_lanc.to_parquet(lanc_parquet, index=False)

    resumo_csv = None
    lanc_csv = None
    if write_csv:
        resumo_csv = os.path.join(out_dir, "fatura_resumo.csv")
        lanc_csv = os.path.join(out_dir, "fatura_lancamentos.csv")
        df_resumo.to_csv(resumo_csv, index=False)
        df_lanc.to_csv(lanc_csv, index=False)

    out_json = None
    if also_json:
        out_json = os.path.join(out_dir, "fatura_consolidada.json")
        payload = {
            "pdf_path": pdf_path,
            "resumo": asdict(resumo),
            "lancamentos": df_lanc.to_dict(orient="records"),
        }
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    # 4) métricas simples úteis para logs
    totals_by_cat = None
    if not df_lanc.empty and "categoria" in df_lanc.columns:
        totals_by_cat = (
            df_lanc.groupby("categoria")["valor"].sum().sort_values(ascending=False).to_dict()
        )

    return {
        "pdf_path": pdf_path,
        "out_dir": out_dir,
        "files": {
            "resumo_parquet": resumo_parquet,
            "lanc_parquet": lanc_parquet,
            "resumo_csv": resumo_csv,
            "lanc_csv": lanc_csv,
            "json": out_json,
        },
        "counts": {
            "resumo_rows": int(len(df_resumo)),
            "lanc_rows": int(len(df_lanc)),
        },
        "totals_by_categoria": totals_by_cat,
    }
