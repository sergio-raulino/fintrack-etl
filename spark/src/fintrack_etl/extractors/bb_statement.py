from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import pandas as pd
import pdfplumber


# ---------------- helpers ----------------
def normalize_space(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "")).strip()


def brl_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    s = s.replace("R$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    return "\n".join(pages)


# ---------------- parsing ----------------
@dataclass
class ExtratoHeader:
    cliente: Optional[str] = None
    periodo_inicio: Optional[str] = None
    periodo_fim: Optional[str] = None
    agencia: Optional[str] = None
    conta: Optional[str] = None
    competencia: Optional[str] = None  # YYYY-MM


def parse_header(full_text: str) -> ExtratoHeader:
    t = full_text

    cliente = None
    m = re.search(r"Cliente\s+([A-ZÀ-ÿ ]+)", t, flags=re.IGNORECASE)
    if m:
        cliente = normalize_space(m.group(1))

    # Período: 01 a 30/11/2025
    ini, fim, comp = None, None, None
    mp = re.search(r"Período:\s*(\d{2})\s*a\s*(\d{2})/(\d{2})/(\d{4})", t, flags=re.IGNORECASE)
    if mp:
        d_ini = mp.group(1)
        d_fim = mp.group(2)
        mm = mp.group(3)
        yyyy = mp.group(4)
        ini = f"{d_ini}/{mm}/{yyyy}"
        fim = f"{d_fim}/{mm}/{yyyy}"
        comp = f"{yyyy}-{mm}"

    # Agência: 4041-X Conta: 18506-X
    agencia, conta = None, None
    ma = re.search(r"Agência:\s*([0-9A-Z\-]+)", t, flags=re.IGNORECASE)
    if ma:
        agencia = normalize_space(ma.group(1))
    mc = re.search(r"Conta:\s*([0-9A-Z\-]+)", t, flags=re.IGNORECASE)
    if mc:
        conta = normalize_space(mc.group(1))

    return ExtratoHeader(
        cliente=cliente,
        periodo_inicio=ini,
        periodo_fim=fim,
        agencia=agencia,
        conta=conta,
        competencia=comp,
    )


def parse_lancamentos(full_text: str) -> pd.DataFrame:
    """
    Extrai tabela 'Lançamentos' do Extrato BB (layout textual).
    Estratégia:
      - detecta grupo/seção: linhas tipo "Pix - Enviado", "Pagamento de Boleto", etc.
      - detecta lançamento quando começa com dd/mm/aaaa e termina com "valor ( +|- )"
      - agrega linhas de continuação (descrição complementar) até o próximo lançamento ou grupo
    """
    lines = [normalize_space(x) for x in full_text.splitlines() if normalize_space(x)]

    # detecta linha de lançamento:
    # ex: 03/11/2025 14134 167104 TRIBUNAL ... 15.149,04 (+)
    # ex: 31/10/2025 Saldo Anterior 260,11 (-)
    launch_re = re.compile(
        r"^(?P<data>\d{2}/\d{2}/\d{4})\s+"
        r"(?:(?P<lote>\d{3,})\s+)?"
        r"(?:(?P<doc>\d{3,})\s+)?"
        r"(?P<hist>.*?)\s*"
        r"(?P<valor>[\d\.\,]+)\s+\((?P<sinal>[+-])\)\s*$"
    )

    # Cabeçalho da tabela (ignorar)
    header_like = {
        "Lançamentos",
        "Dia Lote Documento Histórico Valor",
        "Extrato de Conta Corrente",
    }

    def is_group_line(ln: str) -> bool:
        # Heurística: "Pix - Enviado", "Pagamento de Boleto", "BB Rende Fácil", etc.
        # Evita confundir com continuação (que geralmente tem data/hora ou nome).
        if re.match(r"^\d{2}/\d{2}/\d{4}\b", ln):
            return False
        if ln in header_like:
            return False
        if ln.lower().startswith("saldo do dia"):
            return True
        # linhas curtas e "título"
        if len(ln) <= 40 and any(ch.isalpha() for ch in ln) and not re.search(r"\d", ln):
            return True
        if " - " in ln and len(ln) <= 60:
            return True
        # nomes típicos de grupos do extrato
        if ln.lower() in {"pix - enviado", "pix - recebido", "pagamento de boleto", "recebimento de proventos",
                          "cobrança de juros", "cobrança de i.o.f.", "bb rende fácil"}:
            return True
        return False

    rows: List[Dict[str, Any]] = []
    current_group: Optional[str] = None
    current_row: Optional[Dict[str, Any]] = None

    def flush_current():
        nonlocal current_row
        if current_row:
            # normaliza historico final
            current_row["historico"] = normalize_space(current_row.get("historico") or "")
            current_row["complemento"] = normalize_space(current_row.get("complemento") or "")
            # junta histórico + complemento se existir
            if current_row["complemento"]:
                current_row["historico_full"] = normalize_space(
                    f"{current_row['historico']} | {current_row['complemento']}"
                )
            else:
                current_row["historico_full"] = current_row["historico"]

            rows.append(current_row)
            current_row = None

    for ln in lines:
        if ln in header_like:
            continue

        # grupo/seção
        if is_group_line(ln):
            # salva lançamento anterior antes de trocar grupo
            flush_current()
            current_group = ln
            continue

        m = launch_re.match(ln)
        if m:
            # novo lançamento: fecha anterior
            flush_current()

            data = m.group("data")
            lote = m.group("lote")
            doc = m.group("doc")
            hist = normalize_space(m.group("hist"))
            valor = brl_to_float(m.group("valor"))
            sinal = m.group("sinal")

            signed_val = None
            if valor is not None:
                signed_val = valor if sinal == "+" else -valor

            current_row = {
                "data": data,
                "lote": lote,
                "documento": doc,
                "grupo": current_group,
                "historico": hist,
                "complemento": "",
                "valor": signed_val,
                "sinal": sinal,
            }
            continue

        # continuação do histórico
        if current_row:
            # algumas linhas são "Saldo do dia 0,00 (+)" sem data — se cair aqui, vira complemento
            if current_row["complemento"]:
                current_row["complemento"] += " " + ln
            else:
                current_row["complemento"] = ln

    flush_current()

    return pd.DataFrame(rows)


def extract_bb_statement(pdf_path: str) -> Dict[str, Any]:
    full_text = extract_text(pdf_path)
    header = parse_header(full_text)
    df = parse_lancamentos(full_text)
    return {
        "header": header,
        "lancamentos": df,
    }
