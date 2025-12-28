# src/fintrack_etl/extractors/bradesco_bill.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import pdfplumber


def normalize_space(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "")).strip()


def brl_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace("R$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    return "\n".join(pages)


@dataclass
class BradescoResumo:
    titular: Optional[str] = None
    produto: Optional[str] = None
    total_fatura: Optional[float] = None
    vencimento: Optional[str] = None
    fechamento_proxima: Optional[str] = None

    limite_compras: Optional[float] = None
    limite_saque: Optional[float] = None

    pagamento_minimo: Optional[float] = None

    saldo_anterior: Optional[float] = None
    creditos_pagamentos: Optional[float] = None
    compras_debitos: Optional[float] = None
    total_resumo: Optional[float] = None


def find_first(pattern: str, text: str, flags=re.IGNORECASE | re.MULTILINE) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def parse_resumo(full_text: str) -> BradescoResumo:
    t = full_text

    produto = find_first(r"^\s*(VISA.*)$", t)  # "VISA INFINITE PRIME"

    titular = find_first(r"\n([A-ZÀ-Ÿ ]{8,})\n\s*CENTRO\b", t)
    if not titular:
        # fallback: pega o 1º titular que aparece nos blocos "NOME Cartão 4066 XXXX..."
        titular = find_first(
            r"\n([A-ZÀ-Ÿ ]{8,})\s+Cart[aã]o\s+\d{4}\s+X{4}\s+X{4}\s+\d{4}\b", t
        )

    # "Total da fatura ... R$ 12.027,45 05/12/2025"
    total = find_first(r"Total da fatura.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)
    venc = find_first(r"Total da fatura.*?(\d{2}/\d{2}/\d{4})", t, flags=re.IGNORECASE | re.DOTALL)

    fechamento = find_first(r"Previsão de fechamento da próxima fatura:\s*(\d{2}/\d{2}/\d{4})", t)

    # Limites vêm num bloco com "Limite de compras  Limite de saque  R$ X  R$ Y"
    limites = re.search(
        r"Limite de compras\s+Limite de saque\s+R\$\s*([\d\.\,]+)\s+R\$\s*([\d\.\,]+)",
        t,
        flags=re.IGNORECASE | re.DOTALL,
    )
    limite_compras = limites.group(1) if limites else None
    limite_saque = limites.group(2) if limites else None

    pag_min = find_first(r"Pagamento mínimo\s*R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)

    # Resumo da fatura (padrão pontilhado)
    saldo_ant = find_first(r"Saldo anterior.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)
    cred_pag = find_first(r"\(\-\)\s*Créditos/Pagamentos.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)
    comp_deb = find_first(r"\(\+\)\s*Compras/Débitos.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)
    total_res = find_first(r"\(=\)\s*Total.*?R\$\s*([\d\.\,]+)", t, flags=re.IGNORECASE | re.DOTALL)

    return BradescoResumo(
        titular=normalize_space(titular) if titular else None,
        produto=normalize_space(produto) if produto else None,
        total_fatura=brl_to_float(total) if total else None,
        vencimento=venc,
        fechamento_proxima=fechamento,
        limite_compras=brl_to_float(limite_compras) if limite_compras else None,
        limite_saque=brl_to_float(limite_saque) if limite_saque else None,
        pagamento_minimo=brl_to_float(pag_min) if pag_min else None,
        saldo_anterior=brl_to_float(saldo_ant) if saldo_ant else None,
        creditos_pagamentos=brl_to_float(cred_pag) if cred_pag else None,
        compras_debitos=brl_to_float(comp_deb) if comp_deb else None,
        total_resumo=brl_to_float(total_res) if total_res else None,
    )


def parse_lancamentos(full_text: str) -> pd.DataFrame:
    lines = [normalize_space(x) for x in full_text.splitlines() if normalize_space(x)]

    # Ex.: "SERGIO MAIA RAULINO Cartão 4066 XXXX XXXX 9953"
    card_ctx_re = re.compile(
        r"^(?P<titular>.+?)\s+Cart[aã]o\s+\d{4}\s+X{4}\s+X{4}\s+(?P<final>\d{4})$",
        re.IGNORECASE,
    )

    # Ex.: "Número do Cartão 4066 XXXX XXXX 9953"
    card_number_re = re.compile(
        r"^N[uú]mero do Cart[aã]o\s+\d{4}\s+X{4}\s+X{4}\s+(?P<final>\d{4})$",
        re.IGNORECASE,
    )

    # estado corrente do bloco (titular/cartão)
    current_titular: Optional[str] = None
    current_card_final: Optional[str] = None

    def maybe_update_ctx(ln: str) -> bool:
        nonlocal current_titular, current_card_final
        s = normalize_space(ln)

        m = card_ctx_re.match(s)
        if not m:
            return False

        raw_titular = normalize_space(m.group("titular"))
        titular_l = raw_titular.lower()

        # ⛔️ evita falso-positivo: "Número do Cartão ..."
        if titular_l in {"número do", "numero do"} or "número do cartão" in titular_l or "numero do cartao" in titular_l:
            return False

        # Reforço: geralmente titular é nome com 2+ palavras
        parts = [p for p in re.split(r"\s+", raw_titular) if p]
        if len(parts) < 2:
            return False

        current_titular = raw_titular.upper()
        current_card_final = m.group("final")
        return True

    rows: List[Dict[str, Any]] = []

    date_prefix = re.compile(r"^(?P<data>\d{2}/\d{2})\s+")

    # pagamento: captura o primeiro valor após "PAGTO" (com '-' opcional)
    pay_re = re.compile(
        r"^(?P<data>\d{2}/\d{2})\s+(?P<desc>PAGTO\..*?)\s+(?P<valor>[\d\.\,]+-?)\b",
        re.IGNORECASE,
    )

    # lançamento genérico (último número no fim da linha)
    txn_re = re.compile(
        r"^(?P<data>\d{2}/\d{2})\s+(?P<rest>.+?)\s+(?P<valor>-?[\d\.\,]+)\s*$"
    )

    ignore_contains = [
        "página ",
        "número do cartão",
        "cotação",
        "data histórico",
        "do dólar",
        "central de atendimento",
        "mensagem importante",
        "programa de fidelidade",
        "pontos acumulados",
        "saldo de pontos",
        "associado",
        "para consultar",
        "juros",
        "iof",
        "valor em r$",
        "taxas mensais",
        "crédito rotativo",
        "pagamento de contas",
        "parcelamento fatura",
        "compras parceladas",
        "total da fatura em real",
        "total da fatura",
        "total utilizado",
        "disponível em",
        "limites",
        "limite de compras",
        "limite de saque",
        "compras r$",
        "saque r$",
    ]

    money_token_re = re.compile(r"\b\d{1,3}(?:\.\d{3})*,\d{2}-?\b")

    def is_ignored(ln: str) -> bool:
        l = ln.lower()
        if not date_prefix.match(ln) and len(ln) < 8:
            return True
        return any(x in l for x in ignore_contains)

    def sanitize_line(ln: str) -> str:
        cuts = [
            " Total para as próximas faturas",
            " Compras R$",
            " Saque R$",
            " Total Utilizado",
            " Disponível em",
            " Taxas mensais",
            " Pagamento de Contas",
            " Parcelamento Fatura",
            " Compras Parceladas",
            " Crédito Rotativo",
        ]
        for c in cuts:
            idx = ln.find(c)
            if idx != -1:
                ln = ln[:idx].strip()
        return ln

    def split_desc_city(rest: str) -> Tuple[str, Optional[str]]:
        tokens = rest.split()
        if len(tokens) < 2:
            return rest, None
        for n in (3, 2, 1):
            if len(tokens) <= n:
                continue
            city_tokens = tokens[-n:]
            desc_tokens = tokens[:-n]
            city = " ".join(city_tokens).strip()
            desc = " ".join(desc_tokens).strip()
            if re.search(r"[A-Za-zÀ-ÿ]", city) and not re.search(r"[\/\*]{1,}", city):
                return desc, city
        return rest, None

    for ln in lines:
        # 0) atualiza contexto de titular/cartão
        if maybe_update_ctx(ln):
            continue

        # 0.1) atualiza apenas o cartão corrente (não mexe em titular)
        mnum = card_number_re.match(normalize_space(ln))
        if mnum:
            current_card_final = mnum.group("final")
            continue

        # 1) ignora lixo
        if is_ignored(ln):
            continue

        # 2) sanitiza antes de casar regex
        ln = sanitize_line(ln)
        if not ln:
            continue

        # 3) pagamento
        mp = pay_re.match(ln)
        if mp:
            raw = mp.group("valor")
            sign = -1 if raw.endswith("-") else 1
            raw_num = raw[:-1] if raw.endswith("-") else raw
            val = brl_to_float(raw_num)

            rows.append(
                {
                    "data": mp.group("data"),
                    "descricao": normalize_space(mp.group("desc")),
                    "cidade": None,
                    "valor": (val * sign) if val is not None else None,
                    "tipo": "pagamento",
                    "titular_cartao": current_titular,
                    "cartao_final": current_card_final,
                }
            )
            continue

        # 4) contaminadas (múltiplos valores na mesma linha)
        if date_prefix.match(ln):
            money_tokens = money_token_re.findall(ln)
            if len(money_tokens) >= 2:
                continue

        # 5) transação normal
        mt = txn_re.match(ln)
        if not mt:
            continue

        data = mt.group("data")
        rest = normalize_space(mt.group("rest"))
        valor = brl_to_float(mt.group("valor"))
        if valor is None:
            continue

        desc, cidade = split_desc_city(rest)

        rows.append(
            {
                "data": data,
                "descricao": desc,
                "cidade": cidade,
                "valor": valor,
                "tipo": "compra" if valor >= 0 else "ajuste",
                "titular_cartao": current_titular,
                "cartao_final": current_card_final,
            }
        )

    return pd.DataFrame(rows).reset_index(drop=True)


def extract_bradesco_bill(pdf_path: str) -> Dict[str, Any]:
    full_text = extract_text(pdf_path)
    resumo = parse_resumo(full_text)
    df = parse_lancamentos(full_text)

    if not df.empty:
        # ---- FIX: preencher titular_cartao faltante por cartao_final (mode) ----
        # Ex.: PAGTO pode vir antes do bloco "<TITULAR> Cartão ...", mas já tem cartao_final.
        if "cartao_final" in df.columns and "titular_cartao" in df.columns:
            df["_titular_fill"] = (
                df.dropna(subset=["cartao_final"])
                .groupby("cartao_final")["titular_cartao"]
                .apply(lambda s: s.dropna().mode().iloc[0] if not s.dropna().empty else None)
            )
            df["titular_cartao"] = df["titular_cartao"].fillna(df["cartao_final"].map(df["_titular_fill"]))
            df.drop(columns=["_titular_fill"], inplace=True, errors="ignore")

        # anexa campos do resumo em todas as linhas
        df["vencimento"] = resumo.vencimento
        df["total_fatura"] = resumo.total_fatura
        df["produto"] = resumo.produto

    return {"resumo": resumo, "lancamentos": df}
