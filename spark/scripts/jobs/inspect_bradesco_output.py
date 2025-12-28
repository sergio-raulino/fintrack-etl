#!/usr/bin/env python3
# scripts/inspect_bradesco_output.py

from __future__ import annotations

import argparse
import os
from typing import Optional, Dict, Any, List

import pandas as pd

# Ajuste este import se necessário
from fintrack_etl.extractors.bradesco_bill import extract_bradesco_bill


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _as_str(x) -> str:
    return "(null)" if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == "") else str(x)


def compute_mode_titular_by_cartao(df: pd.DataFrame) -> pd.Series:
    """
    Retorna um Series indexado por cartao_final com o mode (mais frequente) de titular_cartao.
    - Se houver empate, o pandas.mode() retorna múltiplos; aqui pegamos o primeiro (ordem do pandas).
    - Se não existir nenhum titular válido para um cartao_final, retorna NaN/None.
    """
    if df.empty or "cartao_final" not in df.columns or "titular_cartao" not in df.columns:
        return pd.Series(dtype="object")

    def _mode_or_none(s: pd.Series) -> Optional[str]:
        s2 = s.dropna()
        if s2.empty:
            return None
        m = s2.mode()
        if m.empty:
            return None
        return str(m.iloc[0])

    out = (
        df.dropna(subset=["cartao_final"])
        .groupby("cartao_final")["titular_cartao"]
        .apply(_mode_or_none)
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspeciona saída do extractor Bradesco")
    parser.add_argument(
        "--pdf",
        default="data/processed/movimentacoes_25_11/fatura_bradesco__fatura_bradesco/fatura_bradesco.pdf",
        help="Caminho do PDF da fatura",
    )
    parser.add_argument("--show", type=int, default=40, help="Qtd de linhas para HEAD/Tail")
    parser.add_argument(
        "--expected-finals",
        default="0039,9952,9953",
        help="Finais esperados (csv), ex: 0039,9952,9953",
    )
    parser.add_argument(
        "--show-null-titular",
        type=int,
        default=30,
        help="Máx de linhas com titular_cartao nulo para exibir",
    )
    args = parser.parse_args()

    pdf_path = args.pdf
    expected = [x.strip() for x in (args.expected_finals or "").split(",") if x.strip()]

    _print_header("0) Arquivo analisado")
    if not os.path.exists(pdf_path):
        print(f"❌ PDF não encontrado: {pdf_path}")
        raise SystemExit(2)
    print(f"✅ PDF: {pdf_path}")

    out: Dict[str, Any] = extract_bradesco_bill(pdf_path)
    resumo = out["resumo"]
    df: pd.DataFrame = out["lancamentos"]

    _print_header("1) Resumo extraído")
    # imprime dataclass de forma estável
    for k in [
        "titular",
        "produto",
        "total_fatura",
        "vencimento",
        "fechamento_proxima",
        "limite_compras",
        "limite_saque",
        "pagamento_minimo",
        "saldo_anterior",
        "creditos_pagamentos",
        "compras_debitos",
        "total_resumo",
    ]:
        v = getattr(resumo, k, None)
        print(f"{k:22}: {_as_str(v)}")

    _print_header("2) DataFrame de lançamentos")
    print(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")
    print(f"Colunas: {list(df.columns)}")

    _print_header("3) Contagem por cartao_final (e titular_cartao)")

    if df.empty:
        print("⚠️ DataFrame vazio.")
        return

    # contagem por cartao_final
    if "cartao_final" in df.columns:
        cnt = df["cartao_final"].fillna("(null)").value_counts()
        print("\nContagem (linhas) por cartao_final:")
        print(cnt.to_string())

        present = sorted([x for x in df["cartao_final"].dropna().unique().tolist() if isinstance(x, str)])
        print("\nValidação de finais esperados:")
        print(f"Esperados: {expected}")
        print(f"Presentes : {present}")
        if expected:
            missing = [x for x in expected if x not in present]
            if not missing:
                print(f"✅ OK: todos os finais esperados foram capturados ({' / '.join(expected[::-1])}).")
            else:
                print(f"❌ Faltando finais: {missing}")

    # contagem por cartao_final + titular_cartao
    if {"cartao_final", "titular_cartao"}.issubset(df.columns):
        tmp = df.copy()
        tmp["cartao_final"] = tmp["cartao_final"].fillna("(null)")
        tmp["titular_cartao"] = tmp["titular_cartao"].fillna("(null)")
        cnt2 = tmp.groupby(["cartao_final", "titular_cartao"]).size().sort_values(ascending=False)
        print("\nContagem por cartao_final + titular_cartao:")
        print(cnt2.to_string())

    # ---- NOVO: mode por cartão ----
    _print_header("3.1) Mode de titular_cartao por cartao_final (calculado)")
    mode_map = compute_mode_titular_by_cartao(df)
    if mode_map.empty:
        print("⚠️ Não foi possível calcular mode (colunas ausentes ou df vazio).")
    else:
        # monta tabela amigável
        t = (
            mode_map.rename("titular_mode")
            .reset_index()
            .rename(columns={"index": "cartao_final"})
            .sort_values("cartao_final")
        )
        # conta quantas linhas têm titular nulo em cada cartão (para debugar)
        null_cnt = (
            df.assign(_null=df["titular_cartao"].isna() if "titular_cartao" in df.columns else True)
            .groupby("cartao_final")["_null"]
            .sum()
            .reset_index()
            .rename(columns={"_null": "qtd_titular_null"})
        )
        merged = t.merge(null_cnt, on="cartao_final", how="left")
        print(merged.to_string(index=False))

    # ---- NOVO: linhas com titular_cartao nulo ----
    _print_header("3.2) Linhas com titular_cartao nulo (se houver)")
    if "titular_cartao" not in df.columns:
        print("⚠️ Coluna titular_cartao não existe no DataFrame.")
    else:
        null_rows = df[df["titular_cartao"].isna()].copy()
        if null_rows.empty:
            print("✅ Não há linhas com titular_cartao nulo.")
        else:
            print(f"⚠️ Existem {len(null_rows)} linhas com titular_cartao nulo.")
            cols_pref = [c for c in ["data", "descricao", "cidade", "valor", "tipo", "cartao_final", "titular_cartao"] if c in null_rows.columns]
            show_n = max(1, int(args.show_null_titular))
            print(null_rows[cols_pref].head(show_n).to_string(index=False))
            if len(null_rows) > show_n:
                print(f"... (mostrando {show_n} de {len(null_rows)})")

    _print_header("4) HEAD / TAIL")
    show = max(1, int(args.show))
    print("\nHEAD:")
    print(df.head(show).to_string(index=False))
    print("\nTAIL:")
    print(df.tail(show).to_string(index=False))

    _print_header("5) Resumo estatístico da coluna valor")
    if "valor" in df.columns:
        print(df["valor"].describe().to_string())
    else:
        print("⚠️ Coluna valor não existe no DataFrame.")

    # amostra por cartão
    _print_header("7) Amostra por cartão (primeiras 5 por cartao_final)")
    if "cartao_final" in df.columns:
        for cf, g in df.groupby("cartao_final", dropna=False):
            label = "(null)" if pd.isna(cf) else str(cf)
            print(f"\n--- cartao_final = {label} | linhas = {len(g)} ---")
            cols = [c for c in ["data", "descricao", "cidade", "valor", "tipo", "titular_cartao", "cartao_final"] if c in g.columns]
            print(g[cols].head(5).to_string(index=False))

    print("\n✅ Inspeção concluída.")


if __name__ == "__main__":
    main()
