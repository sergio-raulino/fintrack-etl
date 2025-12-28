#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse
from typing import Dict, Any

import pandas as pd
import yaml


def load_budget(path: str) -> Dict[str, float]:
    with open(path, "r", encoding="utf-8") as f:
        data: Dict[str, Any] = yaml.safe_load(f) or {}

    budget: Dict[str, float] = {}
    for k, v in data.items():
        if v is None:
            continue
        try:
            budget[str(k)] = float(v)
        except Exception as e:
            raise ValueError(f"Valor inválido no budget.yaml para '{k}': {v!r}") from e

    return budget


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compara realizado x orçamento do FinTrack (mensal).")
    ap.add_argument("--budget", default="./budget.yaml", help="Caminho do budget.yaml")
    ap.add_argument(
        "--actual-csv",
        default="./data/processed/bb_fatura/monthly_by_category.csv",
        help="CSV de realizado (por categoria), gerado pelo monthly_summary.py",
    )
    ap.add_argument("--out-dir", default="./data/processed/bb_fatura", help="Diretório de saída")
    ap.add_argument("--exclude", default="Financeiro", help="Categorias a excluir (separadas por vírgula)")
    ap.add_argument("--currency", default="R$", help="Símbolo de moeda para impressão")
    args = ap.parse_args()

    exclude = {c.strip() for c in (args.exclude or "").split(",") if c.strip()}

    budget = load_budget(args.budget)

    df_act = pd.read_csv(args.actual_csv)
    if df_act.empty:
        raise SystemExit(f"Arquivo de realizado está vazio: {args.actual_csv}")

    # Espera colunas: categoria, total
    if not {"categoria", "total"}.issubset(df_act.columns):
        raise SystemExit(f"CSV de realizado precisa ter colunas 'categoria' e 'total'. Encontrado: {list(df_act.columns)}")

    df_act = df_act.copy()
    df_act["categoria"] = df_act["categoria"].astype(str)
    df_act["total"] = pd.to_numeric(df_act["total"], errors="coerce").fillna(0.0)

    # Excluir categorias não desejadas (ex: Financeiro)
    if exclude:
        df_act = df_act[~df_act["categoria"].isin(exclude)].copy()

    # Monta DataFrame de orçamento
    df_budget = pd.DataFrame([{"categoria": k, "orcado": v} for k, v in budget.items()])

    # Merge orçamento x realizado
    df = df_budget.merge(df_act.rename(columns={"total": "realizado"}), on="categoria", how="outer")

    df["orcado"] = pd.to_numeric(df["orcado"], errors="coerce")
    df["realizado"] = pd.to_numeric(df["realizado"], errors="coerce").fillna(0.0)

    # Onde não tem orçamento (categoria apareceu no realizado mas não existe no budget)
    df["sem_orcamento"] = df["orcado"].isna()

    # Define orçado=0 para categorias sem orçamento (para métricas, mas marcamos sem_orcamento=True)
    df.loc[df["orcado"].isna(), "orcado"] = 0.0

    df["diferenca"] = df["realizado"] - df["orcado"]
    df["status"] = df["diferenca"].apply(lambda x: "OK" if x <= 0 else "ESTOURO")

    # % estouro: se orçado = 0, não faz sentido percentual
    def pct(row) -> float:
        if row["orcado"] <= 0:
            return float("nan")
        return (row["diferenca"] / row["orcado"]) * 100.0

    df["pct_esto"] = df.apply(pct, axis=1)

    # Ordenação por maior estouro (valor absoluto de diferença positiva)
    df["esto_abs"] = df["diferenca"].apply(lambda x: x if x > 0 else 0.0)
    df = df.sort_values(["esto_abs", "realizado"], ascending=[False, False]).drop(columns=["esto_abs"])

    ensure_dir(args.out_dir)

    out_csv = os.path.join(args.out_dir, "budget_vs_actual.csv")
    df.to_csv(out_csv, index=False)

    # Resumo no terminal
    total_orcado = df["orcado"].sum()
    total_real = df["realizado"].sum()
    delta = total_real - total_orcado

    # top 5 estouros
    top_over = df[df["diferenca"] > 0].head(5)

    def fmt_money(x: float) -> str:
        # formatação pt-BR simples sem locale
        s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{args.currency} {s}"

    print("\n=== FinTrack | Orçamento vs Realizado ===")
    print(f"Arquivo de saída: {out_csv}")
    print(f"Total orçado:    {fmt_money(total_orcado)}")
    print(f"Total realizado: {fmt_money(total_real)}")
    print(f"Diferença:       {fmt_money(delta)} ({'ESTOURO' if delta > 0 else 'OK'})")

    sem_orc = df[df["sem_orcamento"] == True]
    if not sem_orc.empty:
        print("\nCategorias sem orçamento (apareceram no realizado mas não estão no budget.yaml):")
        for c in sem_orc["categoria"].tolist():
            print(f" - {c}")

    if not top_over.empty:
        print("\nTop estouros:")
        for _, r in top_over.iterrows():
            pct_txt = "" if pd.isna(r["pct_esto"]) else f" ({r['pct_esto']:.1f}%)"
            print(f" - {r['categoria']}: realizado {fmt_money(r['realizado'])} | orçado {fmt_money(r['orcado'])} | +{fmt_money(r['diferenca'])}{pct_txt}")
    else:
        print("\nNenhum estouro ✅")

    print()


if __name__ == "__main__":
    main()
