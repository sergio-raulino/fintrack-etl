#!/usr/bin/env python3
import os
import argparse
import pandas as pd


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", default="./data/processed/bb_fatura/fatura_lancamentos.csv")
    ap.add_argument("--out-dir", default="./data/processed/bb_fatura")
    ap.add_argument("--topn", type=int, default=15)
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)

    if df.empty:
        print("Arquivo vazio:", args.in_csv)
        return

    safe_mkdir(args.out_dir)

    # Normaliza
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df["parcelado_suspeito"] = df.get("parcelado_suspeito", False).fillna(False)
    df["recorrente_suspeita"] = df.get("recorrente_suspeita", False).fillna(False)

    # 1) Totais por categoria
    by_cat = (
        df.groupby("categoria")["valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"valor": "total"})
    )
    by_cat.to_csv(os.path.join(args.out_dir, "monthly_by_category.csv"), index=False)

    # 2) Totais por categoria + parcelado
    by_cat_parc = (
        df.groupby(["categoria", "parcelado_suspeito"])["valor"]
        .sum()
        .reset_index()
        .rename(columns={"valor": "total"})
        .sort_values(["categoria", "parcelado_suspeito"], ascending=[True, False])
    )
    by_cat_parc.to_csv(os.path.join(args.out_dir, "monthly_by_category_parcelado.csv"), index=False)

    # 3) Total recorrente suspeito
    recorr = (
        df[df["recorrente_suspeita"] == True]
        .groupby("categoria")["valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"valor": "total_recorrente_suspeito"})
    )
    recorr.to_csv(os.path.join(args.out_dir, "monthly_recorrentes.csv"), index=False)

    # 4) Top gastos (por valor absoluto)
    df["valor_abs"] = df["valor"].abs()
    top = df.sort_values("valor_abs", ascending=False).head(args.topn)[
        ["data", "descricao", "categoria", "subcategoria", "valor", "parcelado_suspeito", "recorrente_suspeita"]
    ]
    top.to_csv(os.path.join(args.out_dir, "top_gastos.csv"), index=False)

    # 5) Top parcelados
    top_parc = df[df["parcelado_suspeito"] == True].sort_values("valor_abs", ascending=False).head(args.topn)[
        ["data", "descricao", "categoria", "subcategoria", "valor"]
    ]
    top_parc.to_csv(os.path.join(args.out_dir, "top_parcelados.csv"), index=False)

    print("\nOK ✅ Relatórios gerados em:", args.out_dir)
    print(" - monthly_by_category.csv")
    print(" - monthly_by_category_parcelado.csv")
    print(" - monthly_recorrentes.csv")
    print(" - top_gastos.csv")
    print(" - top_parcelados.csv")


if __name__ == "__main__":
    main()
