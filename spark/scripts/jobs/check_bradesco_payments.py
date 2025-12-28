#!/usr/bin/env python3
from pathlib import Path
import pandas as pd

CSV = Path("data/processed/movimentacoes_25_11/fatura_bradesco__fatura_bradesco/fatura_lancamentos.csv")

def main():
    df = pd.read_csv(CSV)
    pay = df[df["descricao"].astype(str).str.contains("PAGTO", case=False, na=False)]
    print(f"CSV: {CSV}")
    print(f"Linhas: {len(df)} | Pagamentos: {len(pay)}")
    if len(pay):
        print(pay.sort_values("valor").to_string(index=False))

if __name__ == "__main__":
    main()
