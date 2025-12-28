from sp_list_client import fetch_columns

def dump_columns(list_key: str, label: str) -> None:
    cols = fetch_columns(list_key)
    for c in cols:
        print(
            f"[{label}] displayName={c.get('displayName')!r} "
            f"internalName={c.get('name')!r} "
            f"type={c.get('columnGroup') or c.get('odata.type')}"
        )

if __name__ == "__main__":
    dump_columns("orcamento_financas", "ORÇAMENTO")
    print("-" * 80)
    dump_columns("contratos", "CONTRATOS")
