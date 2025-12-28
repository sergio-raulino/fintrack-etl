import gzip
import json
from itertools import islice

caminho = "/home/tj.ce.gov.br/22666/Downloads/shard-contratos-00000.jsonl.gz"

with gzip.open(caminho, "rt", encoding="utf-8") as f:
    # mostra alguns exemplos
    for linha in islice(f, 5):
        registro = json.loads(linha)
        print("Chaves:", list(registro.keys()))
        print("Exemplo de registro:", registro)
        print("-" * 80)
