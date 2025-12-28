from datetime import datetime
import os
import json
from typing import List, Optional
from pyspark.sql import SparkSession
import math


def _monta_particao(
    base: str,
    nome_tabela: str,
    ingestao_year: int,
    ingestao_month: str,
    ingestao_day: str,
) -> str:
    """
    s3a://<bucket>/<origin>/<instance>/<schema>/<tabela>/ingestao_year=YYYY/ingestao_month=MM/ingestao_day=DD

    Ex.: para SharePoint:
      base = s3a://tjce-lnd-adm/sp/list/orcamento_financas
      nome_tabela = "orcamento_financas" ou "contratos"
    """
    return os.path.join(
        base.rstrip("/"),
        nome_tabela,
        f"ingestao_year={ingestao_year}",
        f"ingestao_month={ingestao_month}",
        f"ingestao_day={ingestao_day}",
    )


def _list_status(spark: SparkSession, path: str):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    p = Path(path)
    fs = p.getFileSystem(hconf)
    return fs, p, fs.listStatus(p)


def _rename_part_to_single(spark: SparkSession, dest_dir: str, final_filename: str) -> None:
    """
    Renomeia o ÚNICO part-*.gz do diretório para <final_filename>.
    Usado para o array bruto (um arquivo).
    """
    fs, dir_path, statuses = _list_status(spark, dest_dir)
    Path = spark._jvm.org.apache.hadoop.fs.Path

    for st in statuses:
        name = st.getPath().getName()
        if name.startswith("part-") and name.endswith(".gz"):
            src = st.getPath()
            dst = Path(dir_path, final_filename)
            if fs.exists(dst):
                fs.delete(dst, True)
            fs.rename(src, dst)
            return
    raise FileNotFoundError(f"part-*.gz não encontrado em {dest_dir}")


def _rename_parts_to_shards_jsonl(spark: SparkSession, dest_dir: str) -> None:
    """
    Renomeia TODOS os part-*.gz do diretório para:
        shard-00000.jsonl.gz, shard-00001.jsonl.gz, ...

    Ou seja, arquivos de JSON Lines (1 registro JSON por linha),
    comprimidos com gzip.
    """
    fs, dir_path, statuses = _list_status(spark, dest_dir)
    Path = spark._jvm.org.apache.hadoop.fs.Path

    idx = 0
    found = False
    # ordena por nome para ter determinismo
    names = sorted([st.getPath().getName() for st in statuses])
    for name in names:
        if name.startswith("part-") and name.endswith(".gz"):
            found = True
            src = Path(dir_path, name)
            dst_name = f"shard-{idx:05d}.jsonl.gz"
            dst = Path(dir_path, dst_name)
            if fs.exists(dst):
                fs.delete(dst, True)
            fs.rename(src, dst)
            idx += 1

    if not found:
        raise FileNotFoundError(f"Nenhum part-*.gz encontrado para renomear em {dest_dir}")


def incluir_data_ingestao(registros_batch, dt_ingestao_dados: datetime):
    dt_ingestao_json = dt_ingestao_dados.isoformat(timespec="seconds")
    registros_enriquecidos: List[dict] = []
    for row in registros_batch:
        if isinstance(row, dict):
            row = dict(row)
            row["dt_ingestao_dados"] = dt_ingestao_json
        registros_enriquecidos.append(row)
    return registros_enriquecidos


# ============================ Public API ============================

def salvar_jsonl_batch(
    spark: SparkSession,
    nome_tabela: str,
    registros: List[dict],
    caminho_base: str,
    dt_ingestao_dados: Optional[datetime] = None,
):
    """
    Emite DOIS artefatos na landing (sem catálogo):

    1) JSON Lines processável:
       <base>/<tabela>/ingestao_*/shard-00000.jsonl.gz
       (1 registro JSON por linha, gzip)

    2) Array bruto para auditoria:
       <base>/<tabela>/ingestao_*/raw_payload/response.json.gz
       (um ÚNICO array JSON, gzip)

    Observações:
    - Não aplica normalização nem schema.
    - Usa Spark para escrever em s3a e Hadoop FS para renomear part-*.gz.
    - Para SharePoint, basta passar nome_tabela = "orcamento_financas" ou "contratos"
      que as duas pastas são criadas naturalmente.
    """
    if not registros:
        print(f"⚠️ Sem dados para {nome_tabela}")
        return

    if dt_ingestao_dados is None:
        dt_ingestao_dados = datetime.now()

    ingestao_year = dt_ingestao_dados.year
    ingestao_month = f"{dt_ingestao_dados.month:02d}"
    ingestao_day = f"{dt_ingestao_dados.day:02d}"

    # Incluir campo dt_ingestao_dados
    registros = incluir_data_ingestao(registros, dt_ingestao_dados)

    if ingestao_year is None or ingestao_month is None or ingestao_day is None:
        raise ValueError("ingestao_year, ingestao_month e ingestao_day são obrigatórios para a landing.")

    # Garante lista
    if isinstance(registros, dict):
        registros = [registros]

    # ---------------------------
    # 1) JSON Lines (processável)
    # ---------------------------
    json_lines = [json.dumps(r, ensure_ascii=False) for r in registros]
    rdd = spark.sparkContext.parallelize(json_lines)
    df_jsonl = spark.createDataFrame(rdd.map(lambda x: (x,)), ["value"])

    destino_particao = _monta_particao(
        base=caminho_base,
        nome_tabela=nome_tabela,
        ingestao_year=ingestao_year,
        ingestao_month=ingestao_month,
        ingestao_day=ingestao_day,
    )

    # 1) Estima bytes médios por registro (em memória)
    sample_n = min(len(json_lines), 2000)
    avg_rec_bytes = (
        sum(len(s.encode("utf-8")) for s in json_lines[:sample_n]) / max(sample_n, 1)
    ) or 1.0
    total_bytes = avg_rec_bytes * len(json_lines)

    # 2) Define alvo de shard comprimido (~128MB por shard)
    TARGET_MB = 128
    TARGET_BYTES = TARGET_MB * 1024 * 1024
    compression_factor = 2.0  # aproximação do ganho de gzip
    estimated_gz_bytes = total_bytes / compression_factor

    n_shards = max(1, math.ceil(estimated_gz_bytes / TARGET_BYTES))

    (
        df_jsonl
        .repartition(n_shards)   # controla quantos arquivos saem
        .write
        .mode("append")
        .option("compression", "gzip")
        .text(destino_particao)
    )

    # Renomeia part-*.gz -> shard-00000.jsonl.gz, shard-00001.jsonl.gz, ...
    _rename_parts_to_shards_jsonl(spark, destino_particao)

    # ---------------------------------
    # 2) Array bruto (auditoria fiel)
    # ---------------------------------
    json_array = json.dumps(registros, ensure_ascii=False)
    df_array = spark.createDataFrame([(json_array,)], ["value"])

    destino_raw_payload = os.path.join(destino_particao, "raw_payload")

    (
        df_array.coalesce(1)  # força um único arquivo para o array
        .write
        .mode("append")       # append, mesmo se reprocessar o mesmo dia
        .option("compression", "gzip")
        .text(destino_raw_payload)
    )

    _rename_part_to_single(spark, destino_raw_payload, "response.json.gz")

    print(f"✅ JSON Lines gravado em: {destino_particao}")
    print(f"✅ Array bruto gravado em: {os.path.join(destino_raw_payload, 'response.json.gz')}")
