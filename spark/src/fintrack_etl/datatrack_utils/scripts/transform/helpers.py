from typing import Dict, Iterable, Sequence, Optional
from pyspark.sql import DataFrame, functions as F, types as T



# =========================
# Renome e Cast
# =========================
def rename_many(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    out = df
    for old, new in mapping.items():
        if old in out.columns and old != new:
            out = out.withColumnRenamed(old, new)
    return out

def cast_many(df: DataFrame, type_map: Dict[str, T.DataType]) -> DataFrame:
    out = df
    for col, dtype in type_map.items():
        if col in out.columns:
            out = out.withColumn(col, F.col(col).cast(dtype))
    return out

# =========================
# Limpeza básica
# =========================
def null_if_blank(df: DataFrame, cols: Sequence[str]) -> DataFrame:
    """
    Converte "" ou strings só com espaços em NULL para cada coluna em cols.
    """
    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.col(c)))
    return out

# =========================
# Datas e Timestamps
# =========================
_DEFAULT_DATE_PATTERNS = (
    "yyyy-MM-dd",      # 2025-11-12
    "dd/MM/yyyy",      # 12/11/2025
    "yyyyMMdd",        # 20251112
    "ddMMyyyy",        # 12112025
)

_DEFAULT_TS_PATTERNS = (
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy'T'HH:mm:ss",
)

def _coalesce_many(exprs):
    return F.coalesce(*exprs) if exprs else None


def to_date_multi(df: DataFrame, col: str, patterns: Iterable[str] = _DEFAULT_DATE_PATTERNS) -> DataFrame:
    """
    Converte coluna string -> DateType testando múltiplos formatos.
    Mantém o mesmo nome da coluna.
    """
    if col not in df.columns:
        return df
    # limpa espaços e vazios
    out = null_if_blank(df, [col])
    # tenta cada padrão
    exprs = [F.to_date(F.col(col), p) for p in patterns]
    parsed = _coalesce_many(exprs)
    return out.withColumn(col, parsed)

def to_timestamp_multi(df: DataFrame, col: str, patterns: Iterable[str] = _DEFAULT_TS_PATTERNS) -> DataFrame:
    """
    Converte coluna string -> TimestampType testando múltiplos formatos.
    Mantém o mesmo nome da coluna.
    """
    if col not in df.columns:
        return df
    out = null_if_blank(df, [col])
    exprs = [F.to_timestamp(F.col(col), p) for p in patterns]
    parsed = _coalesce_many(exprs)
    return out.withColumn(col, parsed)

def normalize_cpf(df: DataFrame, col: str = "cpf") -> DataFrame:
    """
    Normaliza CPF removendo APENAS '.', '-' e espaços
    e faz left-pad para 11 com '0'. Mantém NULLs.
    """
    if col not in df.columns:
        return df

    base = F.col(col).cast(T.StringType())
    # remove apenas ponto, hífen e espaço
    cleaned = F.regexp_replace(base, r"[.\-\s]", "")
    padded  = F.lpad(cleaned, 11, "0")

    out = F.when(base.isNull(), F.lit(None)).otherwise(padded)
    return df.withColumn(col, out)

def normalize_date_column(
    df: DataFrame,
    col: str,
    patterns: Iterable[str] = _DEFAULT_DATE_PATTERNS
) -> DataFrame:
    """
    Pipeline completo de normalização para datas em string:
    - trim
    - "" -> null
    - tentativas múltiplas de parse
    - mantém o mesmo nome como DateType
    """
    if col not in df.columns:
        return df
    out = df
    out = out.withColumn(col, F.trim(F.col(col)))
    out = null_if_blank(out, [col])
    return to_date_multi(out, col, patterns)

def ensure_partition_date(df: DataFrame, col: str) -> DataFrame:
    """
    Garante que 'col' é DateType para usar com transforms do Iceberg (years/months/days).
    Se for string, tenta parse padrão; se for timestamp, faz cast para date.
    """
    if col not in df.columns:
        return df
    dt = df.schema[col].dataType
    if isinstance(dt, T.DateType):
        return df
    if isinstance(dt, T.StringType):
        return to_date_multi(df, col, _DEFAULT_DATE_PATTERNS)
    if isinstance(dt, T.TimestampType):
        return df.withColumn(col, F.to_date(F.col(col)))
    # outros tipos: converte pra string e tenta parse
    return to_date_multi(df.withColumn(col, F.col(col).cast("string")), col, _DEFAULT_DATE_PATTERNS)
