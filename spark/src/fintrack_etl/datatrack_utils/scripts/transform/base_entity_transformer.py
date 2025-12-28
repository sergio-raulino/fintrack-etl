from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Dict
from enum import Enum


class TipoParticionamento(Enum):
    ANO = "ANO"
    MES = "MES"
    DIA = "DIA"
    BUCKET = "BUCKET"


class BaseEntityTransformer(ABC):
    """
    Agora suporta múltiplos inputs e um output específico.
    """
    # nome da entidade (por ex: "sap.custodiados")
    entity_name: str = None

    # lista de tabelas que a strategy precisa carregar com o nome completo no catalogo
    inputs: List[str] = []        # exemplo: ["tjce_raw_jud.sap.custodiados", "sap.movimentacoes"]


    # configurações de escrita
    primary_key: List[str] = []
    coluna_particao: Optional[str] = None
    tipo_particionamento: Optional[TipoParticionamento] = None

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # -----------------------------------------
    # Carregar múltiplas tabelas como DataFrames
    # -----------------------------------------
    def load_inputs(self) -> Dict[str, DataFrame]:
        dfs = {}
        for table in self.inputs:
            dfs[table] = self.spark.table(table)
        return dfs

    # -----------------------------------------
    # Pré-transform (default)
    # -----------------------------------------
    def pre_transform(self, dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        return dfs

    # -----------------------------------------
    # Transform principal (AGORA OBRIGATÓRIO)
    # -----------------------------------------
    @abstractmethod
    def transform(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        """
        Implementado por cada strategy.
        Deve retornar APENAS 1 DataFrame final.
        """
        pass

    # -----------------------------------------
    # Pós-transform
    # -----------------------------------------
    def post_transform(self, df: DataFrame) -> DataFrame:
        return df
