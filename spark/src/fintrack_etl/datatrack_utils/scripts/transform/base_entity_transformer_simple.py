from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession

from datatrack_utils.scripts.transform.base_entity_transformer import (
    BaseEntityTransformer,
    TipoParticionamento,
)


class BaseEntityTransformerSimple(BaseEntityTransformer):
    """
    Transformer de passagem (no-op):
    - Se usado via SUBCLASSE: define atributos em nível de classe
      (entity_name, inputs, primary_key, coluna_particao, tipo_particionamento)
    - Se usado direto (fallback): passa tudo via __init__

    Em ambos os casos, transform() só devolve o DF do primeiro input.
    """

    # opcionais como atributos de classe (para subclasses)
    entity_name: Optional[str] = None
    inputs: Optional[List[str]] = None
    primary_key: List[str] = []
    coluna_particao: Optional[str] = None
    tipo_particionamento: Optional[TipoParticionamento] = None

    def __init__(
        self,
        spark: SparkSession,
        entity_name: Optional[str] = None,
        inputs: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        coluna_particao: Optional[str] = None,
        tipo_particionamento: Optional[TipoParticionamento] = None,
    ):
        super().__init__(spark)

        # Prioriza o que vier no __init__; se vier None, usa atributo de classe
        if entity_name is not None:
            self.entity_name = entity_name
        if inputs is not None:
            self.inputs = inputs
        if primary_key is not None:
            self.primary_key = primary_key
        if coluna_particao is not None:
            self.coluna_particao = coluna_particao
        if tipo_particionamento is not None:
            self.tipo_particionamento = tipo_particionamento

        if not self.entity_name:
            raise ValueError(
                f"{self.__class__.__name__} precisa de 'entity_name' "
                f"(via atributo de classe ou parâmetro do __init__)."
            )
        if not self.inputs:
            raise ValueError(
                f"{self.__class__.__name__} precisa de 'inputs' "
                f"(via atributo de classe ou parâmetro do __init__)."
            )

    def transform(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        input_principal = self.inputs[0]
        return dfs[input_principal]
