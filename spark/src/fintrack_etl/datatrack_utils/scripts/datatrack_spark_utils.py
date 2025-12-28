import math
import logging,sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col
import os
from dotenv import load_dotenv
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
import io,json
from pyspark.sql.types import StructType
import json
from datetime import datetime
from minio import Minio
from pyspark.sql import functions as F
from datatrack_utils.scripts.transform.base_entity_transformer import (
     TipoParticionamento
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


# Em tjce_utils/scripts/datatrack_spark_utils.py
class DataTrackSparkUtils:
    def __init__(self,logger=None):
        self.log = logger or self._criar_logger_padrao()
    
    def _criar_logger_padrao(self):
        import logging, sys
        logger = logging.getLogger("Utils")
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger       


    def get_minio_parameteres(self,minio_parameters)-> dict:
            """Retorna os parâmetros de conexão com o MinIO.
            minio_parameters: str - local do .env dos parametros do minio
            """
            logging.info(f"Carregando configurações do MinIO de: {minio_parameters}")
            load_dotenv(minio_parameters)        
            return {
                "endpoint": os.getenv("HOST_API"),
                "access_key": os.getenv("ACCESS_KEY"),
                "secret_key": os.getenv("SECRET_KEY")
            }
        
        
    def deduplicar_dados(self,df: DataFrame, chaves: list, chave_ordenacao: str) -> DataFrame:
        """
        Funcao que remove duplicadas do dados, mantendo apenas o registro mais recente para cada chave e ordenando com base na dt_ingestao_dados
        df: Dataframe - Dataframe dos dados
        chaves: List - Lista das chaves primarias da tabela
        """
        nome_tabela_temporaria = "tb_dados_spark"
        if not chaves:
            logging.warning("Nenhuma chave primária definida. Não foi possível de-duplicar.")
            return df
        
        df.createOrReplaceTempView(nome_tabela_temporaria)

        string_chaves = ", ".join(chaves)
        string_ordenacao = ''
        
        if chave_ordenacao in df.columns:
            string_ordenacao = f"ORDER BY {chave_ordenacao} DESC"
        else:
            logging.warning("Não foi encontrado a coluna de ordenacao no dataframe para fazer a deduplicacao, nesse caso a deduplicacao sera feita apenas com bases nas chaves")
        
        
        query_sql = f"""
            SELECT
                *
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY {string_chaves} {string_ordenacao}) as row_num_duplicada
                    FROM
                        {nome_tabela_temporaria}
                ) ranked_data
            WHERE
                row_num_duplicada = 1
        """
        
        logging.info(F"Executando query de de-duplicação com base nas chaves: {chaves}")
        df_deduplicado = df.sparkSession.sql(query_sql)
        
        # Remoção coluna de duplicacao do dataframe
        df_final = df_deduplicado.drop("row_num_duplicada") 
        logging.info('Deduplicacao finalizada')
        
        return df_final
    
    def inputs_is_empty(self,dfs_inputs: Dict[str, DataFrame]) -> bool:
        # retorna True se TODOS os inputs estiverem vazios
        return all(df.rdd.isEmpty() for df in dfs_inputs.values())


    def create_table_controle_carga_inputs_trusted(self,spark: SparkSession,tabela_controle_carga: str) -> None:
            """Cria a tabela de controle de carga
                spark: SparkSession - Sessão spark
                tabela_controle_carga: str - Nome da tabela de controle de carga no catalogo
            """
            if not spark.catalog.tableExists(tabela_controle_carga):
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {tabela_controle_carga} (
                        entidade_trusted   STRING,
                        input_table        STRING,
                        last_dt_ingestao   TIMESTAMP,
                        last_run           TIMESTAMP
                    )
                    USING iceberg
                """)        

    def get_ultima_carga_input_controle_carga_trusted(
        self,
        spark: SparkSession,
        entidade_trusted: str,
        inputs: List[str],
        tabela_controle_carga: str,
    ) -> Dict[str, Optional[datetime]]:
        """
        Retorna um dict: { input_table -> last_dt_ingestao } para a entidade_trusted.
        Se não houver registro, não retorna chave ou retorna None.
        """
        if not spark.catalog.tableExists(tabela_controle_carga):
            return {}

        df_ctrl = (
            spark.table(tabela_controle_carga)
            .filter(F.col("entidade_trusted") == entidade_trusted)
            .filter(F.col("input_table").isin(inputs))
            .groupBy("input_table")
            .agg(F.max("last_dt_ingestao").alias("last_dt_ingestao"))
        )

        ultimas_cargas: Dict[str, Optional[datetime]] = {}
        for row in df_ctrl.collect():
            ultimas_cargas[row["input_table"]] = row["last_dt_ingestao"]

        return ultimas_cargas
    
    def carregar_inputs_incremental_controle_carga(
            self,
            spark: SparkSession,
            inputs: List[str],
            ultimas_cargas: Dict[str, Optional[datetime]],
            coluna_incremental: str = "dt_ingestao_dados",
    ):
        """
        Carrega inputs de forma incremental com base no controle de carga por tabela da trusted.

        Params:
            spark           : SparkSession
            inputs          : lista de nomes de tabelas (fully qualified)
            ultimas_cargas      : dict { input_table -> last_dt_ingestao } (pode ter None)
            coluna_incremental : nome da coluna de data/hora usada como incremental

        Return:
            dfs_inputs      : dict { input_table -> DataFrame filtrado }
            input_max_dt    : dict { input_table -> max(coluna_incremental)}
        """
        dfs_inputs: Dict[str, DataFrame] = {}
        input_max_data_cargas: Dict[str, Optional[datetime]] = {}
        logging.info(f"Inputs de dependencia da carga: {inputs}")
        for input_table in inputs:
            df_raw = spark.table(input_table)

            ultima_carga = ultimas_cargas.get(input_table)
            if ultima_carga:
                logging.info(
                    f"Lendo dados do Input: {input_table} apartir da ultima carga da {coluna_incremental} > {ultima_carga}"
                )
                df_incremental = df_raw.filter(F.col(coluna_incremental) > F.lit(ultima_carga))
            else:
                logging.info(f"Lendo dados full do input {input_table}")
                df_incremental = df_raw

            dfs_inputs[input_table] = df_incremental

            # calcula o max(coluna_incremental) consumido deste input nessa rodada
            maxima_data_carga = df_incremental.select(F.max(coluna_incremental).alias("ult_data_carga")).first()
            input_max_data_cargas[input_table] = maxima_data_carga["ult_data_carga"]

        return dfs_inputs, input_max_data_cargas 



    def update_ultima_carga_input_controle_carga_trusted(
        self,
        spark,
        entidade_trusted: str,
        input_max_dt: Dict[str, Optional[datetime]],
        tabela_controle_carga: str,
    ) -> None:
        rows = []
        now = datetime.now()

        for input_table, max_dt in input_max_dt.items():
            rows.append((entidade_trusted, input_table, max_dt, now))

        if not rows:
            return

        schema = StructType([
            StructField("entidade_trusted", StringType(), False),
            StructField("input_table",      StringType(), False),
            StructField("last_dt_ingestao", TimestampType(), True),
            StructField("last_run",         TimestampType(), False),
        ])

        df = spark.createDataFrame(rows, schema=schema)
        df.writeTo(tabela_controle_carga).append()

        logging.info("Controle de Carga Atualizado")


    def evoluir_schema(
        self,
        spark: SparkSession,
        df_fonte: DataFrame,
        nome_tabela_destino: str
    ) -> DataFrame:
        """
        Compara o schema de um DataFrame de fonte com uma tabela de destino,
        evolui o schema da tabela de destino adicionando as colunas novas que surgirem, 
        se necessário, e retorna um DataFrame da fonte com o schema alinhado, pronto para ser escrito.

        - spark (SparkSession): A sessão Spark.
        - df_fonte (DataFrame): O DataFrame com os novos dados (origem).
        - nome_tabela_destino (str): O nome completo da tabela de destino (ex: 'catalogo.schema.tabela').

        Retorno:
            DataFrame: O DataFrame de fonte com o schema ajustado para corresponder ao do destino.
        """
        # Se a tabela de destino não existe, não há schema para evoluir. Retorna o DF original.
        if not spark.catalog.tableExists(nome_tabela_destino):
            logging.warning(
                f"Tabela de destino '{nome_tabela_destino}' não existe. "
                "Nenhuma evolução de schema necessária."
            )
            return df_fonte

        # Pega os schemas da fonte e do destino
        df_destino = spark.table(nome_tabela_destino)
        colunas_fonte = set(df_fonte.columns)
        colunas_destino = set(df_destino.columns)

        # -----------------------------------------
        # 1) Adicionar colunas novas no destino
        # -----------------------------------------
        colunas_a_adicionar = colunas_fonte - colunas_destino
        if colunas_a_adicionar:
            logging.info(
                f"Evoluindo schema: Adicionando colunas novas ao destino: {colunas_a_adicionar}"
            )

            for col_name in sorted(list(colunas_a_adicionar)):
                field = df_fonte.schema[col_name]
                type_string = field.dataType.simpleString()

                alter_query = (
                    f"ALTER TABLE {nome_tabela_destino} "
                    f"ADD COLUMN `{col_name}` {type_string}"
                )
                spark.sql(alter_query)

            logging.info("Schema da tabela de destino atualizado.")

            # Recarrega a definição do DataFrame de destino para refletir o novo schema
            df_destino = spark.table(nome_tabela_destino)
            colunas_destino = set(df_destino.columns)

        # -----------------------------------------
        # 2) Preencher na fonte as colunas que existem no destino e não existem na fonte
        # -----------------------------------------
        colunas_a_preencher = colunas_destino - colunas_fonte
        df_fonte_ajustado = df_fonte

        if colunas_a_preencher:
            logging.info(
                f"Alinhando schema da fonte: Adicionando colunas faltantes com NULL: "
                f"{colunas_a_preencher}"
            )
            for col_name in sorted(list(colunas_a_preencher)):
                tipo_coluna = df_destino.schema[col_name].dataType
                df_fonte_ajustado = df_fonte_ajustado.withColumn(
                    col_name,
                    lit(None).cast(tipo_coluna),
                )

        # -----------------------------------------
        # 3) Reordenar colunas da fonte na mesma ordem do destino
        # -----------------------------------------
        ordem_colunas_destino = df_destino.columns
        df_fonte_final = df_fonte_ajustado.select(*ordem_colunas_destino)

        logging.info("Evolução e alinhamento concluídos.")
        return df_fonte_final



    def mesclar_dados(self,spark: SparkSession, df: DataFrame, nome_tabela: str, chaves: list, evolucao_schema : bool = True):
        """
        Funcao que realizar merge dos dados
        spark: SparkSession - Sessão do Spark
        df: Dataframe - Dataframe dos dados
        nome_tabela: Str - nome da tabela de destino no catalogo do hive ex:(catalog.schema.nome_tabela)
        chaves: List - Lista das chaves primarias da tabela para a mesclagem 
        evoluir_schema: bool - Se True o schema de destino sera evoluido, default(True)
        """
        
        if not chaves:
            raise ValueError("Error: Chaves da tabela são necessaria para executar o merge!")
        
        try:
            if spark.catalog.tableExists(nome_tabela):
 
                                    
                # Se evolução
                if evolucao_schema:
                    logging.info(f"Executando Merge e Evolução de Schema")  
                    df = self.evoluir_schema(spark,df,nome_tabela)
                else:
                    logging.info(f"Executando Merge.")  

                df.createOrReplaceTempView("novos_dados_para_merge")
                
                # Condições de JOIN 
                condicao_join = " AND ".join([f"destino.{key} = origem.{key}" for key in chaves])

                # Condições de UPDATE 
                colunas_para_update = [f"destino.{col} = origem.{col}" for col in df.columns if col not in chaves]
                string_update = ", ".join(colunas_para_update)

                # Condições de INSERT
                lista_colunas = ", ".join(df.columns)
                lista_valores_origem = ", ".join([f"origem.{col}" for col in df.columns])

                merge_query = f"""
                    MERGE INTO {nome_tabela} AS destino
                    USING novos_dados_para_merge AS origem
                    ON {condicao_join}
                    WHEN MATCHED 
                    AND origem.dt_ingestao_dados > destino.dt_ingestao_dados THEN
                        UPDATE SET {string_update}
                    WHEN NOT MATCHED THEN
                        INSERT ({lista_colunas}) VALUES ({lista_valores_origem})
                """      
                # Evoluir Schema com inclusao de 0 registros                     
                df.limit(0).writeTo(nome_tabela).option("merge-schema", "true").append()
                spark.sql(merge_query)
                logging.info(f"MERGE para a tabela '{nome_tabela}' concluído.")
            else:
                logging.warning(f"Merge não executado, pois a tabela {nome_tabela}' não existe.")

        except Exception as e:
            logging.error(f"Falha na etapa de merge da tabela: '{nome_tabela}'. Erro: {e}", exc_info=True)
            raise

        
    def schema_to_ddl(self,schema: StructType) -> str:
        """
        Converte um schema de DataFrame para uma string DDL, garantindo
        compatibilidade entre diferentes versões do PySpark 3.
        """
        ddl_parts = []
        for field in schema.fields:      
            ddl_parts.append(f"`{field.name}` {field.dataType.simpleString()}")
            
        return ", ".join(ddl_parts)

    def create_table_iceberg_particao(self,spark:SparkSession, df:DataFrame, nome_tabela_catalogo,location_s3,
                                        coluna_particao,tipo_particionamento,tamanho_particao_mb: int = 256):
            """Realiza a criação de uma tabela iceberg no catalogo da tabela juntamente com seu schema
            spark: SparkSession - Sessão do Spark
            df: Dataframe - Dataframe dos dados
            nome_tabela_catalogo: Str - nome da tabela no catalogo do hive na formatacao(catalog.schema.nome_tabela)   
            location_s3 : Str - Local de salvamento dos dados no iceberg no S3/Minio
            coluna_particao : str - Coluna usada para particionar a tabela
            tipo_particionamento: TipoParticionamento - Enum dos tipos de particionamentos
            tamanho_particao_mb : Int - Tamanho de cada arquivo de escrita em megabyte, default(256 mb)
            """
            logging.info(f"Criando Tabela {nome_tabela_catalogo} Caminho >> {location_s3}")
            
            prefixos_tabela = nome_tabela_catalogo.split('.')
            
            if len(prefixos_tabela) != 3:
                raise ValueError(f"O nome da tabela '{nome_tabela_catalogo}' é inválido, deve esta no padrão'catalogo.schema.tabela'.")  
            catalogo, schema, _ = prefixos_tabela         
            location_schema_s3 = "/".join(location_s3.split('/')[:-1])
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema} LOCATION '{location_schema_s3}'")
            

            tamanho_estimado_df_mb = self.estimar_tamanho_df(df)
            qtd_particoes_ideais = self.calcular_particoes_ideais(
                tamanho_estimado_df_mb, tamanho_particao_mb
            )
            # Tamanho dos ficheiros parquet em bytes
            tamanho_ficheiro_em_bytes = tamanho_particao_mb * 1024 * 1024
            
            logging.info("Iniciando Escrita dos Dados")
            if coluna_particao:
                writer = (
                    df.writeTo(nome_tabela_catalogo)
                    .tableProperty("location", location_s3)
                    .tableProperty(
                        "write.target-file-size-bytes",
                        str(tamanho_ficheiro_em_bytes),
                    )
                )

                writer = self.definir_particionamento(
                    writer,
                    df,
                    coluna_particao,
                    tipo_particionamento,
                    tamanho_particao_mb,
                    tamanho_estimado_df_mb
                )
                writer.createOrReplace()
            else:
                df_final = df.repartition(qtd_particoes_ideais)
                df_final.writeTo(nome_tabela_catalogo) \
                    .tableProperty("location", location_s3) \
                    .tableProperty(
                        "write.target-file-size-bytes",
                        str(tamanho_ficheiro_em_bytes),
                    ) \
                    .createOrReplace()  

    def create_table_iceberg(self,spark: SparkSession, df: DataFrame, nome_tabela: str,location_raw_s3: str, colunas_particao: list, tamanho_ficheiro_tabela_mb: int = 256):
        """Realiza a criação de uma tabela iceberg
        spark: SparkSession - Sessão do Spark
        df: Dataframe - Dataframe dos dados
        nome_tabela: Str - nome da tabela no catalogo do hive na formatacao(catalog.schema.nome_tabela)   
        location_raw_s3 : Str - Local de salvamento dos dados no iceberg
        colunas_particao : List - Lista das colunas usadas para particionar a tabela
        tamanho_arquivo_tabela : Int - Tamanho de cada arquivo de escrita em megabyte, default(256 mb)
        """
        prefixos_tabela = nome_tabela.split('.')
        if len(prefixos_tabela) != 3:
            raise ValueError(f"O nome da tabela '{nome_tabela}' é inválido, deve esta no padrão'catalogo.schema.tabela'.")  
        catalogo, schema, _ = prefixos_tabela         
        location_schema_s3 = "/".join(location_raw_s3.split('/')[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema} LOCATION '{location_schema_s3}'")
        
        # Tamanho dos ficheiros parquet em bytes
        tamanho_ficheiro_em_bytes = tamanho_ficheiro_tabela_mb * 1024 * 1024
        
        try:
            if not spark.catalog.tableExists(nome_tabela):    
                logging.info(f"A tabela '{nome_tabela}' não existe. Criando a tabela pela primeira vez.")       

                schema_ddl_string =  self.schema_to_ddl(df.schema)
                
                particao_spec_sql = ""
                if colunas_particao:
                    particao_str = ", ".join(colunas_particao)
                    particao_spec_sql = f"PARTITIONED BY ({particao_str})"
        
                create_table = f"""
                    CREATE TABLE {nome_tabela} ({schema_ddl_string})
                    USING iceberg
                    LOCATION '{location_raw_s3}'
                    TBLPROPERTIES ('write.target-file-size-bytes' = '{tamanho_ficheiro_em_bytes}')
                    {particao_spec_sql}
                """            
                
                spark.sql(create_table)   
                df.writeTo(nome_tabela).append()
                                    
                logging.info(f"Tabela '{nome_tabela}' criada com sucesso no {catalogo}.{schema} ")
            else:
                logging.warning(f"A tabela '{nome_tabela}' ja existe")  
        except Exception as e:
            logging.error(f"Falha na etapa de criação da tabela: '{nome_tabela}'. Erro: {e}", exc_info=True)
            raise

        
        
    def tempo_execucao(self,inicio):
        elapsed_time_minutes = (time.time() - inicio) / 60
        logging.info("Processo de carga finalizado.")
        logging.info(f"Tempo Total: {elapsed_time_minutes:.3f} minutos")
        
        
                
    def criar_colunas_tempo_ingestao(self,df,*partition_cols_for_write):
        if not partition_cols_for_write:
            partition_cols_for_write = ["dt_ingestao_dados", "ingestao_year", "ingestao_month", "ingestao_day"]
        ingestion_date = datetime.now()
        for part_col in partition_cols_for_write:
            if part_col not in df.columns:
                logging.warning(f"A coluna de partição '{part_col}' não foi encontrada na tabela de origem. "
                                f"Criando-a com base na data/hora atual.")
                if part_col == 'ingestao_year':
                    df = df.withColumn(part_col,lit(ingestion_date.year))
                elif part_col == 'ingestao_month':
                    df = df.withColumn(part_col, lit(ingestion_date.month))
                elif part_col == 'ingestao_day':
                    df = df.withColumn(part_col, lit(ingestion_date.day))
                elif part_col == 'dt_ingestao_dados':
                    df = df.withColumn(part_col, lit(ingestion_date))
        return df


    def load_json_file(self,file_path: str) -> Dict[str, Any]:
        """Lê um único arquivo de configuração JSON e retorna um dicionário."""
        print(f"Lendo arquivo de configuração: '{file_path}'")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"ERRO: Arquivo de configuração '{file_path}' não foi encontrado.")
            raise
        except json.JSONDecodeError:
            logging.error(f"ERRO: O arquivo '{file_path}' não é um JSON válido.")
            raise


    def definir_particionamento(self,writer,df: DataFrame,coluna_particao: str,tipos_particionamentos: TipoParticionamento, tamanho_bucket_mb: int = 256,
                                tamanho_df:int = None):
        """Determina a especificação de particionamento para uma tabela com base na configuração do source.

            writer - o Writer de escrita do dataframe
            df: Dataframe - Dataframe que sera ingerido
            coluna_particao: Str - Coluna usada no particionamento da tabela 
            tipos_particionamentos: TipoParticionamento - Enum dos tipo de particionamento mapeados no Utils
            tamanho_bucket_mb :int - Tamanho em Megabyte dos buckets, default é 256MB.        
            
            Returns: Um Writer com a estrategia de particionamento definido
        """
        try:
            if coluna_particao and tipos_particionamentos:
                logging.info(f"Particionado pela {coluna_particao}: {tipos_particionamentos}.")
                if tipos_particionamentos == TipoParticionamento.ANO :
                    writer = writer.partitionedBy(F.years(coluna_particao))
                elif tipos_particionamentos == TipoParticionamento.MES:
                    writer = writer.partitionedBy(F.months(coluna_particao))
                elif tipos_particionamentos == TipoParticionamento.DIA:
                    writer = writer.partitionedBy(F.days(coluna_particao))
                elif tipos_particionamentos == TipoParticionamento.BUCKET:
                    if not tamanho_df:
                        tamanho_df = self.estimar_tamanho_df(df)
                    if tamanho_df >= tamanho_bucket_mb:
                        total_buckets = self.calcular_numero_de_buckets(tamanho_df,tamanho_bucket_mb)
                        writer = writer.partitionedBy(F.bucket(total_buckets, coluna_particao))
                    else:
                        return writer
                else:
                    logging.warning(f"Erro, tipo de particionamento não mapeado:  {tipos_particionamentos}.")
                    return writer
                    
            return writer
            
        except Exception as e:
            logging.error(f"Falha ao setar o particionamento, tipo_particao:{tipos_particionamentos}. Erro {e}")
            raise

    def calcular_particoes_ideais(self,tamanho_df_mb: float, tamanho_alvo_mb: int) -> int:
        """
        Calcula o número ideal de partições para atingir um tamanho de arquivo alvo.
        tamanho_df_mb: float - Tamanho do dataframe
        tamanho_alvo_mb : int - Tamanho ideal do dataframe
        """
        max_particoes = 4000 # Limite Seguranca Particoes
        if tamanho_df_mb <= 0 or tamanho_alvo_mb <= 0:
            return 1 
        num_particoes = math.ceil(tamanho_df_mb / tamanho_alvo_mb)

        if num_particoes > max_particoes:
            logging.warning(f"O número de partições calculado ({num_particoes}) excedeu o limite máximo de {max_particoes}. "
                    f"Usando o valor máximo.")
            return max_particoes
        
        return max(1, int(num_particoes)) # Retorna o resultado, com mínimo de 1

    def estimar_tamanho_df(self,df):
        """
        Usa as estatísticas do otimizador de consultas do Spark para estimar o tamanho
        de um DataFrame em Megabytes.
        
        Retorna:
            float: O tamanho estimado em MB.
        """
        try:
            tamanho_em_mb = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes() / (1024 * 1024)
            logging.info(f"Tamanho estimado do DataFrame: {tamanho_em_mb:.2f} MB")
            return tamanho_em_mb
        except Exception as e:
            logging.warning(f"Não foi possível estimar o tamanho do DataFrame. Assumindo tamanho 0. Erro: {e}")
            return 0.0

    def calcular_numero_de_buckets(self,tamanho_df, tamanho_alvo_por_particao=256):
        """
        Calcula o número de buckets necessários para particionar um DataFrame.
        
        Args:
            tamanho_df (float): O tamanho estimado do DataFrame em MBs.
            tamanho_alvo_por_particao (float): O tamanho desejado para cada partição em MBs.

        Returns:
            int: O número de buckets calculado (mínimo de 1).
        """
        
        # Arrendodamento para cima        
        num_buckets = math.ceil(tamanho_df / tamanho_alvo_por_particao)
        
        # Inteiro com Minimo 1
        return max(1, int(num_buckets))
    
        

    def write_to_minio(self,df,minio_layer,partition_cols_for_write):
        logging.info(f"Salvando dados em '{minio_layer}'")  
        df.write\
            .mode("append")\
            .partitionBy(*partition_cols_for_write) \
            .format("parquet") \
            .save(minio_layer)


    def create_table_external_hive(self,spark,df,hive_table_name,minio_layer,partition_cols_for_write):
        """
            Funcao que escreve uma tabela em um catalogo hive
            Params:
                spark:SparkSession - Sessão spark configurada para o catalogo hive
                df:Dataframe - Dataframe com os dados a serem escritos
                hive_table_name:Str - Nome da tabela a ser salva no catalogo hive schema.nome_tabela
                minio_layer:Str - local no minio a ser salvo os parquets da tabela
                partition_cols_for_write:List - Colunas e particao da tabela(Obrigatorio que o dataframe ja tenha as colunas)
        """
                
        # Dropar Tabela se Existir
        spark.sql(f"DROP TABLE IF EXISTS {hive_table_name}")

        # Construir a definição das colunas não particionadas do DDL com o nome e tipo
        data_cols_ddl_list = []
        for field in df.schema.fields: 
            if field.name not in partition_cols_for_write:
                # Procurar transformar o tipo do dataframe para o tipo correspondente ao DDL Ex(IntegerType() > int)
                try:
                    type_string = field.dataType.catalogString()
                except AttributeError:
                    type_string = field.dataType.simpleString()
                data_cols_ddl_list.append(f"`{field.name}` {type_string.upper()}")
        data_cols_sql = ",\n  ".join(data_cols_ddl_list)

        #Definição das colunas particionadas do DDL com o nome e tipo      
        partition_cols_ddl_list = []
        for col_name in partition_cols_for_write:
            field = df.schema[col_name] # Acessa o StructField pelo nome da particao
            if field:
                # DDL Ex(IntegerType() > int)
                try:
                    type_string = field.dataType.catalogString()
                except AttributeError:
                    type_string = field.dataType.simpleString()
                partition_cols_ddl_list.append(f"`{field.name}` {type_string.upper()}")
        partition_cols_sql = ",\n  ".join(partition_cols_ddl_list)                    

        # Montar DDL do padrão Hive para tabelas externas particionadas
        create_table_ddl = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table_name} (
            {data_cols_sql}
            )
            PARTITIONED BY (
            {partition_cols_sql}
            )
            STORED AS PARQUET
            LOCATION '{minio_layer}'
            """         
        spark.sql(create_table_ddl)

        # Registrando partições da Tabela criada
        spark.sql(f"MSCK REPAIR TABLE {hive_table_name}")
        
        
    def parse_s3a_url(self,s3a_url: str):
        if not s3a_url.startswith("s3a://"):
            raise ValueError("URL precisa começar com s3a://")
        path = s3a_url.replace("s3a://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix



    def atualizar_controle_carga_tabela_minio(self,config_minio,s3a_path):
        try:
            bucket, path_tabela = self.parse_s3a_url(s3a_path)
            object_name = f"{path_tabela}/_load_control.json"
            #Todo Ajustar para secure True quando for HTTPS
            client = Minio(
                endpoint=config_minio.get('endpoint'),
                access_key=config_minio.get('access_key'),
                secret_key=config_minio.get('secret_key'),
                secure=False
            )
            metadata = {
                "dt_ultima_ingestao_dados": datetime.now().isoformat()
            }
            data = json.dumps(metadata, indent=2).encode("utf-8")

            client.put_object(
                bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type="application/json"
            )

            logging.info(f"Controle de carga atualizado")
        except Exception as e:
            logging.error(f"Falha em atualizar o controle de carga da tabela em '{s3a_path}'. Erro: {e}", exc_info=True)
            raise

    def carregar_source(self,caminho: str):
        logging.info(f"Iniciando leitura do source: {caminho}")
        mapa_final = {}

        # Caso seja um único arquivo
        if os.path.isfile(caminho) and caminho.endswith(".json"):
            arquivos = [caminho]
        elif os.path.isdir(caminho):
            arquivos = [os.path.join(caminho, f) for f in os.listdir(caminho) if f.endswith(".json")]
        else:
            raise ValueError(f"O caminho '{caminho}' não é válido (nem arquivo nem diretório).")

        for caminho_completo in arquivos:
            with open(caminho_completo, 'r', encoding='utf-8') as f:
                config_arquivo = json.load(f)

            informacoes = config_arquivo.get('informacoes', {})
            banco_origem = informacoes.get("banco")
            sistema_origem = informacoes.get("sistema")
            lista_schemas = informacoes.get("schemas", {})

            for schema_obj, tabelas in lista_schemas.items():
                for tabela_nome, config_tabela in tabelas.items():
                    config_tabela['banco'] = banco_origem
                    config_tabela['sistema'] = sistema_origem
                    config_tabela['schema'] = schema_obj
                    config_tabela['nome'] = tabela_nome
                    chave = f"{sistema_origem.lower()}_{schema_obj.lower()}.{tabela_nome.lower()}"
                    mapa_final[chave] = config_tabela

        logging.info(f"Source de configuração do {sistema_origem} para {len(mapa_final)} tabelas carregado com sucesso.")
        return mapa_final

    def limpar_snapshots(self,spark, catalogo, tabela_catalogo):
        tempo_corrente = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Limpando snapshots antigos para {tabela_catalogo}...")

        spark.sql(f"""
            CALL {catalogo}.system.expire_snapshots(
                table => '{tabela_catalogo}',
                older_than => TIMESTAMP '{tempo_corrente}',
                retain_last => 1
            )
        """)

        logging.info(f"Limpeza concluída para {tabela_catalogo}.")

                    
    def remove_colunas_por_tipo(
        self,
        spark,
        schema: str,
        table: str,
        db_origem:str,
        url: str,
        properties: dict,
        tipos_exclusao:list
    ):
        
        logging.info(f"Iniciando a exclusão de colunas de dados dos tipos {tipos_exclusao} para a tabela: {table}...") 

        origin = db_origem.lower()

        if origin == "oracle":
            query_catalogo = f"""
            (SELECT column_name, data_type
            FROM all_tab_columns
            WHERE owner = '{schema}'
                AND table_name = '{table}'
            ORDER BY column_id) T"""
        elif origin == "postgres":
            query_catalogo = f"""
            (SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
                AND table_name   = '{table}'
            ORDER BY ordinal_position) T"""
        else:
            raise ValueError(f"origin_type {db_origem} não suportado")

        meta = spark.read.jdbc(url=url, table=query_catalogo, properties=properties).collect()

        cols = []
        for r in meta:
            d = r.asDict()
            # normaliza acessos (tenta ambos os casos)
            colname = d.get("COLUMN_NAME") or d.get("column_name")
            if not colname:
                continue

            if origin == "oracle":
                dtype = (d.get("DATA_TYPE") or d.get("data_type") or "")
                if dtype in tipos_exclusao:
                    continue
            else:  # postgres
                udt = (d.get("udt_name") or d.get("UDT_NAME") or "")
                dtype = udt or (d.get("data_type") or d.get("DATA_TYPE") or "")
                if dtype in tipos_exclusao:
                    continue

            cols.append(colname)

        if not cols:
            raise RuntimeError(f"Nenhuma coluna encontrada em {schema}.{table}")

        # Quota cada coluna com aspas duplas
        return ", ".join(f'"{c}"' for c in cols)
