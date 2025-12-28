from __future__ import annotations
import os
from urllib.parse import urlparse

from datatrack_utils.scripts.env_loader import EnvLoader  # mantido p/ compatibilidade
from spark.src.fintrack_etl.datatrack_utils.scripts.datatrack_spark_utils import DataTrackSparkUtils
from dotenv import load_dotenv

# ---------------- Environments ----------------
# MinIO: mantém o mesmo padrão das outras origens
minio_parameters = DataTrackSparkUtils().get_minio_parameteres("connections/minio.env")

# Catálogo da camada LND (bucket, catálogo Trino, etc.)
load_dotenv("connections/hivecatalog_fin.env")

# Config específica da origem SharePoint (Orçamento e Finanças)
load_dotenv("connections/gdrive.env")

# ------------- Variáveis de ambiente -------------
# Metadados da origem no Data Lake
origin = os.getenv("ORIGIN", "sp")           # ex.: "sp"
instance = os.getenv("INSTANCE", "list")     # ex.: "list"
schema = os.getenv("SCHEMA", "gestaoadmti")

bucket = os.getenv("TJCE_LND_BUCKET")
camada_trino = os.getenv("TJCE_LND_CATALOG")
schema_trino = f"{instance}_{schema}"        # ex.: "list_gestaoadmti"

# Caminho base no bucket landing
# Ex.: tjce-lnd-adm/sp/list/gestaoadmti
caminho_base = f"{bucket}/{origin}/{instance}/{schema}"

# ------------- Azure AD / Microsoft Graph -------------

tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

sp_site_url = os.getenv("SP_SITE")
sp_list_orcamentos_financas = os.getenv("SP_LIST_ORCAMENTOS_FINANCAS")
sp_list_contratos = os.getenv("SP_LIST_CONTRATOS")

if not all([tenant_id, client_id, client_secret, sp_site_url]):
    raise RuntimeError(
        "❌ Variáveis obrigatórias ausentes em 'connections/gdrive.env'.\n"
        "Verifique TENANT_ID, CLIENT_ID, CLIENT_SECRET e SP_SITE."
    )

# Separa host e path do site a partir da URL completa
parsed_site = urlparse(sp_site_url)
site_hostname = parsed_site.netloc           # ex.: tjce365.sp.com
site_path = parsed_site.path                 # ex.: /sites/gestaoadministrativadeti

# ------------- Config das listas SharePoint -------------

# Dicionário para facilitar o uso de "apelidos" das listas
# (caso queira no futuro chamar por "gestaoadmti", "contratos", etc.)
lists_config = {
    "orcamento_financas": {
        "display_name": sp_list_orcamentos_financas,
    },
    "contratos": {
        "display_name": sp_list_contratos,
    },
}