# Airflow BI

## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://git.tjce.jus.br/data-science/airflow-bi.git
git branch -M main
git push -uf origin main
```

## Create configura o spark-defaults.conf apropriado:

# escolher o ambiente desejado
./set-spark-defaults.sh development
# ou
./set-spark-defaults.sh stage
# ou
./set-spark-defaults.sh production

# Suba os containers 
Crie a rede comum para ser utilizada:
networks:
  airflow-network:
    external: true
cd etl_project/
docker-compose -f docker-compose.etl.yml up -d
sudo docker-compose -f docker-compose.etl.yml ps
sudo docker-compose -f docker-compose.etl.yml down
sudo docker-compose -f docker-compose.etl.yml logs -f

## Para execuções de DAGs e scripts python dentro dos containers:
airflow (container): sudo docker exec -it airflow bash
airflow tasks test run_spark_save_to_minio_dag run_spark_save_to_minio 2025-06-29
airflow tasks test tjce_lnd_adm_api_windows_ldap_dag tjce_lnd_adm_api_windows_ldap 2025-09-03

spark (container): sudo docker exec -it spark bash

/opt/spark/bin/spark-submit /opt/spark-jobs/data-lake-administrativo/tjce_lnd_adm/api_windows_ldap/main.py
/opt/spark/bin/spark-submit /opt/spark-jobs/data-lake-administrativo/tjce_lnd_adm/main.py --tables ingestion/metadata_jobs/adm_admrh_tables.yaml --schema ingestion/metadata_jobs/adm_admrh_schema.yaml

/opt/spark/bin/spark-submit /opt/spark-jobs/data-lake-administrativo/tjce_lnd_adm/main.py --tables ingestion/metadata_jobs/ssd_ssd_tables.yaml --schema ingestion/metadata_jobs/ssd_ssd_schema.yaml

como root:
chown -R sparkuser:sparkuser /opt/spark-jobs

como sparkuser:
cd /opt/spark-jobs/data-lake-administrativo/tjce_lnd_adm &&
/opt/spark/bin/spark-submit main.py --tables ingestion/metadata_jobs/adm_admrh_tables.yaml --schema ingestion/metadata_jobs/adm_admrh_schema.yaml

/opt/spark/bin/spark-submit \
  --conf spark.pyspark.python=/home/sparkuser/.venv/bin/python \
  --conf spark.pyspark.driver.python=/home/sparkuser/.venv/bin/python \
  /opt/spark/bin/spark-submit /opt/spark-jobs/data-lake-administrativo/tjce_lnd_adm/api_windows_ldap/main.py


## Integrate with your tools

# 1. Para geração dos tokens de acesso ao Google Drive
python3 regenerate_gdrive_token.py

Atualizar a estrutura de clientes em spark/scripts/sources/clients.yaml

Em spark/scripts/sources/clients.yaml, configurar o hash na variável: GDRIVE_FINTRACK_FOLDER_ROOT_ID 

# Como rodar para a família cruz_raulino_familia, tudo que tiver no mapa:
python3 setup_gdrive_structure.py \
  --year-start 2025 \
  --year-end 2027 \
  --output-map spark/scripts/sources/gdrive_folders_map.json

# Filtrar só novembro de 2025, por exemplo:
  python3 spark/scripts/jobs/sdl_lnd_fin/bank_ori_lnd_finantial_files_extract.py \
  --client cruz_raulino_familia \
  --year 2025 \
  --month 11

