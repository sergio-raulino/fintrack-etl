from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="forms_cruz_raulino_ori_lnd_extract",
    description="Extração dos dados financeiros do Google Drive para MinIO (camada landing) via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "forms", "planilha", "minio", "landing"],
) as dag:

    bank_ori_lnd_finantial_files_extract = SSHOperator(
        task_id="forms_cruz_raulino_ori_lnd_extract",
        ssh_conn_id="ssh_spark",
        command="""
            set -e
            echo "🚀 Iniciando a extração dos gastos compartilhados do forms, via Spark"

            cd /opt/spark-jobs
            export JAVA_HOME=/opt/java/openjdk

            /opt/spark/bin/spark-submit \
              --master spark://spark:7077 \
              --deploy-mode client \
              --py-files /opt/spark-jobs/fintrack_etl.zip \
              /opt/spark-jobs/jobs/sdl_lnd_fin/forms_cruz_raulino_ori_lnd_extract.py \
                --client cruz_raulino_familia \
                --local-dir ./data/raw/drive \
                --force

            echo "✅ Extração finalizada com sucesso"
        """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    bank_ori_lnd_finantial_files_extract
