from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="bank_ori_lnd_finantial_files_extract",
    description="Extração dos dados financeiros do Google Drive para MinIO (camada landing) via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "gdrive", "bank", "minio", "landing"],
) as dag:

    bank_ori_lnd_finantial_files_extract = SSHOperator(
        task_id="bank_ori_lnd_finantial_files_extract_task",
        ssh_conn_id="ssh_spark",
        command="""
            set -e
            echo "🚀 Iniciando a extração dos dados financeiros do Google Drive via Spark"

            cd /opt/spark-jobs
            export JAVA_HOME=/opt/java/openjdk

            /opt/spark/bin/spark-submit \
            --master spark://spark:7077 \
            --deploy-mode client \
            --py-files /opt/spark-jobs/fintrack_etl.zip \
            /opt/spark-jobs/jobs/sdl_lnd_fin/bank_ori_lnd_finantial_files_extract.py \
            --client cruz_raulino_familia \
            --local-dir ./data/raw/drive \
            --year 2025 \
            --month 10


            /opt/spark/bin/spark-submit \
              --master spark://spark:7077 \
              --deploy-mode client \
              --py-files /opt/spark-jobs/fintrack_etl.zip \
              /opt/spark-jobs/jobs/sdl_lnd_fin/bank_ori_lnd_finantial_files_extract.py \
                --client cruz_raulino_familia \
                --local-dir ./data/raw/drive \
                --year 2025 \
                --month 11

            /opt/spark/bin/spark-submit \
              --master spark://spark:7077 \
              --deploy-mode client \
              --py-files /opt/spark-jobs/fintrack_etl.zip \
              /opt/spark-jobs/jobs/sdl_lnd_fin/bank_ori_lnd_finantial_files_extract.py \
                --client cruz_raulino_familia \
                --local-dir ./data/raw/drive \
                --year 2025 \
                --month 12

            /opt/spark/bin/spark-submit \
              --master spark://spark:7077 \
              --deploy-mode client \
              --py-files /opt/spark-jobs/fintrack_etl.zip \
              /opt/spark-jobs/jobs/sdl_lnd_fin/bank_ori_lnd_finantial_files_extract.py \
                --client cruz_raulino_familia \
                --local-dir ./data/raw/drive \
                --year 2026 \
                --month 1

            echo "✅ Extração finalizada com sucesso"
        """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    bank_ori_lnd_finantial_files_extract
