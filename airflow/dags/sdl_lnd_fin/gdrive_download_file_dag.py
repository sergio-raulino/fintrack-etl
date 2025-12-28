from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="fintrack_download_file",
    description="Teste de download de arquivo do Google Drive via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "gdrive", "download"],
) as dag:

    spark_download_file = SSHOperator(
        task_id="spark_download_file_task",
        ssh_conn_id="ssh_spark",
        command="""
            set -e
            echo "🚀 Iniciando teste de autenticação Google Drive via Spark"
            cd /opt/spark-jobs
            export JAVA_HOME=/opt/java/openjdk

            /opt/spark/bin/spark-submit \
            --master spark://spark:7077 \
            --deploy-mode client \
            --py-files fintrack_etl.zip \
            jobs/sdl_lnd_fin/download_drive_file.py

            echo "✅ Download finalizado com sucesso"
            """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    spark_download_file
