from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="fintrack_extract_bb_bill",
    description="Extração da fatura do BB do Google Drive via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "gdrive", "download"],
) as dag:

    spark_extract_bb_bill = SSHOperator(
        task_id="spark_extract_bb_bill_task",
        ssh_conn_id="ssh_spark",
        command="""
            set -e
            echo "🚀 Iniciando a extração da fatura do BB do Google Drive via Spark"

            cd /opt/spark-jobs
            export JAVA_HOME=/opt/java/openjdk

            /opt/spark/bin/spark-submit \
            --master spark://spark:7077 \
            --deploy-mode client \
            --py-files /opt/spark-jobs/fintrack_etl.zip \
            /opt/spark-jobs/jobs/extract_bb_bill.py \
            --source drive \
            --also-json \
            --write-csv

            echo "✅ Extração finalizada com sucesso"
            """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    spark_extract_bb_bill
