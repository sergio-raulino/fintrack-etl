from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="bank_lnd_raw_finantial_files_load",
    description="Extração dos dados financeiros do Google Drive para MinIO (camada landing) via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "gdrive", "bank", "minio", "landing"],
) as dag:

    bank_lnd_raw_finantial_files_load = SSHOperator(
        task_id="bank_lnd_raw_finantial_files_load_task",
        ssh_conn_id="ssh_spark",
        command="""
            set -e
            echo "🚀 Iniciando a extração dos dados financeiros da landing para raw, via Spark"

            cd /opt/spark-jobs
            export JAVA_HOME=/opt/java/openjdk

            /opt/spark/bin/spark-submit \
              --master spark://spark:7077 \
              --deploy-mode client \
              --py-files /opt/spark-jobs/fintrack_etl.zip \
              /opt/spark-jobs/jobs/sdl_raw_fin/bank_lnd_raw_finantial_files_load.py \
                --client cruz_raulino_familia

            echo "✅ Carga lnd->raw finalizada com sucesso"

            # set -e
            # echo "🚀 Iniciando a extração dos dados financeiros da landing para raw, via Spark"

            # cd /opt/spark-jobs
            # export JAVA_HOME=/opt/java/openjdk
            # /opt/spark/bin/spark-submit \
            #   --master spark://spark:7077 \
            #   --deploy-mode client \
            #   --py-files /opt/spark-jobs/fintrack_etl.zip \
            #   /opt/spark-jobs/jobs/sdl_raw_fin/bank_lnd_raw_finantial_files_load.py \
            #     --client cruz_raulino_familia \
            #     --bank bb \
            #     --doc-type extratos \
            #     --year 2025 \
            #     --month 11

            echo "✅ Carga lnd->raw finalizada com sucesso"
        """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    bank_lnd_raw_finantial_files_load