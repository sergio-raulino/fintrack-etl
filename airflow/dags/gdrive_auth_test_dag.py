from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
import pendulum

TZ = pendulum.timezone("America/Fortaleza")

with DAG(
    dag_id="fintrack_drive_auth_test",
    description="Teste de autenticação Google Drive via Spark + fintrack_etl",
    start_date=days_ago(1),
    schedule=None,  # manual (ideal para teste)
    catchup=False,
    tags=["fintrack", "spark", "gdrive", "auth"],
) as dag:

    spark_drive_auth_test = SSHOperator(
        task_id="spark_drive_auth_test_task",
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
            jobs/drive_auth_test.py

            echo "✅ Teste finalizado com sucesso"
            """,
        cmd_timeout=60 * 60 * 1,
        conn_timeout=60,
        get_pty=True,
    )

    spark_drive_auth_test
