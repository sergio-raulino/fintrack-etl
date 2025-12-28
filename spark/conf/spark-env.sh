#!/usr/bin/env bash
# Foreground (útil em containers)
export SPARK_NO_DAEMONIZE=true

# Shuffle externo embutido no worker + limpeza
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS \
 -Dspark.worker.cleanup.enabled=true \
 -Dspark.worker.cleanup.interval=600 \
 -Dspark.worker.cleanup.appDataTtl=86400 \
 -Dspark.shuffle.service.enabled=true"

# Python da venv
export PYSPARK_PYTHON=/home/sparkuser/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=/home/sparkuser/.venv/bin/python

# Diretórios padronizados (existem via entrypoint)
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_WORKER_DIR=/opt/spark/work
export SPARK_LOCAL_DIRS=/tmp/spark-local

# Portas explícitas
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081

# Rede: no compose você já define SPARK_LOCAL_IP=spark
# (Opcional) Para master, se quiser fixar:
# export SPARK_MASTER_HOST=spark

# Robustez (opcional)
ulimit -n 65536 2>/dev/null || true
