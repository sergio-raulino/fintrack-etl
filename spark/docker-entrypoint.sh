#!/bin/bash
set -euo pipefail

echo "🔐 Iniciando SSH..."
/usr/sbin/sshd

# Garantir diretórios do Spark (imagem oficial não cria por padrão)
mkdir -p /tmp/spark-local
chown -R sparkuser:sparkuser /tmp/spark-local || true
chmod 1777 /tmp/spark-local || true

mkdir -p /opt/spark/work /opt/spark/logs
chown -R sparkuser:sparkuser /opt/spark/work /opt/spark/logs || true
chown -R sparkuser:sparkuser /opt/spark-jobs || true

# Detecta JAVA_HOME dinamicamente se não existir ou estiver errado
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  if command -v java >/dev/null 2>&1; then
    JAVA_BIN="$(command -v java)"
    export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
    echo "🔎 JAVA_HOME detectado: ${JAVA_HOME}"
  else
    echo "❌ java não encontrado no PATH"; exit 1
  fi
fi

# Sanidade
"${JAVA_HOME}/bin/java" -version || { echo "❌ JAVA_HOME inválido"; exit 1; }

SPARK_MODE="${SPARK_MODE:-master}"

echo "🚀 Iniciando Spark como ${SPARK_MODE} (usuário: sparkuser)..."
if [ "$SPARK_MODE" = "worker" ]; then
  : "${SPARK_MASTER_URL:?SPARK_MASTER_URL não definido}"
  exec su -m -s /bin/bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker ${SPARK_MASTER_URL}" sparkuser
else
  # Master UI: 8080 | RPC: 7077
  exec su -m -s /bin/bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master" sparkuser
fi
