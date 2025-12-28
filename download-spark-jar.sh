#!/usr/bin/env bash
set -euo pipefail

# Descobre a pasta onde está o script e usa spark/jars relativo a ela
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${BASE_DIR}/spark/jars"

mkdir -p "${JARS_DIR}"

download_if_missing() {
  local url="$1"
  local file="$2"

  if [ -f "${JARS_DIR}/${file}" ]; then
    echo "✔ ${file} já existe em ${JARS_DIR}, pulando download."
  else
    echo "⬇ Baixando ${file}..."
    wget -O "${JARS_DIR}/${file}" "${url}"
  fi
}

cd "${JARS_DIR}"

# AWS SDK
download_if_missing \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.734/aws-java-sdk-bundle-1.12.734.jar" \
  "aws-java-sdk-bundle-1.12.734.jar"

# Hadoop 3.3.6
download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar" \
  "hadoop-auth-3.3.6.jar"

download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar" \
  "hadoop-common-3.3.6.jar"

download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar" \
  "hadoop-client-api-3.3.6.jar"

download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar" \
  "hadoop-client-runtime-3.3.6.jar"

# Hadoop AWS 3.3.6
download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
  "hadoop-aws-3.3.6.jar"

# Spark Hive 3.5.4 (Scala 2.12)
download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/3.5.4/spark-hive_2.12-3.5.4.jar" \
  "spark-hive_2.12-3.5.4.jar"

# Iceberg (Spark Runtime 3.5 — Scala 2.12)
download_if_missing \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar" \
  "iceberg-spark-runtime-3.5_2.12-1.7.2.jar"

# StAX & Woodstox
download_if_missing \
  "https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar" \
  "stax2-api-4.2.1.jar"

download_if_missing \
  "https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.5.1/woodstox-core-6.5.1.jar" \
  "woodstox-core-6.5.1.jar"
