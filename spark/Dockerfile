# ===== Base oficial Apache Spark 3.5.4 (Scala 2.12, Java 17, Python 3) =====
FROM apache/spark:3.5.4-scala2.12-java17-python3-ubuntu

USER root

# ------------------------------------------------------------
# 1) SO base + SSH + toolchain p/ pyenv
# ------------------------------------------------------------
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openssh-server ca-certificates git curl wget xz-utils nano vim \
      make build-essential \
      libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
      libffi-dev liblzma-dev tk-dev \
      libgdbm-dev libnss3-dev libncursesw5-dev uuid-dev libedit-dev \
      libexpat1-dev && \
    mkdir -p /var/run/sshd && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# 2) Instala pyenv + Python 3.11.9
# ------------------------------------------------------------
ENV PYENV_ROOT=/opt/pyenv
ENV PATH=${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:$PATH
ENV PYTHON_CONFIGURE_OPTS="--enable-shared"

# (dev) clona pyenv ignorando SSL (se seu ambiente exige)
RUN git config --global http.sslVerify false && \
    git clone --depth 1 https://github.com/pyenv/pyenv.git ${PYENV_ROOT} && \
    git config --global --unset http.sslVerify

RUN mkdir -p ${PYENV_ROOT}/cache && \
    curl -k -L -o ${PYENV_ROOT}/cache/Python-3.11.9.tar.xz \
      https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tar.xz && \
    ${PYENV_ROOT}/bin/pyenv install 3.11.9 && \
    ${PYENV_ROOT}/bin/pyenv global 3.11.9 && \
    python -m pip install --no-cache-dir --upgrade pip setuptools wheel

# ------------------------------------------------------------
# 3) Hadoop 3.3.6 + AWS SDK (substitui jars default)
# ------------------------------------------------------------
# Zera qualquer hadoop 3.x anterior que venha na imagem
# RUN rm -f \
#   /opt/spark/jars/hadoop-aws-*.jar \
#   /opt/spark/jars/hadoop-auth-*.jar \
#   /opt/spark/jars/hadoop-common-*.jar \
#   /opt/spark/jars/hadoop-client-*.jar \
#   /opt/spark/jars/aws-java-sdk-*.jar
  
# COPY jars/*.jar /opt/spark/jars/

# ------------------------------------------------------------
# 3) Hadoop 3.3.6 + AWS SDK + Iceberg + Spark Hive (substitui jars default)
# ------------------------------------------------------------

# remove TODOS os jars de hadoop/aws que queremos substituir
RUN rm -f \
  /opt/spark/jars/hadoop-aws-*.jar \
  /opt/spark/jars/hadoop-common-*.jar \
  /opt/spark/jars/hadoop-auth-*.jar \
  /opt/spark/jars/hadoop-client-*.jar \
  /opt/spark/jars/hadoop-client-api-*.jar \
  /opt/spark/jars/hadoop-client-runtime-*.jar \
  /opt/spark/jars/aws-java-sdk-bundle-*.jar \
  /opt/spark/jars/aws-java-sdk-*.jar

# adiciona o CONJUNTO de jars Hadoop 3.3.6 + AWS SDK
COPY jars/hadoop-common-3.3.6.jar          /opt/spark/jars/
COPY jars/hadoop-auth-3.3.6.jar            /opt/spark/jars/
COPY jars/hadoop-client-api-3.3.6.jar      /opt/spark/jars/
COPY jars/hadoop-client-runtime-3.3.6.jar  /opt/spark/jars/
COPY jars/hadoop-aws-3.3.6.jar             /opt/spark/jars/
COPY jars/aws-java-sdk-bundle-1.12.734.jar /opt/spark/jars/

# Iceberg e Spark Hive (como no DataJust)
COPY jars/iceberg-spark-runtime-3.5_2.12-1.7.2.jar /opt/spark/jars/
COPY jars/spark-hive_2.12-3.5.4.jar                /opt/spark/jars/

# ------------------------------------------------------------
# 4) Usuário sparkuser + SSH
# ------------------------------------------------------------
RUN useradd -u 1001 -ms /bin/bash sparkuser && \
    echo 'sparkuser:spark' | chpasswd && \
    mkdir -p /home/sparkuser/.ssh && \
    chmod 700 /home/sparkuser/.ssh && \
    chown -R sparkuser:sparkuser /home/sparkuser/.ssh

COPY ssh/airflow_id_rsa.pub /home/sparkuser/.ssh/authorized_keys
RUN chmod 600 /home/sparkuser/.ssh/authorized_keys && \
    chown sparkuser:sparkuser /home/sparkuser/.ssh/authorized_keys

# Endurece e permite login do sparkuser; root por SSH proibido
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config && \
    printf '\nAllowUsers sparkuser\n' >> /etc/ssh/sshd_config

EXPOSE 22 7077 4040 8080

# ------------------------------------------------------------
# 5) JAVA_HOME / SPARK_HOME + perfil do sparkuser
# ------------------------------------------------------------
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN printf 'export SPARK_HOME=/opt/spark\nexport PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH\nexport PYENV_ROOT=/opt/pyenv\nexport PATH=$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH\n' \
    > /etc/profile.d/00-sparkuser-env.sh

# ------------------------------------------------------------
# 6) Venv do sparkuser + requirements
# ------------------------------------------------------------
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN chown sparkuser:sparkuser /tmp/requirements-spark.txt && \
    su - sparkuser -c "/opt/pyenv/versions/3.11.9/bin/python -m venv /home/sparkuser/.venv" && \
    su - sparkuser -c "/home/sparkuser/.venv/bin/pip install --no-cache-dir -U pip setuptools wheel" && \
    su - sparkuser -c "/home/sparkuser/.venv/bin/pip install --no-cache-dir -r /tmp/requirements-spark.txt"

# Também exporta para processos não-login
ENV VIRTUAL_ENV=/home/sparkuser/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYSPARK_PYTHON=/home/sparkuser/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/home/sparkuser/.venv/bin/python

# ------------------------------------------------------------
# 7) Jobs + entrypoint
# ------------------------------------------------------------
RUN mkdir -p /opt/spark-jobs
COPY --chown=sparkuser:sparkuser scripts/ /opt/spark-jobs/

COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]