FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openssh-client ca-certificates git curl wget xz-utils nano vim && \
    rm -rf /var/lib/apt/lists/*

# Cria diretório .ssh para o usuário airflow e Copia as chaves já geradas
RUN mkdir -p /home/airflow/.ssh && \
    chmod 700 /home/airflow/.ssh

COPY ssh/id_rsa /home/airflow/.ssh/id_rsa
COPY ssh/id_rsa.pub /home/airflow/.ssh/id_rsa.pub

#  Ajusta as permissões das chaves copiadas
RUN chmod 600 /home/airflow/.ssh/id_rsa && \
    chmod 644 /home/airflow/.ssh/id_rsa.pub && \
    touch /home/airflow/.ssh/known_hosts && \
    chown -R airflow: /home/airflow/.ssh

# Copia e garante executável como root
COPY docker-entrypoint-ext.sh /usr/local/bin/entrypoint-ext.sh
RUN chmod 0755 /usr/local/bin/entrypoint-ext.sh

# Copie e instale requirements como airflow
USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt"

    ENTRYPOINT ["/usr/local/bin/entrypoint-ext.sh"]