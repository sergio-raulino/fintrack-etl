#!/usr/bin/env bash
# docker-entrypoint-ext.sh
# - Lê variáveis do ambiente (via env_file do compose)
# - Prepara metastore, usuário admin, conexões versionadas
# - Recria conexões ssh_spark e smtp_tjce
# - Sobe scheduler (bg) e webserver (PID 1)

# Segurança e robustez
set -euo pipefail
set +H   # desativa history expansion para senhas com '!'

echo "[entrypoint] iniciando…"

# ===== Variáveis esperadas do ambiente =====
: "${AIRFLOW_ADMIN_PASSWORD:=admin}"          # defina em .env (não versionar)
: "${AIRFLOW__SMTP__SMTP_PASSWORD:=}"         # defina em .env (não versionar)

# Log não-sensível para diagnóstico
if [ -n "${AIRFLOW__SMTP__SMTP_PASSWORD}" ]; then
  echo "[entrypoint] SMTP password presente no ambiente (len=${#AIRFLOW__SMTP__SMTP_PASSWORD})"
else
  echo "[entrypoint][WARN] AIRFLOW__SMTP__SMTP_PASSWORD não foi definido no ambiente."
fi

# ===== Migração do metastore =====
echo "[entrypoint] airflow db migrate"
airflow db migrate

# ===== Usuário admin =====
if airflow users list | awk '{print $1}' | grep -qx admin; then
  echo "[entrypoint] admin existe; atualizando senha"
  airflow users set-password --username admin --password "${AIRFLOW_ADMIN_PASSWORD}"
else
  echo "[entrypoint] criando admin"
  airflow users create \
    --username admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password "${AIRFLOW_ADMIN_PASSWORD}"
fi

# ===== Import de conexões versionadas (opcional) =====
if [ -r /opt/airflow/config/connections.yaml ]; then
  echo "[entrypoint] Importando /opt/airflow/config/connections.yaml"
  airflow connections import /opt/airflow/config/connections.yaml || echo "[entrypoint][WARN] Import falhou; seguindo"
else
  echo "[entrypoint] connections.yaml não encontrado — pulando import"
fi

# ===== Recria conexão ssh_spark (idempotente) =====
echo "Deletando conexões ssh via URI"
airflow connections delete ssh_spark || true

echo "[entrypoint] Recriando conexão ssh_spark via URI"
airflow connections add ssh_spark \
  --conn-uri "ssh://sparkuser@spark:22?key_file=%2Fhome%2Fairflow%2F.ssh%2Fid_rsa&no_host_key_check=true"

# echo "[entrypoint] Recriando conexão smtp_tjce"
# airflow connections delete smtp_tjce || true
# airflow connections add smtp_tjce \
#   --conn-type smtp \
#   --conn-host "webmail.tj.ce.gov.br" \
#   --conn-port 587 \
#   --conn-login "naoresponda_datajust_etl@tjce.jus.br" \
#   --conn-password "${AIRFLOW__SMTP__SMTP_PASSWORD:-}" \
#   --conn-extra '{
#     "mail_from": "naoresponda_datajust_etl@tjce.jus.br"
#   }'

echo "[entrypoint] Conexões (parcial):"
airflow connections list | sed -n '1,120p'

# ===== Sobe serviços =====
echo "[entrypoint] iniciando scheduler (bg) + webserver"
airflow scheduler &                # background
exec airflow webserver             # PID 1 (mantém container vivo)

