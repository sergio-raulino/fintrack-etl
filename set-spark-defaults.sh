#!/usr/bin/env bash
set -Eeuo pipefail

# Uso: ./set-spark-defaults.sh {development|stage|production}

usage() {
  echo "Uso: $0 {development|stage|production}"
  exit 1
}

env_arg="${1:-}"
[[ -z "$env_arg" ]] && usage
# normaliza para minúsculas
env_arg="${env_arg,,}"

case "$env_arg" in
  development|stage|production) ;;
  *) echo "Erro: environment inválido: '$env_arg'"; usage ;;
esac

# Caminhos (partindo da raiz do repo – onde este script está salvo)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_DIR="${ROOT_DIR}/spark/conf"

if [[ ! -d "$CONF_DIR" ]]; then
  echo "Erro: diretório não encontrado: ${CONF_DIR}"
  exit 2
fi

cd "$CONF_DIR"

TARGET="spark-defaults-${env_arg}.conf"
LINK="spark-defaults.conf"

if [[ ! -f "$TARGET" ]]; then
  echo "Erro: arquivo alvo inexistente: ${CONF_DIR}/${TARGET}"
  echo "Arquivos disponíveis:"
  ls -1 spark-defaults-*.conf || true
  exit 3
fi

# Cria/recria o link simbólico (forçando substituição se já existir)
ln -sfn "$TARGET" "$LINK"

# Confirmação com caminho resolvido
RESOLVED="$(readlink -f "$LINK" 2>/dev/null || realpath "$LINK" 2>/dev/null || echo "$TARGET")"

echo "OK: ${CONF_DIR}/${LINK} -> ${RESOLVED}"
ls -l "$LINK"
