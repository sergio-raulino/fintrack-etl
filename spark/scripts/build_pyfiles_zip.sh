#!/usr/bin/env bash
set -euo pipefail

# Raiz do repo (assume que você roda a partir do repo; se não, ajusta)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SRC_DIR="${REPO_ROOT}/src"
PKG_DIR="${SRC_DIR}/fintrack_etl"
OUT_ZIP="${REPO_ROOT}/fintrack_etl.zip"

if [[ ! -d "${PKG_DIR}" ]]; then
  echo "ERRO: pacote não encontrado em: ${PKG_DIR}"
  exit 1
fi

echo "Limpando zip antigo: ${OUT_ZIP}"
rm -f "${OUT_ZIP}"

echo "Gerando zip: ${OUT_ZIP}"
# Empacota mantendo o caminho "fintrack_etl/..." na raiz do zip
(
  cd "${SRC_DIR}"
  zip -r "${OUT_ZIP}" "fintrack_etl" \
    -x "*/__pycache__/*" "*.pyc" "*.pyo" "*.pyd" ".DS_Store" \
       "fintrack_etl/*.egg-info/*" "fintrack_etl/**/.pytest_cache/*"
)

echo "OK ✅ Zip criado:"
ls -lh "${OUT_ZIP}"
