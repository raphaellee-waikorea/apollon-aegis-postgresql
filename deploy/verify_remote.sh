#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# verify_remote.sh
#
# Standalone health check for an already-deployed instance — run this any
# time later (without redeploying) to confirm the service is still up.
# Uses the same deploy.env as deploy_remote.sh.
#
# Usage:
#   ./verify_remote.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/deploy.env"
CONTAINER_NAME="apollon-aegis-collector-postgresql"
DB_USER="apollon"
DB_NAME="apollon"
HOST_PORT="31110"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy deploy.env.example to deploy.env first." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${REMOTE_HOST:?REMOTE_HOST not set in deploy.env}"
: "${REMOTE_USER:?REMOTE_USER not set in deploy.env}"
: "${REMOTE_DIR:=/opt/apollon-postgresql}"
: "${REMOTE_PORT:=22}"

if [[ -z "${REMOTE_PASS:-}" ]]; then
  read -r -s -p "SSH password for ${REMOTE_USER}@${REMOTE_HOST}: " REMOTE_PASS
  echo
fi

SSH_OPTS=(-p "${REMOTE_PORT}" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
if command -v sshpass >/dev/null 2>&1; then
  SSH=(sshpass -p "${REMOTE_PASS}" ssh "${SSH_OPTS[@]}")
else
  SSH=(ssh "${SSH_OPTS[@]}")
fi

"${SSH[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "
  set -e
  cd '${REMOTE_DIR}'

  echo '--- docker compose ps ---'
  docker compose ps

  echo
  echo '--- container health ---'
  docker inspect -f '{{.State.Health.Status}}' ${CONTAINER_NAME} 2>/dev/null || echo 'unknown (no healthcheck data yet)'

  echo
  echo '--- pg_isready ---'
  docker exec ${CONTAINER_NAME} pg_isready -U ${DB_USER} -d ${DB_NAME}

  echo
  echo '--- pgvector extension check ---'
  if docker exec ${CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -tAc \"select 1 from pg_extension where extname='vector';\" | grep -q 1; then
    echo 'vector extension: OK'
  else
    echo 'vector extension: NOT FOUND'
  fi

  echo
  echo '--- host port ${HOST_PORT} listening check ---'
  (ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null) | grep ':${HOST_PORT} ' \
    && echo 'port ${HOST_PORT}: LISTENING' \
    || echo 'WARNING: port ${HOST_PORT} not seen listening on host'

  echo
  echo '--- disk usage of data dir ---'
  du -sh '${REMOTE_DIR}/data' 2>/dev/null || true
"
