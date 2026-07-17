#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# deploy_remote.sh
#
# Ships the Dockerfile / docker-compose.yml / init scripts in ../ to
# ${REMOTE_DIR} (default /opt/apollon-postgresql) on a remote server over
# SSH, builds the image, starts the service with docker compose, and then
# verifies it actually came up correctly (container healthy, pg_isready,
# pgvector extension present, port listening).
#
# IMPORTANT: run this from a machine that has real network access to the
# target server (your own laptop/desktop terminal, or a shell directly on
# the server). It cannot be run from a network-isolated sandbox.
#
# Usage:
#   cd postgresql/deploy
#   cp deploy.env.example deploy.env   # if deploy.env doesn't already exist
#   # edit deploy.env with the server's IP / user / password
#   ./deploy_remote.sh
#
# Requirements on the machine you run this FROM: bash, ssh, scp, (rsync
# recommended). sshpass is optional but avoids repeated password prompts:
#   macOS:          brew install hudochenkov/sshpass/sshpass
#   Debian/Ubuntu:  sudo apt-get install sshpass
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # .../postgresql
ENV_FILE="$SCRIPT_DIR/deploy.env"
CONTAINER_NAME="apollon-aegis-collector-postgresql"
DB_USER="apollon"
DB_NAME="apollon"
HOST_PORT="31110"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found." >&2
  echo "       Copy deploy.env.example to deploy.env and fill in the server details first." >&2
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
  SCP_OPTS=(-P "${REMOTE_PORT}" -o StrictHostKeyChecking=accept-new)
  SCP=(sshpass -p "${REMOTE_PASS}" scp "${SCP_OPTS[@]}")
  export SSHPASS="${REMOTE_PASS}"
  RSYNC_RSH_CMD="sshpass -e ssh ${SSH_OPTS[*]}"
else
  echo "NOTE: sshpass not found on this machine — you'll be prompted for the" >&2
  echo "      password several times during this run." >&2
  SSH=(ssh "${SSH_OPTS[@]}")
  SCP_OPTS=(-P "${REMOTE_PORT}" -o StrictHostKeyChecking=accept-new)
  SCP=(scp "${SCP_OPTS[@]}")
  RSYNC_RSH_CMD="ssh ${SSH_OPTS[*]}"
fi

remote_run() {
  "${SSH[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "$1"
}

echo "==> [1/5] Testing SSH connection to ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PORT} ..."
remote_run "echo '    connected as' \$(whoami)@\$(hostname)"

echo "==> [2/5] Ensuring ${REMOTE_DIR} exists ..."
remote_run "
  set -e
  if [ ! -d '${REMOTE_DIR}' ]; then
    sudo mkdir -p '${REMOTE_DIR}'
    sudo chown \$(id -u):\$(id -g) '${REMOTE_DIR}'
  fi
  mkdir -p '${REMOTE_DIR}/init' '${REMOTE_DIR}/data'
"

echo "==> [3/5] Copying Docker environment files to ${REMOTE_HOST}:${REMOTE_DIR} ..."
if command -v rsync >/dev/null 2>&1; then
  rsync -az --delete \
    --exclude 'data/' \
    --exclude 'deploy/' \
    -e "$RSYNC_RSH_CMD" \
    "$POSTGRES_DIR"/ "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"
else
  "${SCP[@]}" "$POSTGRES_DIR/Dockerfile" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/Dockerfile"
  "${SCP[@]}" "$POSTGRES_DIR/docker-compose.yml" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/docker-compose.yml"
  "${SCP[@]}" "$POSTGRES_DIR/.env.example" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/.env.example"
  "${SCP[@]}" "$POSTGRES_DIR/init/01-init-extensions.sql" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/init/01-init-extensions.sql"
fi

echo "==> [4/5] Building the image and starting the service ..."
remote_run "
  set -e
  cd '${REMOTE_DIR}'
  [ -f .env ] || cp .env.example .env
  docker compose up -d --build
"

echo "==> [5/5] Verifying the deployment ..."
remote_run "
  set -e
  cd '${REMOTE_DIR}'

  echo '--- docker compose ps ---'
  docker compose ps

  echo
  echo '--- waiting for the healthcheck to report healthy (up to 60s) ---'
  ok=false
  for i in \$(seq 1 12); do
    status=\$(docker inspect -f '{{.State.Health.Status}}' ${CONTAINER_NAME} 2>/dev/null || echo 'unknown')
    echo \"  attempt \$i: \$status\"
    if [ \"\$status\" = 'healthy' ]; then ok=true; break; fi
    sleep 5
  done
  if [ \"\$ok\" != 'true' ]; then
    echo 'WARNING: container did not report healthy within 60s, continuing checks anyway.'
  fi

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
  echo '--- recent container logs (last 20 lines) ---'
  docker compose logs --tail=20 postgresql
"

echo
echo "==> Done."
echo "    Connect from the server host with:"
echo "      psql -h 127.0.0.1 -p ${HOST_PORT} -U ${DB_USER} -d ${DB_NAME}"
echo "    (bound to 127.0.0.1 only — use an SSH tunnel to reach it from elsewhere)"
