#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# deploy_git_clone.sh
#
# On the target server, (re)clones the apollon-aegis-postgresql GitHub repo
# into ${REMOTE_DIR}/${CLONE_DIR_NAME} (default: /opt/apollon-postgresql/postgresql),
# then builds the image, starts the service with docker compose, and verifies
# it actually came up correctly.
#
# Difference from deploy_remote.sh: that script pushes files from THIS
# machine via rsync/scp. This script instead has the SERVER pull the code
# straight from GitHub, so after a `git push` from your laptop, re-running
# this script (or just re-running the git pull + restart portion) is enough
# to update the server — no local file copy needed.
#
# Run this from a machine with real network access to the target server
# (your own laptop/desktop terminal, or a shell directly on the server).
# It cannot be run from a network-isolated sandbox.
#
# Usage:
#   cd postgresql/deploy
#   cp deploy.env.example deploy.env   # if it doesn't already exist
#   # edit deploy.env (REPO_URL / CLONE_DIR_NAME / REPO_BRANCH / GIT_TOKEN)
#   ./deploy_git_clone.sh
#
# Requirements on the SERVER: git, docker, the docker compose plugin.
# Requirements on the machine you run this FROM: bash, ssh.
# sshpass is optional but avoids repeated password prompts:
#   macOS:          brew install hudochenkov/sshpass/sshpass
#   Debian/Ubuntu:  sudo apt-get install sshpass
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/deploy.env"
CONTAINER_NAME="apollon-aegis-collector-postgresql"
DB_USER="apollon"
DB_NAME="apollon"
HOST_PORT="31110"
SHARED_NETWORK="apollon-aegis-network"

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
: "${REPO_URL:=https://github.com/raphaellee-waikorea/apollon-aegis-postgresql.git}"
: "${CLONE_DIR_NAME:=postgresql}"
: "${REPO_BRANCH:=main}"
: "${GIT_TOKEN:=}"

CLONE_PATH="${REMOTE_DIR}/${CLONE_DIR_NAME}"

# If a token is set, embed it into an https clone URL for non-interactive
# access to a private repo. Leave GIT_TOKEN empty for a public repo.
CLONE_URL="$REPO_URL"
if [[ -n "$GIT_TOKEN" && "$REPO_URL" == https://github.com/* ]]; then
  CLONE_URL="https://${GIT_TOKEN}@${REPO_URL#https://}"
fi

if [[ -z "${REMOTE_PASS:-}" ]]; then
  read -r -s -p "SSH password for ${REMOTE_USER}@${REMOTE_HOST}: " REMOTE_PASS
  echo
fi

SSH_OPTS=(-p "${REMOTE_PORT}" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
if command -v sshpass >/dev/null 2>&1; then
  SSH=(sshpass -p "${REMOTE_PASS}" ssh "${SSH_OPTS[@]}")
else
  echo "NOTE: sshpass not found on this machine — you'll be prompted for the" >&2
  echo "      password several times during this run." >&2
  SSH=(ssh "${SSH_OPTS[@]}")
fi

remote_run() {
  "${SSH[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "$1"
}

echo "==> [1/6] Testing SSH connection to ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PORT} ..."
remote_run "echo '    connected as' \$(whoami)@\$(hostname)"

echo "==> [2/6] Ensuring ${REMOTE_DIR} exists ..."
remote_run "
  set -e
  if [ ! -d '${REMOTE_DIR}' ]; then
    sudo mkdir -p '${REMOTE_DIR}'
    sudo chown \$(id -u):\$(id -g) '${REMOTE_DIR}'
  fi
"

echo "==> [3/6] Ensuring shared network ${SHARED_NETWORK} exists ..."
remote_run "
  set -e
  if ! docker network inspect '${SHARED_NETWORK}' >/dev/null 2>&1; then
    echo '  ${SHARED_NETWORK} does not exist yet — creating it'
    docker network create '${SHARED_NETWORK}'
  else
    echo '  ${SHARED_NETWORK} already exists'
  fi
"

echo "==> [4/6] Cloning/updating ${REPO_URL} -> ${CLONE_PATH} (branch ${REPO_BRANCH}) ..."
remote_run "
  set -e
  if [ -d '${CLONE_PATH}/.git' ]; then
    echo '  existing clone found — fetching and hard-resetting to origin/${REPO_BRANCH}'
    cd '${CLONE_PATH}'
    git remote set-url origin '${CLONE_URL}'
    git fetch origin '${REPO_BRANCH}'
    git checkout '${REPO_BRANCH}'
    git reset --hard 'origin/${REPO_BRANCH}'
    git remote set-url origin '${REPO_URL}'
  else
    echo '  no existing clone — cloning fresh'
    git clone --branch '${REPO_BRANCH}' '${CLONE_URL}' '${CLONE_PATH}'
    cd '${CLONE_PATH}'
    git remote set-url origin '${REPO_URL}'
  fi
"

echo "==> [5/6] Building the image and starting the service ..."
remote_run "
  set -e
  cd '${CLONE_PATH}'
  [ -f .env ] || cp .env.example .env
  docker compose up -d --build
"

echo "==> [6/6] Verifying the deployment ..."
remote_run "
  set -e
  cd '${CLONE_PATH}'

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
  echo '--- ${SHARED_NETWORK} attachment check ---'
  if docker network inspect '${SHARED_NETWORK}' -f '{{range .Containers}}{{.Name}} {{end}}' | grep -qw '${CONTAINER_NAME}'; then
    echo '${SHARED_NETWORK}: ATTACHED'
  else
    echo 'WARNING: ${CONTAINER_NAME} is not attached to ${SHARED_NETWORK}'
  fi

  echo
  echo '--- deployed commit ---'
  git -C '${CLONE_PATH}' log --oneline -1

  echo
  echo '--- recent container logs (last 20 lines) ---'
  docker compose logs --tail=20 postgresql
"

echo
echo "==> Done."
echo "    Deployed at: ${REMOTE_USER}@${REMOTE_HOST}:${CLONE_PATH}"
echo "    Connect from the server host with:"
echo "      psql -h 127.0.0.1 -p ${HOST_PORT} -U ${DB_USER} -d ${DB_NAME}"
echo "    (bound to 127.0.0.1 only — use an SSH tunnel to reach it remotely)"
echo "    Also reachable from other apollon-aegis containers on ${SHARED_NETWORK}"
echo "    at hostname '${CONTAINER_NAME}', port 5432."
