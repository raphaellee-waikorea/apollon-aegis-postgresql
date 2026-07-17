#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# attach_network.sh
#
# Ensures the shared apollon-aegis-network exists on the target server and
# that the standalone postgres container is attached to it — so it's
# reachable by container name from the other /opt/apollon-aegis services
# (fast-api, airflow, etc.), in addition to its own dedicated
# apollon-aegis-collector-network.
#
# Safe to re-run any time: creating an already-existing network, or
# connecting an already-connected container, is treated as a no-op rather
# than an error.
#
# This is normally NOT something you need to run by hand — docker-compose.yml
# already declares apollon-aegis-network as an external network the service
# attaches to on every `docker compose up`, and deploy_remote.sh /
# deploy_git_clone.sh both call this script's logic before bringing the
# service up (so the network exists even on a fresh host where the main
# apollon-aegis stack hasn't been started yet). Use this script directly
# when you just want to (re)attach an already-running container without a
# full redeploy — e.g. after a container was recreated some other way and
# fell off the network.
#
# Usage:
#   cd postgresql/deploy
#   cp deploy.env.example deploy.env   # if it doesn't already exist
#   ./attach_network.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/deploy.env"
CONTAINER_NAME="apollon-aegis-postgresql"
SHARED_NETWORK="apollon-aegis-network"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy deploy.env.example to deploy.env first." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${REMOTE_HOST:?REMOTE_HOST not set in deploy.env}"
: "${REMOTE_USER:?REMOTE_USER not set in deploy.env}"
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

echo "==> Ensuring ${SHARED_NETWORK} exists and ${CONTAINER_NAME} is attached to it ..."
"${SSH[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "
  set -e
  if ! docker network inspect '${SHARED_NETWORK}' >/dev/null 2>&1; then
    echo '  ${SHARED_NETWORK} does not exist yet — creating it'
    docker network create '${SHARED_NETWORK}'
  else
    echo '  ${SHARED_NETWORK} already exists'
  fi

  if docker ps -a --format '{{.Names}}' | grep -qx '${CONTAINER_NAME}'; then
    if docker network inspect '${SHARED_NETWORK}' -f '{{range .Containers}}{{.Name}} {{end}}' | grep -qw '${CONTAINER_NAME}'; then
      echo '  ${CONTAINER_NAME} is already connected to ${SHARED_NETWORK}'
    else
      echo '  connecting ${CONTAINER_NAME} to ${SHARED_NETWORK}'
      docker network connect '${SHARED_NETWORK}' '${CONTAINER_NAME}'
    fi
  else
    echo '  ${CONTAINER_NAME} is not running yet — network will be attached automatically on the next docker compose up (declared as external in docker-compose.yml)'
  fi

  echo
  echo '  containers currently on ${SHARED_NETWORK}:'
  docker network inspect '${SHARED_NETWORK}' -f '{{range .Containers}}    - {{.Name}}{{println}}{{end}}'
"

echo "==> Done."
