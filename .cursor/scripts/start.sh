#!/usr/bin/env bash
set -euo pipefail

cd /workspace

DOCKER=(docker)
if ! docker info >/dev/null 2>&1; then
  if sudo docker info >/dev/null 2>&1; then
    DOCKER=(sudo docker)
  fi
fi

ensure_docker() {
  if "${DOCKER[@]}" info >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v dockerd >/dev/null 2>&1; then
    echo "Docker is not installed; LocalStack cannot start." >&2
    exit 1
  fi
  if ! command -v fuse-overlayfs >/dev/null 2>&1; then
    echo "fuse-overlayfs is required for Docker in Cloud Agent VMs." >&2
    exit 1
  fi
  sudo modprobe fuse 2>/dev/null || true
  if [[ ! -f /etc/docker/daemon.json ]]; then
    echo '{"storage-driver":"fuse-overlayfs"}' | sudo tee /etc/docker/daemon.json >/dev/null
  fi
  sudo update-alternatives --set iptables /usr/sbin/iptables-legacy 2>/dev/null || true
  if ! pgrep -x dockerd >/dev/null; then
    sudo dockerd >/tmp/dockerd.log 2>&1 &
    for _ in $(seq 1 30); do
      if sudo docker info >/dev/null 2>&1; then
        DOCKER=(sudo docker)
        break
      fi
      sleep 1
    done
  fi
  "${DOCKER[@]}" info >/dev/null
}

wait_for_localstack() {
  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  export AWS_DEFAULT_REGION=us-east-1
  for _ in $(seq 1 60); do
    if curl -sf http://127.0.0.1:4566/_localstack/health | python3 -c "import sys,json; s=json.load(sys.stdin)['services'].get('sqs',''); sys.exit(0 if s in ('available','running') else 1)"; then
      return 0
    fi
    sleep 2
  done
  echo "LocalStack did not become ready on port 4566." >&2
  exit 1
}

ensure_docker
"${DOCKER[@]}" compose up -d localstack
wait_for_localstack
echo "LocalStack SQS is ready at http://127.0.0.1:4566"
