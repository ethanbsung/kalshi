#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker-compose.phase-a.yml"

docker compose -f "$COMPOSE_FILE" up -d

cat <<'EOF'
Phase A infra started.
Use these env vars in your shell:
  export BUS_URL="nats://127.0.0.1:4222"
  export PG_DSN="postgresql://kalshi:kalshi@127.0.0.1:5432/kalshi"
EOF
