#!/usr/bin/env bash
# scripts/deploy.sh
#
# Fraud Signal Pipeline — Environment Bootstrap
# Provisions Kafka topics, builds the Rust processor and TypeScript
# aggregator, and launches all services in the target environment.
#
# Usage:
#   ./scripts/deploy.sh --env local
#   ./scripts/deploy.sh --env staging --kafka-brokers broker1:9092,broker2:9092

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

ENV="local"
KAFKA_BROKERS="localhost:9092"
KAFKA_REPLICATION=1
INPUT_TOPIC="txn.raw"
OUTPUT_TOPIC="fraud.signals"
DECISION_TOPIC="fraud.decisions"
RUST_BINARY="./processor/target/release/fraud-processor"
TS_DIST="./dist"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)            ENV="$2";             shift 2 ;;
    --kafka-brokers)  KAFKA_BROKERS="$2";   shift 2 ;;
    *)                echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# Staging/production use higher replication factors
if [[ "$ENV" == "staging" || "$ENV" == "production" ]]; then
  KAFKA_REPLICATION=3
fi

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

ts() { date '+%Y-%m-%dT%H:%M:%S'; }
info()  { echo "$(ts) [INFO]  $*"; }
warn()  { echo "$(ts) [WARN]  $*" >&2; }
error() { echo "$(ts) [ERROR] $*" >&2; exit 1; }
step()  { echo ""; echo "$(ts) ══ $* ══"; }

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------

step "Checking dependencies"

check_cmd() {
  command -v "$1" &>/dev/null || error "'$1' is required but not installed."
}

check_cmd cargo
check_cmd node
check_cmd npm
check_cmd kafka-topics.sh
check_cmd jq
check_cmd curl

RUST_VERSION=$(cargo --version)
NODE_VERSION=$(node --version)
info "Rust: $RUST_VERSION"
info "Node: $NODE_VERSION"

# ---------------------------------------------------------------------------
# Kafka topic provisioning
# ---------------------------------------------------------------------------

step "Provisioning Kafka topics (env=$ENV, brokers=$KAFKA_BROKERS)"

provision_topic() {
  local topic="$1"
  local partitions="${2:-12}"
  local retention_ms="${3:-86400000}"  # 24h default

  if kafka-topics.sh \
      --bootstrap-server "$KAFKA_BROKERS" \
      --list 2>/dev/null | grep -q "^${topic}$"; then
    info "Topic '$topic' already exists — skipping creation"
  else
    info "Creating topic '$topic' (partitions=$partitions, replication=$KAFKA_REPLICATION)"
    kafka-topics.sh \
      --bootstrap-server "$KAFKA_BROKERS" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor "$KAFKA_REPLICATION" \
      --config "retention.ms=${retention_ms}" \
      --config "compression.type=lz4" \
      --if-not-exists
  fi
}

provision_topic "$INPUT_TOPIC"    24  86400000   # 24h, 24 partitions for high throughput
provision_topic "$OUTPUT_TOPIC"   12  172800000  # 48h
provision_topic "$DECISION_TOPIC" 6   604800000  # 7d (audit retention)

# ---------------------------------------------------------------------------
# Build Rust processor
# ---------------------------------------------------------------------------

step "Building Rust stream processor"

export RUSTFLAGS="-C target-cpu=native"

cargo build \
  --manifest-path processor/Cargo.toml \
  --release \
  2>&1 | tail -20

if [[ ! -f "$RUST_BINARY" ]]; then
  error "Rust binary not found at $RUST_BINARY after build"
fi

BINARY_SIZE=$(du -sh "$RUST_BINARY" | cut -f1)
info "Rust processor built successfully (size: $BINARY_SIZE)"

# ---------------------------------------------------------------------------
# Build TypeScript aggregator
# ---------------------------------------------------------------------------

step "Building TypeScript aggregator"

npm ci --prefix . --silent
npm run build --prefix . 2>&1

if [[ ! -d "$TS_DIST" ]]; then
  error "TypeScript dist not found at $TS_DIST after build"
fi

info "TypeScript build complete"

# ---------------------------------------------------------------------------
# Launch (local only; staging/prod use systemd/k8s)
# ---------------------------------------------------------------------------

if [[ "$ENV" == "local" ]]; then
  step "Launching services (local mode)"

  export KAFKA_BROKERS INPUT_TOPIC OUTPUT_TOPIC

  # Launch Rust processor in background
  "$RUST_BINARY" &
  RUST_PID=$!
  info "Rust processor started (PID=$RUST_PID)"

  # Launch TypeScript aggregator
  node "$TS_DIST/index.js" &
  NODE_PID=$!
  info "TypeScript aggregator started (PID=$NODE_PID)"

  # Write PID file for monitor.sh
  echo "$RUST_PID $NODE_PID" > /tmp/fraud-pipeline.pids
  info "PIDs written to /tmp/fraud-pipeline.pids"

  # Wait and tail logs
  wait
else
  info "Non-local environment: skipping process launch (use your orchestrator)"
fi

step "Deployment complete ✓"
