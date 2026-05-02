#!/usr/bin/env bash
# scripts/monitor.sh
#
# Fraud Signal Pipeline — Operations Monitor
# Continuously probes Kafka consumer lag, processor latency,
# throughput metrics, and WebSocket client health.
# Emits structured JSON log lines compatible with Datadog / Loki.
#
# Usage:
#   ./scripts/monitor.sh --interval 5
#   ./scripts/monitor.sh --interval 10 --lag-threshold 5000

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

INTERVAL_SECS=5
LAG_THRESHOLD=1000       # Alert if consumer lag exceeds this
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
CONSUMER_GROUP="fraud-processor-v1"
SIGNAL_TOPIC="fraud.signals"
WS_HEALTH_URL="${WS_HEALTH_URL:-http://localhost:3001/health}"
ALERT_WEBHOOK="${ALERT_WEBHOOK:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)       INTERVAL_SECS="$2"; shift 2 ;;
    --lag-threshold)  LAG_THRESHOLD="$2"; shift 2 ;;
    *)                echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ts_iso() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

emit_metric() {
  # Structured JSON metric line
  local level="$1" name="$2" value="$3" unit="${4:-}"
  printf '{"ts":"%s","level":"%s","metric":"%s","value":%s,"unit":"%s"}\n' \
    "$(ts_iso)" "$level" "$name" "$value" "$unit"
}

emit_event() {
  local level="$1" message="$2"
  printf '{"ts":"%s","level":"%s","event":"%s"}\n' \
    "$(ts_iso)" "$level" "$message"
}

alert() {
  local message="$1"
  emit_event "ALERT" "$message"
  if [[ -n "$ALERT_WEBHOOK" ]]; then
    curl -sf -X POST "$ALERT_WEBHOOK" \
      -H "Content-Type: application/json" \
      -d "{\"text\":\"[fraud-pipeline] ALERT: ${message}\"}" || true
  fi
}

# ---------------------------------------------------------------------------
# Health probes
# ---------------------------------------------------------------------------

check_kafka_lag() {
  local lag
  lag=$(kafka-consumer-groups.sh \
    --bootstrap-server "$KAFKA_BROKERS" \
    --describe \
    --group "$CONSUMER_GROUP" \
    2>/dev/null \
    | awk 'NR>1 && $NF ~ /^[0-9]+$/ {sum += $NF} END {print sum+0}')

  emit_metric "INFO" "kafka.consumer.lag" "$lag" "messages"

  if [[ "$lag" -gt "$LAG_THRESHOLD" ]]; then
    alert "Consumer lag ${lag} exceeds threshold ${LAG_THRESHOLD} on group=${CONSUMER_GROUP}"
  fi
}

check_ws_server() {
  local http_code clients
  local response
  response=$(curl -sf --max-time 3 "$WS_HEALTH_URL" 2>/dev/null || echo '{}')
  http_code=$(curl -so /dev/null -w "%{http_code}" --max-time 3 "$WS_HEALTH_URL" 2>/dev/null || echo "0")
  clients=$(echo "$response" | jq -r '.clients // 0' 2>/dev/null || echo "0")

  if [[ "$http_code" == "200" ]]; then
    emit_metric "INFO" "ws.server.healthy"  "1" "boolean"
    emit_metric "INFO" "ws.clients.connected" "$clients" "count"
  else
    emit_metric "WARN" "ws.server.healthy" "0" "boolean"
    alert "WebSocket server health check failed (HTTP $http_code)"
  fi
}

check_processor_pid() {
  if [[ ! -f /tmp/fraud-pipeline.pids ]]; then
    return
  fi

  read -r RUST_PID NODE_PID < /tmp/fraud-pipeline.pids

  for pid_info in "rust:$RUST_PID" "node:$NODE_PID"; do
    local name="${pid_info%%:*}"
    local pid="${pid_info##*:}"
    if kill -0 "$pid" 2>/dev/null; then
      local rss
      rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0")
      emit_metric "INFO" "process.${name}.rss_kb" "$rss" "kilobytes"
      emit_metric "INFO" "process.${name}.alive"  "1"    "boolean"
    else
      emit_metric "WARN" "process.${name}.alive" "0" "boolean"
      alert "Process '$name' (PID=$pid) is not running"
    fi
  done
}

check_disk() {
  local usage
  usage=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
  emit_metric "INFO" "disk.root.usage_pct" "$usage" "percent"
  if [[ "$usage" -gt 85 ]]; then
    alert "Disk usage at ${usage}% — approaching capacity"
  fi
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

emit_event "INFO" "Monitor started (interval=${INTERVAL_SECS}s, lag_threshold=${LAG_THRESHOLD})"

while true; do
  check_kafka_lag   || emit_event "WARN" "kafka_lag_check_failed"
  check_ws_server   || emit_event "WARN" "ws_health_check_failed"
  check_processor_pid
  check_disk

  sleep "$INTERVAL_SECS"
done
