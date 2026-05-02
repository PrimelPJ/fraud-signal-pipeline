// src/index.ts
//
// Fraud Signal Pipeline — TypeScript Aggregator + WebSocket Server
// Consumes fraud.signals from Kafka, aggregates into 1-second rolling
// windows, and broadcasts structured signal batches to connected dashboard
// clients over WebSocket.

import { Kafka, Consumer, EachMessagePayload, logLevel } from "kafkajs";
import { WebSocketServer, WebSocket } from "ws";
import { createServer } from "http";

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

interface FraudSignal {
  txn_id:           string;
  account_id:       string;
  composite_score:  number;
  decision:         "BLOCK" | "REVIEW" | "PASS";
  triggered_rules:  string[];
  velocity_score:   number;
  entropy_score:    number;
  rule_score:       number;
  evaluated_at_ms:  number;
}

interface AggregatedWindow {
  window_start_ms:  number;
  window_end_ms:    number;
  total_txns:       number;
  blocked:          number;
  reviewed:         number;
  passed:           number;
  avg_score:        number;
  rule_hit_counts:  Record<string, number>;
  signals:          FraudSignal[];
}

interface ClientMessage {
  type: "SUBSCRIBE" | "UNSUBSCRIBE" | "PING";
  filters?: { min_score?: number; decisions?: string[] };
}

// ---------------------------------------------------------------------------
// Rolling window aggregator
// ---------------------------------------------------------------------------

class SignalAggregator {
  private readonly windowMs = 1_000; // 1-second rolling window
  private buffer: FraudSignal[] = [];
  private windowStart = Date.now();
  private readonly maxBufferSize = 10_000;

  ingest(signal: FraudSignal): void {
    if (this.buffer.length >= this.maxBufferSize) {
      // Evict oldest 10% on overflow
      this.buffer.splice(0, Math.floor(this.maxBufferSize * 0.1));
    }
    this.buffer.push(signal);
  }

  flush(): AggregatedWindow | null {
    const now = Date.now();
    if (now - this.windowStart < this.windowMs) return null;

    const windowEnd = now;
    const windowSignals = this.buffer.filter(
      (s) => s.evaluated_at_ms >= this.windowStart && s.evaluated_at_ms < windowEnd
    );

    if (windowSignals.length === 0) {
      this.windowStart = windowEnd;
      return null;
    }

    const blocked  = windowSignals.filter((s) => s.decision === "BLOCK").length;
    const reviewed = windowSignals.filter((s) => s.decision === "REVIEW").length;
    const passed   = windowSignals.filter((s) => s.decision === "PASS").length;

    const avgScore =
      windowSignals.reduce((sum, s) => sum + s.composite_score, 0) /
      windowSignals.length;

    const ruleCounts: Record<string, number> = {};
    for (const signal of windowSignals) {
      for (const rule of signal.triggered_rules) {
        ruleCounts[rule] = (ruleCounts[rule] ?? 0) + 1;
      }
    }

    const window: AggregatedWindow = {
      window_start_ms:  this.windowStart,
      window_end_ms:    windowEnd,
      total_txns:       windowSignals.length,
      blocked,
      reviewed,
      passed,
      avg_score:        Math.round(avgScore * 100) / 100,
      rule_hit_counts:  ruleCounts,
      signals:          windowSignals,
    };

    // Advance window; retain signals from new window start
    this.buffer = this.buffer.filter((s) => s.evaluated_at_ms >= windowEnd);
    this.windowStart = windowEnd;

    return window;
  }
}

// ---------------------------------------------------------------------------
// WebSocket broadcast server
// ---------------------------------------------------------------------------

class SignalBroadcaster {
  private readonly clients = new Map<
    WebSocket,
    { filters: ClientMessage["filters"]; connectedAt: number }
  >();

  register(ws: WebSocket): void {
    this.clients.set(ws, { filters: {}, connectedAt: Date.now() });
    ws.on("message", (data) => this.handleClientMessage(ws, data.toString()));
    ws.on("close", () => this.clients.delete(ws));
    ws.send(JSON.stringify({ type: "CONNECTED", server: "fraud-signal-pipeline/v1" }));
  }

  private handleClientMessage(ws: WebSocket, raw: string): void {
    try {
      const msg: ClientMessage = JSON.parse(raw);
      if (msg.type === "SUBSCRIBE" && msg.filters) {
        const meta = this.clients.get(ws);
        if (meta) meta.filters = msg.filters;
      } else if (msg.type === "PING") {
        ws.send(JSON.stringify({ type: "PONG", ts: Date.now() }));
      }
    } catch {
      // Ignore malformed client messages
    }
  }

  broadcast(window: AggregatedWindow): void {
    const payload = JSON.stringify({ type: "SIGNAL_BATCH", data: window });
    let sent = 0;

    for (const [ws, meta] of this.clients) {
      if (ws.readyState !== WebSocket.OPEN) continue;

      // Apply client-side filters
      const { min_score, decisions } = meta.filters ?? {};
      if (min_score != null && window.avg_score < min_score) continue;
      if (decisions && !decisions.some((d) => window[d.toLowerCase() as keyof AggregatedWindow])) continue;

      ws.send(payload);
      sent++;
    }

    if (sent > 0) {
      console.log(
        `[broadcast] window=${window.window_end_ms} txns=${window.total_txns} ` +
        `blocked=${window.blocked} avg_score=${window.avg_score} clients=${sent}`
      );
    }
  }

  get size(): number {
    return this.clients.size;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
  const SIGNAL_TOPIC  = process.env.SIGNAL_TOPIC  ?? "fraud.signals";
  const WS_PORT       = parseInt(process.env.WS_PORT ?? "3001", 10);

  // Kafka consumer
  const kafka    = new Kafka({ brokers: KAFKA_BROKERS, logLevel: logLevel.WARN });
  const consumer: Consumer = kafka.consumer({ groupId: "signal-aggregator-v1" });

  await consumer.connect();
  await consumer.subscribe({ topic: SIGNAL_TOPIC, fromBeginning: false });

  const aggregator  = new SignalAggregator();
  const broadcaster = new SignalBroadcaster();

  // WebSocket server
  const httpServer = createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", clients: broadcaster.size }));
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  const wss = new WebSocketServer({ server: httpServer });
  wss.on("connection", (ws) => broadcaster.register(ws));

  httpServer.listen(WS_PORT, () => {
    console.log(`[ws] WebSocket server listening on :${WS_PORT}`);
  });

  // Flush aggregator every 500ms and broadcast if window is ready
  setInterval(() => {
    const window = aggregator.flush();
    if (window) broadcaster.broadcast(window);
  }, 500);

  // Consume Kafka messages
  await consumer.run({
    eachMessage: async ({ message }: EachMessagePayload) => {
      if (!message.value) return;
      try {
        const signal: FraudSignal = JSON.parse(message.value.toString());
        aggregator.ingest(signal);
      } catch (err) {
        console.error("[consumer] Failed to parse signal:", err);
      }
    },
  });

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    console.log("[shutdown] SIGTERM received, draining...");
    await consumer.disconnect();
    httpServer.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("[fatal]", err);
  process.exit(1);
});
