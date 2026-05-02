# Fraud Signal Pipeline

A **real-time transaction fraud signal processing pipeline** combining a high-throughput Rust stream processor with a TypeScript analytics dashboard and Bash-based DevOps automation. Designed to evaluate transactions against a configurable rule engine and statistical anomaly detectors before they settle, with sub-10ms median evaluation latency at sustained throughput of 50,000 TPS.

---

## Architecture

```mermaid
flowchart LR
    subgraph Ingestion
        A[Payment Gateway] -->|Kafka Topic:\ntxn.raw| B[Rust Stream Processor]
        C[ACH Feed] -->|Kafka Topic:\ntxn.raw| B
    end

    subgraph Processing["Processing — Rust Core"]
        B --> D[Schema Validator]
        D --> E[Feature Extractor]
        E --> F{Rule Engine}
        F -->|Rule Hit| G[Signal Emitter]
        F -->|Clean| H[Pass-Through]
        E --> I[Velocity Calculator\nSliding Window]
        I --> F
        E --> J[Entropy Scorer\nString Anomaly]
        J --> F
    end

    subgraph Signals
        G -->|Kafka Topic:\nfraud.signals| K[Signal Aggregator]
        K -->|Kafka Topic:\nfraud.decisions| L[Decision Engine]
        L -->|BLOCK| M[Block Queue]
        L -->|REVIEW| N[Review Queue]
        L -->|PASS| O[Settlement Queue]
    end

    subgraph Observability["Observability — TypeScript"]
        K --> P[WebSocket Server\n:3001]
        P --> Q[React Dashboard\n:3000]
        Q --> R[Signal Heatmap]
        Q --> S[Velocity Chart]
        Q --> T[Rule Hit Table]
    end

    subgraph Ops["DevOps — Bash"]
        U[deploy.sh] --> V[Kafka Topic Setup]
        U --> W[Processor Binary Build]
        U --> X[Dashboard Build]
        Y[monitor.sh] --> Z[Latency Probe]
        Y --> AA[Lag Monitor]
    end
```

---

## Signal Model

Each transaction is evaluated against a scored rule set. The aggregate **fraud signal score** $F$ is:

$$F(t) = \alpha \cdot R(t) + \beta \cdot V(t) + \gamma \cdot E(t)$$

Where:
- $R(t)$ — Deterministic rule engine hit score $\in \{0, 1, \ldots, n\}$ (count of triggered rules, weighted by severity)
- $V(t)$ — Velocity anomaly score derived from a **count-min sketch** over a 5-minute sliding window
- $E(t)$ — String entropy score for merchant name and IP fields using **Shannon entropy**:

$$H(X) = -\sum_{i} p_i \log_2 p_i$$

Hyperparameters $(\alpha, \beta, \gamma)$ default to $(0.50, 0.30, 0.20)$ and are tunable at runtime.

---

## Decision Matrix

| F Score | Decision | Action |
|---|---|---|
| ≥ 80 | **BLOCK** | Synchronous decline; emit `fraud.block` event |
| 50–79 | **REVIEW** | Route to manual analyst queue; soft decline |
| < 50 | **PASS** | Forward to settlement; log signal for model training |

---

## Processing Pipeline

```mermaid
sequenceDiagram
    participant GW as Payment Gateway
    participant K1 as Kafka [txn.raw]
    participant RS as Rust Processor
    participant K2 as Kafka [fraud.signals]
    participant TS as TypeScript Aggregator
    participant WS as WebSocket Server
    participant UI as React Dashboard

    GW->>K1: Publish transaction (protobuf)
    RS->>K1: Poll batch (max 500 msgs, 5ms timeout)
    RS->>RS: Deserialize + validate schema
    RS->>RS: Extract features (velocity, entropy, rule flags)
    RS->>RS: Evaluate rule engine (50ns/txn p50)
    RS->>K2: Publish FraudSignal {txn_id, score, triggered_rules}
    TS->>K2: Consume fraud.signals
    TS->>TS: Aggregate into 1s rolling window
    TS->>WS: Broadcast SignalBatch
    WS->>UI: WebSocket push (JSON)
    UI->>UI: Update heatmap + charts (60fps)
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Stream Processor** | Rust 1.78 + `rdkafka` + `tokio` | High-throughput rule evaluation, feature extraction |
| **Analytics Dashboard** | TypeScript 5.4 + React 18 + Recharts | Live signal visualization, WebSocket consumer |
| **Message Broker** | Apache Kafka 3.7 | Durable, ordered event transport |
| **Serialization** | Protocol Buffers 3 | Zero-copy deserialization in Rust hot path |
| **DevOps** | Bash 5.2 | Environment bootstrap, topology management |
| **Observability** | Prometheus + Grafana | Latency histograms, consumer lag, throughput |

---

## Project Structure

```
fraud-signal-pipeline/
├── processor/             # Rust stream processor (core hot path)
│   ├── src/
│   │   ├── main.rs        # Tokio runtime bootstrap + Kafka consumer loop
│   │   └── stream.rs      # Feature extraction, rule engine, signal emission
│   └── Cargo.toml
├── src/                   # TypeScript dashboard + aggregator
│   ├── index.ts           # WebSocket server + Kafka consumer
│   └── dashboard.ts       # React components + real-time chart logic
├── scripts/
│   ├── deploy.sh          # End-to-end environment bootstrap
│   └── monitor.sh         # Ops monitoring and alerting probes
├── proto/
│   └── transaction.proto
└── README.md
```

---

## Quickstart

```bash
# Bootstrap infrastructure and build all components
chmod +x scripts/deploy.sh
./scripts/deploy.sh --env local

# Monitor pipeline health
./scripts/monitor.sh --interval 5
```

---

## Performance

| Metric | Value |
|---|---|
| Median evaluation latency | < 8ms p50 |
| p99 evaluation latency | < 25ms p99 |
| Sustained throughput | 50,000 TPS |
| Kafka consumer lag target | < 1,000 msgs |
| Memory footprint (processor) | ~48 MB RSS |

---

## License

MIT © Primel Jayawardana
