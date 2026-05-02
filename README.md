<<<<<<< HEAD
# Fraud Signal Pipeline

[![TypeScript](https://img.shields.io/badge/TypeScript-5.4+-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://typescriptlang.org)
[![Rust](https://img.shields.io/badge/Rust-1.78+-000000?style=flat-square&logo=rust&logoColor=white)](https://rust-lang.org)
[![Bash](https://img.shields.io/badge/Bash-5.2+-4EAA25?style=flat-square&logo=gnubash&logoColor=white)](https://gnu.org/software/bash)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)

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
=======
# NetWatch Anomaly Detector

A **distributed network packet anomaly detection system** for identifying exfiltration patterns, lateral movement, and volumetric DDoS precursors at wire speed. The C++ capture layer ingests raw packets via `libpcap`, extracts flow-level features, and streams them over gRPC to a Java-based distributed coordinator that orchestrates detection across multiple sensor nodes. An R analysis module ingests the coordinator's aggregated flow telemetry to perform statistical baseline modeling and threshold calibration using **CUSUM** (Cumulative Sum Control Charts) and **Isolation Forest** outlier scoring.

---

## System Architecture

```mermaid
flowchart TD
    subgraph Edge["Edge Sensors (C++ / libpcap)"]
        A1[eth0 — Capture Thread] --> P1[Packet Parser]
        A2[eth1 — Capture Thread] --> P1
        P1 --> F[Flow Tracker\nFive-Tuple Hash Map]
        F --> E[Feature Extractor\nIAT · Entropy · Flags]
        E --> G[gRPC Flow Emitter]
    end

    subgraph Coordinator["Distributed Coordinator (Java)"]
        G -->|FlowRecord stream| C[Coordinator Node]
        C --> D[Flow Aggregator\nTumbling Windows 10s]
        D --> L[CUSUM Change Detector]
        D --> M[Threshold Evaluator]
        L --> N{Anomaly Decision}
        M --> N
        N -->|ALERT| O[Alert Publisher\nKafka: netwatch.alerts]
        N -->|NORMAL| Q[Telemetry Sink]
        C --> R[Node Registry\nHeartbeat Manager]
    end

    subgraph Analysis["Statistical Analysis (R)"]
        Q --> S[Telemetry Collector]
        S --> T[Baseline Modeler\nExpected Flow Distributions]
        T --> U[Isolation Forest\nAnomaly Scoring]
        T --> V[CUSUM Calibrator\nAdaptive Thresholds]
        V -->|Updated Thresholds JSON| C
        U --> W[Anomaly Report\nggplot2 Visualizations]
    end

    subgraph Ops
        O --> X[SIEM Integration\nSplunk / QRadar]
        W --> Y[Analyst Dashboard]
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127
    end
```

---

<<<<<<< HEAD
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
=======
## Detection Methodology

### Flow Feature Vector

Each network flow $f$ is represented as a feature vector $\mathbf{x} \in \mathbb{R}^d$:

$$\mathbf{x} = \left[ \bar{\Delta t},\ \sigma_{\Delta t},\ H_{payload},\ \frac{|\text{SYN}|}{|\text{PKT}|},\ \frac{|\text{RST}|}{|\text{PKT}|},\ \bar{L},\ \text{PPM} \right]^T$$

Where:
- $\bar{\Delta t}$ — Mean inter-arrival time (ms)
- $\sigma_{\Delta t}$ — IAT standard deviation (jitter)
- $H_{payload}$ — Shannon payload entropy $\in [0, 8]$ bits
- $\text{SYN}, \text{RST}$ ratios — TCP flag anomaly indicators
- $\bar{L}$ — Mean packet length
- $\text{PPM}$ — Packets per minute (volumetric indicator)

### CUSUM Change Detection

The CUSUM statistic $S_t$ accumulates deviations from a learned baseline $\mu_0$:

$$S_t = \max(0,\ S_{t-1} + (x_t - \mu_0 - k))$$

An alert is raised when $S_t > h$, where $k$ (slack parameter) and $h$ (decision threshold) are calibrated by the R analysis module from historical baseline distributions.

### Isolation Forest

For multivariate anomaly scoring, an Isolation Forest partitions the feature space by randomly selecting split features and values. The anomaly score for observation $x$ is:

$$s(x, n) = 2^{-\frac{E[h(x)]}{c(n)}}$$

where $h(x)$ is the path length in the isolation tree and $c(n)$ is the expected path length normalization factor. Scores $s \geq 0.65$ trigger a **SUSPICIOUS** classification.

---

## Component Flow

```mermaid
sequenceDiagram
    participant Cap as C++ Capture Layer
    participant Coord as Java Coordinator
    participant R as R Analysis Module
    participant SIEM

    Cap->>Cap: pcap_loop() on raw socket
    Cap->>Cap: Parse Ethernet/IP/TCP headers
    Cap->>Cap: Update flow table (five-tuple)
    Cap->>Coord: gRPC stream FlowRecord{} every 100ms
    Coord->>Coord: Aggregate into 10s tumbling window
    Coord->>Coord: Compute CUSUM S_t per flow group
    alt S_t > h (threshold breach)
        Coord->>SIEM: Publish Alert{flow, score, reason}
        Coord->>R: Send anomalous FlowBatch for reanalysis
    else Normal
        Coord->>R: Periodic telemetry batch (60s)
    end
    R->>R: Fit baseline distributions (Poisson / Gaussian)
    R->>R: Run Isolation Forest on batch
    R->>R: Recalibrate h, k per flow group
    R->>Coord: PUT /api/thresholds (JSON config update)
    R->>R: Generate ggplot2 anomaly heatmap
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127
```

---

## Tech Stack

<<<<<<< HEAD
| Layer | Technology | Role |
|---|---|---|
| **Stream Processor** | Rust 1.78 + `rdkafka` + `tokio` | High-throughput rule evaluation, feature extraction |
| **Analytics Dashboard** | TypeScript 5.4 + React 18 + Recharts | Live signal visualization, WebSocket consumer |
| **Message Broker** | Apache Kafka 3.7 | Durable, ordered event transport |
| **Serialization** | Protocol Buffers 3 | Zero-copy deserialization in Rust hot path |
| **DevOps** | Bash 5.2 | Environment bootstrap, topology management |
| **Observability** | Prometheus + Grafana | Latency histograms, consumer lag, throughput |
=======
| Component | Technology | Role |
|---|---|---|
| **Packet Capture** | C++20, `libpcap`, POSIX threads | Wire-speed packet ingestion and flow tracking |
| **Coordinator** | Java 21, gRPC, Virtual Threads (Project Loom) | Distributed aggregation and change detection |
| **Statistical Analysis** | R 4.4, `isotree`, `ggplot2`, `data.table` | Baseline modeling, Isolation Forest, calibration |
| **Transport (C++ → Java)** | gRPC + Protobuf 3 | Low-latency flow record streaming |
| **Transport (Java → SIEM)** | Apache Kafka | Durable alert delivery |
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127

---

## Project Structure

```
<<<<<<< HEAD
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
=======
netwatch-anomaly-detector/
├── capture/                     # C++ capture and feature extraction
│   ├── PacketAnalyzer.cpp        # libpcap loop, header parsing, flow tracking
│   └── Detector.cpp              # Feature computation and gRPC emission
├── coordinator/                  # Java distributed coordinator
│   └── src/main/java/dev/primel/netwatch/
│       ├── Coordinator.java      # gRPC server, flow aggregation, CUSUM
│       └── NodeManager.java      # Sensor node registry and heartbeat manager
├── analysis/                     # R statistical analysis
│   ├── anomaly_model.R           # Baseline fitting, Isolation Forest, calibration
│   └── visualize.R               # ggplot2 heatmaps and telemetry plots
├── proto/
│   └── netwatch.proto
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127
└── README.md
```

---

<<<<<<< HEAD
## Quickstart

```bash
# Bootstrap infrastructure and build all components
chmod +x scripts/deploy.sh
./scripts/deploy.sh --env local

# Monitor pipeline health
./scripts/monitor.sh --interval 5
=======
## Building

```bash
# C++ (requires libpcap-dev, libgrpc++-dev, libprotobuf-dev)
cd capture && cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Java (requires JDK 21+, Maven)
cd coordinator && mvn package -q

# R (requires R 4.4+)
Rscript -e "install.packages(c('isotree','ggplot2','data.table','httr2','jsonlite'))"
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127
```

---

<<<<<<< HEAD
## Performance

| Metric | Value |
|---|---|
| Median evaluation latency | < 8ms p50 |
| p99 evaluation latency | < 25ms p99 |
| Sustained throughput | 50,000 TPS |
| Kafka consumer lag target | < 1,000 msgs |
| Memory footprint (processor) | ~48 MB RSS |

---

=======
>>>>>>> 1f21c4a2dc4bb33c770d2cd3df2e0808a6c8f127
## License

MIT © Primel Jayawardana
