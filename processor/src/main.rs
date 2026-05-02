// processor/src/main.rs
//
// Fraud Signal Pipeline — Rust Stream Processor
// High-throughput Kafka consumer loop that evaluates transactions
// against a rule engine and velocity calculator, then publishes
// fraud signals to a downstream Kafka topic.
//
// Targets sub-10ms p99 evaluation latency at 50,000 TPS.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::EnvFilter;

mod stream;
use stream::{FeatureExtractor, RuleEngine, SignalEmitter};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Config {
    kafka_brokers:   String,
    input_topic:     String,
    output_topic:    String,
    consumer_group:  String,
    max_concurrency: usize,
    poll_timeout_ms: u64,
    batch_size:      usize,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            kafka_brokers:   std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            input_topic:     std::env::var("INPUT_TOPIC")
                .unwrap_or_else(|_| "txn.raw".into()),
            output_topic:    std::env::var("OUTPUT_TOPIC")
                .unwrap_or_else(|_| "fraud.signals".into()),
            consumer_group:  std::env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "fraud-processor-v1".into()),
            max_concurrency: std::env::var("MAX_CONCURRENCY")
                .unwrap_or_else(|_| "256".into())
                .parse()
                .context("MAX_CONCURRENCY must be a positive integer")?,
            poll_timeout_ms: 5,
            batch_size:      500,
        })
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Raw transaction payload consumed from Kafka.
#[derive(Debug, Deserialize)]
pub struct RawTransaction {
    pub txn_id:          String,
    pub account_id:      String,
    pub merchant_name:   String,
    pub merchant_mcc:    u16,
    pub amount_cents:    u64,
    pub currency:        String,
    pub ip_address:      String,
    pub device_fp:       String,
    pub timestamp_utc:   i64,
    pub country_code:    String,
}

/// Fraud signal published to the output Kafka topic.
#[derive(Debug, Serialize)]
pub struct FraudSignal {
    pub txn_id:           String,
    pub account_id:       String,
    pub composite_score:  f64,
    pub decision:         Decision,
    pub triggered_rules:  Vec<String>,
    pub velocity_score:   f64,
    pub entropy_score:    f64,
    pub rule_score:       f64,
    pub evaluated_at_ms:  i64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Decision {
    Block,
    Review,
    Pass,
}

impl Decision {
    pub fn from_score(score: f64) -> Self {
        match score as u32 {
            80..=u32::MAX => Self::Block,
            50..=79       => Self::Review,
            _             => Self::Pass,
        }
    }
}

// ---------------------------------------------------------------------------
// Processing pipeline
// ---------------------------------------------------------------------------

/// Processes a single Kafka message through the full evaluation pipeline.
///
/// Deserialization errors are logged and skipped (dead-letter pattern).
/// Evaluation errors cause a warn log but do not halt the consumer.
#[instrument(skip(msg, extractor, rules, emitter, producer, cfg), fields(topic = %msg.topic(), partition = msg.partition(), offset = msg.offset()))]
async fn process_message(
    msg: &BorrowedMessage<'_>,
    extractor: &FeatureExtractor,
    rules:     &RuleEngine,
    emitter:   &SignalEmitter,
    producer:  &FutureProducer,
    cfg:       &Config,
) -> Result<()> {
    let payload = msg.payload().context("Empty message payload")?;

    let txn: RawTransaction = serde_json::from_slice(payload)
        .context("Failed to deserialize transaction")?;

    let txn_id = txn.txn_id.clone();

    // Feature extraction (velocity, entropy)
    let features = extractor.extract(&txn).await;

    // Rule evaluation
    let (rule_score, triggered) = rules.evaluate(&txn, &features);

    // Composite score
    let composite = (0.50 * rule_score)
        + (0.30 * features.velocity_score)
        + (0.20 * features.entropy_score);

    let signal = FraudSignal {
        txn_id:          txn_id.clone(),
        account_id:      txn.account_id,
        composite_score: composite,
        decision:        Decision::from_score(composite),
        triggered_rules: triggered,
        velocity_score:  features.velocity_score,
        entropy_score:   features.entropy_score,
        rule_score,
        evaluated_at_ms: chrono::Utc::now().timestamp_millis(),
    };

    let payload_bytes = serde_json::to_vec(&signal)?;

    producer
        .send(
            FutureRecord::to(&cfg.output_topic)
                .key(&txn_id)
                .payload(&payload_bytes),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| anyhow::anyhow!("Kafka produce error: {e}"))?;

    if signal.decision == Decision::Block {
        warn!(txn_id = %signal.txn_id, score = signal.composite_score, "BLOCK decision");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Structured logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .json()
        .init();

    let cfg = Config::from_env()?;
    info!(?cfg, "Fraud signal processor starting");

    // Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers",       &cfg.kafka_brokers)
        .set("group.id",                &cfg.consumer_group)
        .set("enable.auto.commit",      "false")
        .set("auto.offset.reset",       "earliest")
        .set("fetch.max.bytes",         "10485760") // 10 MB
        .set("max.poll.interval.ms",    "300000")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[&cfg.input_topic])
        .context("Failed to subscribe to input topic")?;

    // Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers",       &cfg.kafka_brokers)
        .set("acks",                    "all")
        .set("compression.type",        "lz4")
        .set("linger.ms",               "2")
        .set("batch.size",              "65536")
        .create()
        .context("Failed to create Kafka producer")?;

    let extractor = Arc::new(FeatureExtractor::new());
    let rules     = Arc::new(RuleEngine::default());
    let emitter   = Arc::new(SignalEmitter::new());
    let producer  = Arc::new(producer);
    let cfg       = Arc::new(cfg);
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency));

    info!("Consumer loop started — waiting for transactions");

    loop {
        match consumer.recv().await {
            Err(e) => {
                error!("Kafka receive error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(msg) => {
                let permit     = semaphore.clone().acquire_owned().await?;
                let extractor  = Arc::clone(&extractor);
                let rules      = Arc::clone(&rules);
                let emitter    = Arc::clone(&emitter);
                let producer   = Arc::clone(&producer);
                let cfg        = Arc::clone(&cfg);

                // Safety: we need an owned copy of the message for the spawn
                let payload    = msg.payload().map(|b| b.to_vec());
                let topic      = msg.topic().to_owned();
                let partition  = msg.partition();
                let offset     = msg.offset();

                tokio::spawn(async move {
                    let _permit = permit; // Released when task completes
                    if let Some(payload) = payload {
                        if let Ok(txn) = serde_json::from_slice::<RawTransaction>(&payload) {
                            let features = extractor.extract(&txn).await;
                            let (rule_score, triggered) = rules.evaluate(&txn, &features);
                            let composite = (0.50 * rule_score)
                                + (0.30 * features.velocity_score)
                                + (0.20 * features.entropy_score);

                            let signal = FraudSignal {
                                txn_id:          txn.txn_id.clone(),
                                account_id:      txn.account_id,
                                composite_score: composite,
                                decision:        Decision::from_score(composite),
                                triggered_rules: triggered,
                                velocity_score:  features.velocity_score,
                                entropy_score:   features.entropy_score,
                                rule_score,
                                evaluated_at_ms: chrono::Utc::now().timestamp_millis(),
                            };

                            if let Ok(bytes) = serde_json::to_vec(&signal) {
                                let _ = producer
                                    .send(
                                        FutureRecord::to(&cfg.output_topic)
                                            .key(&txn.txn_id)
                                            .payload(&bytes),
                                        Duration::from_secs(5),
                                    )
                                    .await;
                            }
                        }
                    }
                });

                consumer.commit_message(&msg, CommitMode::Async)?;
            }
        }
    }
}
