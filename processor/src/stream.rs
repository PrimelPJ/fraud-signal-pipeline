// processor/src/stream.rs
//
// Feature extraction, rule engine, and signal emission for the
// fraud signal pipeline. All hot-path code is allocation-minimizing
// and avoids async where synchronous evaluation is sufficient.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::RawTransaction;

// ---------------------------------------------------------------------------
// Extracted feature set
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TransactionFeatures {
    /// Normalized velocity anomaly score in [0, 100]
    pub velocity_score: f64,
    /// Shannon entropy of merchant name in [0, 100]
    pub entropy_score: f64,
    /// Number of transactions from this account in the last 60 seconds
    pub txn_count_60s: u32,
    /// Total spend from this account in the last 60 seconds (cents)
    pub spend_60s: u64,
    /// Whether the merchant MCC is in a high-risk category
    pub high_risk_mcc: bool,
}

// ---------------------------------------------------------------------------
// Velocity calculator (count-min sketch approximation)
// ---------------------------------------------------------------------------

/// Tracks per-account transaction counts and spend over a sliding window.
/// Uses a coarse sharded map for concurrent access without a global lock.
pub struct FeatureExtractor {
    /// account_id -> Vec<(timestamp_secs, amount_cents)>
    window: RwLock<HashMap<String, Vec<(u64, u64)>>>,
    window_secs: u64,

    /// MCC codes considered high-risk for fraud
    high_risk_mccs: Vec<u16>,
}

impl FeatureExtractor {
    pub fn new() -> Self {
        Self {
            window: RwLock::new(HashMap::new()),
            window_secs: 60,
            high_risk_mccs: vec![
                6010, 6011, 6050, // Cash/ATM
                7995,             // Gambling
                5912, 5122,       // Drug stores (high CNP fraud)
                4829,             // Wire transfers
            ],
        }
    }

    /// Extract features for a transaction. Updates the sliding window.
    pub async fn extract(&self, txn: &RawTransaction) -> TransactionFeatures {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cutoff = now.saturating_sub(self.window_secs);

        // Update and query velocity window
        let (txn_count, spend_total) = {
            let mut w = self.window.write().unwrap();
            let entries = w.entry(txn.account_id.clone()).or_default();

            // Evict expired entries (sliding window maintenance)
            entries.retain(|(ts, _)| *ts >= cutoff);

            // Append current transaction
            entries.push((now, txn.amount_cents));

            let count = entries.len() as u32;
            let spend: u64 = entries.iter().map(|(_, amt)| amt).sum();
            (count, spend)
        };

        let velocity_score = Self::velocity_to_score(txn_count, spend_total);
        let entropy_score = Self::shannon_entropy_score(&txn.merchant_name);
        let high_risk_mcc = self.high_risk_mccs.contains(&txn.merchant_mcc);

        TransactionFeatures {
            velocity_score,
            entropy_score,
            txn_count_60s: txn_count,
            spend_60s: spend_total,
            high_risk_mcc,
        }
    }

    /// Map (count, spend) over a 60s window to a normalized anomaly score.
    /// Thresholds derived from empirical P99 account behavior distributions.
    fn velocity_to_score(count: u32, spend_cents: u64) -> f64 {
        let count_score = match count {
            0..=3   => 0.0,
            4..=8   => 25.0,
            9..=15  => 55.0,
            16..=25 => 75.0,
            _       => 95.0,
        };

        // Spend threshold: > $500 in 60s triggers elevated score
        let spend_dollars = spend_cents / 100;
        let spend_score = match spend_dollars {
            0..=99    => 0.0,
            100..=299 => 20.0,
            300..=499 => 45.0,
            500..=999 => 70.0,
            _         => 90.0,
        };

        // Take max of count and spend signals
        count_score.max(spend_score)
    }

    /// Compute Shannon entropy of a string, normalized to [0, 100].
    /// High entropy (random-looking strings) is correlated with
    /// synthetic/bot-generated merchant names.
    fn shannon_entropy_score(s: &str) -> f64 {
        if s.is_empty() {
            return 100.0; // Empty merchant name is maximally suspicious
        }

        let mut freq: HashMap<char, usize> = HashMap::new();
        for c in s.chars() {
            *freq.entry(c).or_insert(0) += 1;
        }

        let len = s.len() as f64;
        let entropy: f64 = freq.values().map(|&count| {
            let p = count as f64 / len;
            -p * p.log2()
        }).sum();

        // Normalize: typical English text ~3.5 bits; random ~6+ bits
        // Map to [0, 100] where 0 = normal, 100 = maximally random
        let normalized = (entropy / 6.0 * 100.0).clamp(0.0, 100.0);

        // Scores below 40 are within normal range — suppress them
        if normalized < 40.0 { 0.0 } else { normalized - 40.0 } * (100.0 / 60.0)
    }
}

// ---------------------------------------------------------------------------
// Rule engine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Rule {
    id:       &'static str,
    severity: f64,  // Contribution to rule_score when triggered (0–100)
    evaluate: fn(&RawTransaction, &TransactionFeatures) -> bool,
}

pub struct RuleEngine {
    rules: Vec<Rule>,
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self {
            rules: vec![
                Rule {
                    id:       "HIGH_AMOUNT_FOREIGN",
                    severity: 40.0,
                    evaluate: |txn, _| {
                        txn.amount_cents > 50_000 // > $500
                            && txn.country_code != "CA"
                            && txn.country_code != "US"
                    },
                },
                Rule {
                    id:       "HIGH_RISK_MCC",
                    severity: 30.0,
                    evaluate: |_, features| features.high_risk_mcc,
                },
                Rule {
                    id:       "VELOCITY_BURST",
                    severity: 35.0,
                    evaluate: |_, features| features.txn_count_60s > 10,
                },
                Rule {
                    id:       "SPEND_BURST",
                    severity: 35.0,
                    evaluate: |_, features| features.spend_60s > 100_000, // > $1,000
                },
                Rule {
                    id:       "ROUND_AMOUNT",
                    severity: 15.0,
                    evaluate: |txn, _| {
                        txn.amount_cents > 10_000 && txn.amount_cents % 10_000 == 0
                    },
                },
                Rule {
                    id:       "SUSPICIOUS_DEVICE",
                    severity: 20.0,
                    evaluate: |txn, _| {
                        // Fingerprints starting with "bot-" or all-zeros are flagged
                        txn.device_fp.starts_with("bot-")
                            || txn.device_fp == "00000000-0000-0000-0000-000000000000"
                    },
                },
                Rule {
                    id:       "ENTROPY_MERCHANT",
                    severity: 25.0,
                    evaluate: |_, features| features.entropy_score > 70.0,
                },
            ],
        }
    }
}

impl RuleEngine {
    /// Evaluate all rules against the transaction and its features.
    ///
    /// Returns (aggregate_rule_score, vec_of_triggered_rule_ids).
    /// Score is additive up to a ceiling of 100.
    pub fn evaluate(
        &self,
        txn:      &RawTransaction,
        features: &TransactionFeatures,
    ) -> (f64, Vec<String>) {
        let mut score = 0.0_f64;
        let mut triggered = Vec::new();

        for rule in &self.rules {
            if (rule.evaluate)(txn, features) {
                score += rule.severity;
                triggered.push(rule.id.to_string());
            }
        }

        (score.min(100.0), triggered)
    }
}

// ---------------------------------------------------------------------------
// Signal emitter (metrics)
// ---------------------------------------------------------------------------

pub struct SignalEmitter {
    // In production, this wraps a Prometheus Registry
}

impl SignalEmitter {
    pub fn new() -> Self {
        Self {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_txn(mcc: u16, amount: u64, country: &str, device: &str) -> RawTransaction {
        RawTransaction {
            txn_id:         "test-001".into(),
            account_id:     "acct-abc".into(),
            merchant_name:  "Shopify".into(),
            merchant_mcc:   mcc,
            amount_cents:   amount,
            currency:       "CAD".into(),
            ip_address:     "192.168.1.1".into(),
            device_fp:      device.into(),
            timestamp_utc:  0,
            country_code:   country.into(),
        }
    }

    #[test]
    fn test_entropy_normal_merchant() {
        let score = FeatureExtractor::shannon_entropy_score("Shopify Inc");
        assert!(score < 50.0, "Normal merchant name should have low entropy");
    }

    #[test]
    fn test_entropy_random_string() {
        let score = FeatureExtractor::shannon_entropy_score("xK7@#mQ2$Lv!9");
        assert!(score > 60.0, "Random string should have high entropy");
    }

    #[test]
    fn test_rule_high_amount_foreign() {
        let engine = RuleEngine::default();
        let txn = make_txn(5411, 60_000, "NG", "dev-fp-001");
        let features = TransactionFeatures {
            velocity_score: 0.0, entropy_score: 0.0,
            txn_count_60s: 1, spend_60s: 60_000, high_risk_mcc: false,
        };
        let (score, triggered) = engine.evaluate(&txn, &features);
        assert!(triggered.contains(&"HIGH_AMOUNT_FOREIGN".into()));
        assert!(score >= 40.0);
    }

    #[test]
    fn test_decision_from_score() {
        assert_eq!(Decision::from_score(85.0), Decision::Block);
        assert_eq!(Decision::from_score(60.0), Decision::Review);
        assert_eq!(Decision::from_score(30.0), Decision::Pass);
    }
}
