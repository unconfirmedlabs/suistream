//! Shared pieces of the checkpoint stream: NATS subjects and headers, the
//! canonical read mask, source configuration, payload chunking and NATS
//! connection setup.

pub mod chunk;
pub mod config;
pub mod metrics;
pub mod nats;

use std::time::Duration;

/// Subject prefix for checkpoint frames: `sui.mainnet.ckpt.<source>.<region>`.
pub const CHECKPOINT_SUBJECT_PREFIX: &str = "sui.mainnet.ckpt";
/// Every checkpoint frame, from every source and region.
pub const CHECKPOINT_SUBJECT_ALL: &str = "sui.mainnet.ckpt.>";
/// Request subject for checkpoint fetches, broadcast to every ingester.
/// Payload: `<seq>` or `<from>-<to>` (inclusive).
pub const FETCH_SUBJECT: &str = "sui.mainnet.fetch";
/// Largest range a single fetch may ask for.
pub const MAX_FETCH_RANGE: u64 = 100;
/// Subject prefix for ingester health beacons.
pub const HEALTH_SUBJECT_PREFIX: &str = "sui.mainnet.health";
/// Every ingester health beacon.
pub const HEALTH_SUBJECT_ALL: &str = "sui.mainnet.health.>";

/// Checkpoint sequence number of the frame (decimal).
pub const HEADER_CHECKPOINT: &str = "Sui-Checkpoint";
/// Checkpoint digest (base58), for cross-source comparison.
pub const HEADER_DIGEST: &str = "Sui-Checkpoint-Digest";
/// Checkpoint timestamp (unix milliseconds).
pub const HEADER_TIMESTAMP: &str = "Sui-Checkpoint-Timestamp";
/// Source name of the ingester that published the frame.
pub const HEADER_SOURCE: &str = "Sui-Source";
/// Fly region of the ingester that published the frame.
pub const HEADER_REGION: &str = "Sui-Region";
/// Payload encoding; absent means raw protobuf, `zstd` means zstd-compressed.
pub const HEADER_ENCODING: &str = "Content-Encoding";
/// `<i>/<n>` (1-based) when a payload is split into several messages.
pub const HEADER_CHUNK: &str = "Sui-Chunk";
/// Set on fetch replies that carry no checkpoint: `not-found` or `error`.
pub const HEADER_ERROR: &str = "Sui-Error";

pub const ENCODING_ZSTD: &str = "zstd";
pub const ERROR_NOT_FOUND: &str = "not-found";
pub const ERROR_INTERNAL: &str = "error";

/// Canonical read mask: the full checkpoint minus fields that sources render
/// differently (event/object JSON, object display). Used for both
/// `SubscribeCheckpoints` and `GetCheckpoint`; verified accepted by every
/// source in Phase 0. The gateway renders bucket checkpoints with the same
/// mask.
///
/// Not listed because fullnodes never fill them for checkpoints (always
/// empty, so dropping them changes no output): `transactions.objects`,
/// `transactions.transaction_index` and `transactions.events.events.
/// {checkpoint, transaction_digest, transaction_index, event_index}`.
pub const CANONICAL_READ_MASK: &[&str] = &[
    "sequence_number",
    "digest",
    "summary",
    "signature",
    "contents",
    "transactions.digest",
    "transactions.transaction",
    "transactions.signatures",
    "transactions.effects",
    "transactions.checkpoint",
    "transactions.timestamp",
    "transactions.balance_changes",
    "transactions.events.bcs",
    "transactions.events.digest",
    "transactions.events.events.package_id",
    "transactions.events.events.module",
    "transactions.events.events.sender",
    "transactions.events.events.event_type",
    "transactions.events.events.contents",
    "objects.objects.bcs",
    "objects.objects.object_id",
    "objects.objects.version",
    "objects.objects.digest",
    "objects.objects.owner",
    "objects.objects.object_type",
    "objects.objects.has_public_transfer",
    "objects.objects.contents",
    "objects.objects.package",
    "objects.objects.previous_transaction",
    "objects.objects.storage_rebate",
    "objects.objects.balance",
];

/// Paths deliberately excluded from the canonical mask; clients asking for
/// them explicitly get `INVALID_ARGUMENT`.
pub const EXCLUDED_PATHS: &[&str] = &[
    "transactions.events.events.json",
    "objects.objects.json",
    "objects.objects.display",
];

/// Largest message accepted from upstream (tonic defaults to 4 MiB).
pub const MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Largest NATS message we publish; the server's `max_payload` is 8 MiB.
pub const MAX_CHUNK_SIZE: usize = 7 * 1024 * 1024;

/// Interval at which ingesters publish health beacons.
pub const HEALTH_INTERVAL: Duration = Duration::from_secs(5);

/// Detects a process that stopped making progress while its peers did not
/// (e.g. wedged under memory pressure), so it can exit and be restarted.
pub struct StallDetector {
    last: Option<u64>,
    changed_at: std::time::Instant,
    limit: Duration,
    tolerance: u64,
}

impl StallDetector {
    /// Stalled when `own` has not changed for `limit` while peers are more
    /// than `tolerance` ahead.
    pub fn new(limit: Duration, tolerance: u64) -> Self {
        Self {
            last: None,
            changed_at: std::time::Instant::now(),
            limit,
            tolerance,
        }
    }

    pub fn stalled(
        &mut self,
        own: Option<u64>,
        peers: Option<u64>,
        now: std::time::Instant,
    ) -> bool {
        if own != self.last {
            self.last = own;
            self.changed_at = now;
            return false;
        }
        let behind = match (own, peers) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(o), Some(p)) => p > o + self.tolerance,
        };
        behind && now.duration_since(self.changed_at) > self.limit
    }
}

/// Convert to `Bytes` without excess capacity. Long-lived buffers (rings,
/// caches) must not keep e.g. zstd's worst-case output allocation alive.
pub fn exact_bytes(v: Vec<u8>) -> bytes::Bytes {
    bytes::Bytes::from(v.into_boxed_slice())
}

pub fn checkpoint_subject(source: &str, region: &str) -> String {
    format!("{CHECKPOINT_SUBJECT_PREFIX}.{source}.{region}")
}

pub fn health_subject(source: &str, region: &str) -> String {
    format!("{HEALTH_SUBJECT_PREFIX}.{source}.{region}")
}

/// Health beacon published by each ingester.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Health {
    pub source: String,
    pub region: String,
    /// Latest checkpoint published, if any.
    pub checkpoint: Option<u64>,
    /// Unix milliseconds when this beacon was produced.
    pub timestamp_ms: u64,
}

pub fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Fly region of this machine (`FLY_REGION`), or `local`.
pub fn region() -> String {
    std::env::var("FLY_REGION").unwrap_or_else(|_| "local".to_owned())
}

/// Initialise `tracing` from `RUST_LOG` (default `info`), JSON when
/// `LOG_FORMAT=json`.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().init();
    } else {
        builder.init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn stall_detector() {
        let t0 = Instant::now();
        let s = |n| t0 + Duration::from_secs(n);
        let mut d = StallDetector::new(Duration::from_secs(120), 100);
        assert!(!d.stalled(Some(10), Some(500), s(0)));
        assert!(!d.stalled(Some(10), Some(500), s(100)));
        assert!(d.stalled(Some(10), Some(500), s(121)));
        // Progress resets the clock.
        assert!(!d.stalled(Some(11), Some(500), s(122)));
        // Everyone stalled (chain halt, all upstreams down): not our fault.
        assert!(!d.stalled(Some(11), Some(50), s(400)));
        assert!(!d.stalled(Some(11), None, s(400)));
    }
}
