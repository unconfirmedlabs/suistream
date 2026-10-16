//! Prometheus metrics for the ingester.

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    exponential_buckets,
};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub latest_seq: IntGauge,
    pub peer_max_seq: IntGauge,
    pub lag_behind_peers: IntGauge,
    pub stream_ends: IntCounterVec,
    pub streams_opened: IntCounter,
    pub rotations: IntCounter,
    pub published: IntCounter,
    pub publish_errors: IntCounter,
    pub handoff_dropped: IntCounter,
    pub duplicates: IntCounter,
    pub backfilled: IntCounter,
    pub backfill_failures: IntCounter,
    pub backfill_skipped: IntCounter,
    pub fetch_replies: IntCounterVec,
    pub raw_bytes: Histogram,
    pub payload_bytes: Histogram,
    pub chunked: IntCounter,
    pub checkpoint_age: Histogram,
    pub encode_seconds: Histogram,
    pub upstream_info: IntGaugeVec,
}

impl Metrics {
    pub fn new(source: &str, region: &str) -> Self {
        let mut labels = std::collections::HashMap::new();
        labels.insert("source".to_owned(), source.to_owned());
        labels.insert("region".to_owned(), region.to_owned());
        let registry = Registry::new_custom(Some("ingester".into()), Some(labels)).unwrap();
        let size_buckets = exponential_buckets(16.0 * 1024.0, 2.0, 12).unwrap();

        macro_rules! reg {
            ($m:expr) => {{
                let m = $m;
                registry.register(Box::new(m.clone())).unwrap();
                m
            }};
        }

        Self {
            latest_seq: reg!(
                IntGauge::new("latest_checkpoint", "Latest checkpoint published").unwrap()
            ),
            peer_max_seq: reg!(
                IntGauge::new(
                    "peer_max_checkpoint",
                    "Max checkpoint announced by any ingester"
                )
                .unwrap()
            ),
            lag_behind_peers: reg!(
                IntGauge::new(
                    "lag_behind_peers",
                    "peer_max_checkpoint - latest_checkpoint"
                )
                .unwrap()
            ),
            stream_ends: reg!(
                IntCounterVec::new(
                    Opts::new("stream_ends_total", "Upstream stream terminations"),
                    &["code", "routine"],
                )
                .unwrap()
            ),
            streams_opened: reg!(
                IntCounter::new("streams_opened_total", "Upstream streams opened").unwrap()
            ),
            rotations: reg!(
                IntCounter::new("rotations_total", "Make-before-break rotations completed")
                    .unwrap()
            ),
            published: reg!(IntCounter::new("published_total", "Checkpoints published").unwrap()),
            publish_errors: reg!(
                IntCounter::new("publish_errors_total", "NATS publish failures").unwrap()
            ),
            handoff_dropped: reg!(
                IntCounter::new(
                    "handoff_dropped_total",
                    "Live frames dropped because the publisher was behind"
                )
                .unwrap()
            ),
            duplicates: reg!(
                IntCounter::new(
                    "duplicates_total",
                    "Live frames at or below the last published checkpoint"
                )
                .unwrap()
            ),
            backfilled: reg!(
                IntCounter::new(
                    "backfilled_total",
                    "Checkpoints published from GetCheckpoint backfill"
                )
                .unwrap()
            ),
            backfill_failures: reg!(
                IntCounter::new("backfill_failures_total", "Backfill GetCheckpoint failures")
                    .unwrap()
            ),
            backfill_skipped: reg!(
                IntCounter::new(
                    "backfill_skipped_total",
                    "Checkpoints not backfilled because the gap was too large"
                )
                .unwrap()
            ),
            fetch_replies: reg!(
                IntCounterVec::new(
                    Opts::new("fetch_replies_total", "Replies to sui.mainnet.fetch"),
                    &["result"],
                )
                .unwrap()
            ),
            raw_bytes: reg!(
                Histogram::with_opts(
                    HistogramOpts::new("raw_bytes", "Encoded checkpoint frame size")
                        .buckets(size_buckets.clone()),
                )
                .unwrap()
            ),
            payload_bytes: reg!(
                Histogram::with_opts(
                    HistogramOpts::new("payload_bytes", "zstd-compressed frame size")
                        .buckets(size_buckets),
                )
                .unwrap()
            ),
            chunked: reg!(
                IntCounter::new("chunked_total", "Frames split into several NATS messages")
                    .unwrap()
            ),
            checkpoint_age: reg!(
                Histogram::with_opts(
                    HistogramOpts::new(
                        "checkpoint_age_seconds",
                        "Publish time minus checkpoint timestamp"
                    )
                    .buckets(vec![
                        0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 5.0, 10.0, 60.0
                    ]),
                )
                .unwrap()
            ),
            encode_seconds: reg!(
                Histogram::with_opts(
                    HistogramOpts::new(
                        "encode_seconds",
                        "Re-encode + compress time per checkpoint"
                    )
                    .buckets(exponential_buckets(0.0005, 2.0, 12).unwrap()),
                )
                .unwrap()
            ),
            upstream_info: reg!(
                IntGaugeVec::new(
                    Opts::new(
                        "upstream_info",
                        "Upstream server version (GetServiceInfo.server)"
                    ),
                    &["server"],
                )
                .unwrap()
            ),
            registry,
        }
    }
}
