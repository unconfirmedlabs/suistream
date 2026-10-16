//! Prometheus metrics for the gateway.

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub received: IntCounterVec,
    pub emitted: IntCounterVec,
    pub digest_mismatches: IntCounterVec,
    pub malformed: IntCounter,
    pub broken_chunks: IntCounter,
    pub dropped_overflow: IntCounter,
    pub slow_consumer_events: IntCounter,
    pub fallback_active: IntGauge,
    pub fetches: IntCounter,
    pub fetched: IntCounterVec,
    pub gaps: IntCounter,
    pub gap_checkpoints: IntCounter,
    pub head: IntGauge,
    pub health_max: IntGauge,
    pub emit_lag: Histogram,
    pub streams: IntGaugeVec,
    pub streams_rejected: IntCounterVec,
    pub streams_closed: IntCounterVec,
    pub masks_active: IntGauge,
    pub bytes_sent: IntCounterVec,
    pub render_errors: IntCounter,
    pub backfill_fetched: IntCounter,
    pub backfill_rendered: IntCounter,
    pub backfill_missing: IntCounter,
    pub backfill_fetch_seconds: Histogram,
    pub backfill_render_seconds: Histogram,
    pub bucket_errors: IntCounterVec,
    pub backfill_streams: IntGauge,
    pub backfill_gaps: IntCounterVec,
    pub live_fills: IntCounterVec,
    pub active_streams: IntGauge,
}

impl Metrics {
    pub fn new(region: &str) -> Self {
        let mut labels = std::collections::HashMap::new();
        labels.insert("region".to_owned(), region.to_owned());
        let registry = Registry::new_custom(Some("gateway".into()), Some(labels)).unwrap();
        macro_rules! reg {
            ($m:expr) => {{
                let m = $m.unwrap();
                registry.register(Box::new(m.clone())).unwrap();
                m
            }};
        }
        Self {
            received: reg!(IntCounterVec::new(
                Opts::new("received_total", "Complete checkpoint messages received"),
                &["source", "region"]
            )),
            emitted: reg!(IntCounterVec::new(
                Opts::new("emitted_total", "Checkpoints emitted, by winning source"),
                &["source"]
            )),
            digest_mismatches: reg!(IntCounterVec::new(
                Opts::new(
                    "digest_mismatches_total",
                    "Duplicates whose digest differs from the first copy"
                ),
                &["source", "region"]
            )),
            malformed: reg!(IntCounter::new(
                "malformed_total",
                "Messages without valid headers"
            )),
            broken_chunks: reg!(IntCounter::new(
                "broken_chunks_total",
                "Chunk sequences that could not be reassembled"
            )),
            dropped_overflow: reg!(IntCounter::new(
                "dropped_overflow_total",
                "New checkpoints dropped because the sequencer hand-off was full"
            )),
            slow_consumer_events: reg!(IntCounter::new(
                "nats_slow_consumer_total",
                "NATS slow consumer events"
            )),
            fallback_active: reg!(IntGauge::new(
                "fallback_active",
                "1 while subscribed to the cross-region fallback subject"
            )),
            fetches: reg!(IntCounter::new(
                "gap_fetches_total",
                "Gap-fill fetch attempts"
            )),
            fetched: reg!(IntCounterVec::new(
                Opts::new("gap_fetch_results_total", "Gap-fill fetch results"),
                &["result"]
            )),
            gaps: reg!(IntCounter::new("gaps_total", "Unfillable gaps emitted")),
            gap_checkpoints: reg!(IntCounter::new(
                "gap_checkpoints_total",
                "Checkpoints lost to unfillable gaps"
            )),
            head: reg!(IntGauge::new("head_checkpoint", "Last emitted checkpoint")),
            health_max: reg!(IntGauge::new(
                "health_max_checkpoint",
                "Max checkpoint announced by ingesters"
            )),
            emit_lag: reg!(Histogram::with_opts(
                HistogramOpts::new("emit_lag_seconds", "Emit time minus checkpoint timestamp")
                    .buckets(vec![
                        0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 60.0
                    ])
            )),
            streams: reg!(IntGaugeVec::new(
                Opts::new("streams", "Open SSE streams"),
                &["encoding"]
            )),
            streams_rejected: reg!(IntCounterVec::new(
                Opts::new("streams_rejected_total", "Rejected stream requests"),
                &["reason"]
            )),
            streams_closed: reg!(IntCounterVec::new(
                Opts::new("streams_closed_total", "Closed streams"),
                &["reason"]
            )),
            masks_active: reg!(IntGauge::new("masks_active", "Distinct read masks in use")),
            bytes_sent: reg!(IntCounterVec::new(
                Opts::new("bytes_sent_total", "SSE body bytes written"),
                &["encoding"]
            )),
            render_errors: reg!(IntCounter::new(
                "render_errors_total",
                "Checkpoint render failures"
            )),
            backfill_fetched: reg!(IntCounter::new(
                "backfill_fetched_total",
                "Checkpoint objects fetched from the bucket"
            )),
            backfill_rendered: reg!(IntCounter::new(
                "backfill_rendered_total",
                "Bucket checkpoints rendered"
            )),
            backfill_missing: reg!(IntCounter::new(
                "backfill_missing_total",
                "Checkpoints not available from the bucket"
            )),
            backfill_fetch_seconds: reg!(Histogram::with_opts(
                HistogramOpts::new("backfill_fetch_seconds", "Bucket GET latency")
                    .buckets(vec![0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 20.0])
            )),
            backfill_render_seconds: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "backfill_render_seconds",
                    "Render time of one bucket checkpoint (excluding queueing)"
                )
                .buckets(vec![0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0])
            )),
            bucket_errors: reg!(IntCounterVec::new(
                Opts::new(
                    "bucket_errors_total",
                    "Failed bucket requests (not_found_tip, not_found, error) and objects that did not render (render)"
                ),
                &["kind"]
            )),
            backfill_streams: reg!(IntGauge::new(
                "backfill_streams",
                "Streams currently backfilling from the bucket"
            )),
            backfill_gaps: reg!(IntCounterVec::new(
                Opts::new(
                    "backfill_gap_checkpoints_total",
                    "Resume checkpoints reported as gaps (span: beyond the max span, missing: not in the bucket)"
                ),
                &["reason"]
            )),
            live_fills: reg!(IntCounterVec::new(
                Opts::new(
                    "live_bucket_fills_total",
                    "Live holes filled from the bucket"
                ),
                &["result"]
            )),
            active_streams: reg!(IntGauge::new(
                "active_streams",
                "Open SSE streams, including backfilling ones"
            )),
            registry,
        }
    }
}
