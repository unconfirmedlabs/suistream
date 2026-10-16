//! NATS receive path: reassembly and deduplication only.
//!
//! This task must keep up with every copy of every checkpoint (one per
//! subscribed source), so it never decodes or decompresses and never waits on
//! the sequencer: the hand-off is unbounded in messages but bounded in bytes,
//! and anything dropped is recovered by gap-fill.
//!
//! Subscriptions: the region's primary subjects (e.g. `sui.mainnet.ckpt.*.iad`).
//! If none of them delivers for `FALLBACK_AFTER`, the fallback subject
//! (`sui.mainnet.ckpt.>`) is added until the primaries have been healthy for
//! `FALLBACK_RELEASE`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::SelectAll;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::metrics::Metrics;
use crate::sequencer::{Head, Incoming, Input};

const FALLBACK_AFTER: Duration = Duration::from_secs(3);
const FALLBACK_RELEASE: Duration = Duration::from_secs(60);
/// Digests are remembered this far below the head for cross-checks.
const DIGEST_HISTORY: u64 = 64;

/// Header fields of a checkpoint message.
pub struct Parsed {
    pub seq: u64,
    pub source: String,
    pub region: String,
    pub digest: Option<String>,
    pub timestamp_ms: u64,
    pub chunk: Option<(usize, usize)>,
    pub error: Option<String>,
}

pub fn parse_message(msg: &async_nats::Message) -> Option<Parsed> {
    let headers = msg.headers.as_ref()?;
    let get = |name: &str| headers.get(name).map(|v| v.as_str().to_owned());
    let seq = get(common::HEADER_CHECKPOINT)?.parse().ok()?;
    if let Some(enc) = get(common::HEADER_ENCODING)
        && enc != common::ENCODING_ZSTD
    {
        return None;
    }
    Some(Parsed {
        seq,
        source: get(common::HEADER_SOURCE).unwrap_or_default(),
        region: get(common::HEADER_REGION).unwrap_or_default(),
        digest: get(common::HEADER_DIGEST),
        timestamp_ms: get(common::HEADER_TIMESTAMP)
            .and_then(|t| t.parse().ok())
            .unwrap_or_default(),
        chunk: match get(common::HEADER_CHUNK) {
            Some(v) => Some(common::chunk::parse_header(&v)?),
            None => None,
        },
        error: get(common::HEADER_ERROR),
    })
}

/// Result of offering an arrival to the deduplicator.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// First copy: forward to the sequencer.
    New,
    Duplicate,
    /// A duplicate whose digest differs from the first copy.
    DigestMismatch {
        first: String,
    },
}

/// First-wins dedup of checkpoint numbers above the emitted head.
#[derive(Default)]
pub struct Dedup {
    /// seq -> digest of the first copy forwarded.
    seen: BTreeMap<u64, String>,
}

impl Dedup {
    pub fn offer(&mut self, seq: u64, digest: Option<&str>, head: Option<u64>) -> Verdict {
        if let Some(head) = head {
            // Forget what can no longer matter.
            let keep_from = head.saturating_sub(DIGEST_HISTORY);
            if self
                .seen
                .first_key_value()
                .is_some_and(|(k, _)| *k < keep_from)
            {
                self.seen = self.seen.split_off(&keep_from);
            }
        }
        if let Some(first) = self.seen.get(&seq) {
            return match digest {
                Some(d) if !first.is_empty() && d != first => Verdict::DigestMismatch {
                    first: first.clone(),
                },
                _ => Verdict::Duplicate,
            };
        }
        if head.is_some_and(|h| seq <= h) {
            // Already emitted (or skipped); the digest is no longer known.
            return Verdict::Duplicate;
        }
        self.seen.insert(seq, digest.unwrap_or_default().to_owned());
        Verdict::New
    }

    /// Undo `New` for a copy that could not be handed off, so a later copy
    /// can take its place.
    pub fn forget(&mut self, seq: u64) {
        self.seen.remove(&seq);
    }
}

/// When to add and drop the fallback subscription.
pub struct FallbackPolicy {
    primary_last: Instant,
    /// Start of the current run of uninterrupted primary deliveries.
    healthy_since: Instant,
}

impl FallbackPolicy {
    pub fn new(now: Instant) -> Self {
        Self {
            primary_last: now,
            healthy_since: now,
        }
    }

    pub fn on_primary(&mut self, now: Instant) {
        if now.duration_since(self.primary_last) > FALLBACK_AFTER {
            self.healthy_since = now;
        }
        self.primary_last = now;
    }

    /// Silence while disconnected from NATS is not the primaries' fault.
    pub fn on_connected(&mut self, now: Instant) {
        self.primary_last = now;
        self.healthy_since = now;
    }

    /// `Some(true)`: subscribe to the fallback; `Some(false)`: drop it.
    pub fn decide(&mut self, now: Instant, connected: bool, active: bool) -> Option<bool> {
        if !connected {
            self.on_connected(now);
            return None;
        }
        let silent = now.duration_since(self.primary_last) > FALLBACK_AFTER;
        if !active && silent {
            Some(true)
        } else if active && !silent && now.duration_since(self.healthy_since) > FALLBACK_RELEASE {
            Some(false)
        } else {
            None
        }
    }
}

pub struct Receiver {
    pub nats: async_nats::Client,
    pub primary: Vec<String>,
    pub fallback: String,
    pub tx: mpsc::UnboundedSender<Input>,
    pub input_bytes: Arc<AtomicUsize>,
    pub max_input_bytes: usize,
    pub head: Arc<Head>,
    pub metrics: Metrics,
}

impl Receiver {
    pub async fn run(
        self,
        mut nats_events: mpsc::UnboundedReceiver<async_nats::Event>,
    ) -> anyhow::Result<()> {
        let mut primary = SelectAll::new();
        for subject in &self.primary {
            primary.push(self.nats.subscribe(subject.clone()).await?);
        }
        tracing::info!(primary = ?self.primary, fallback = %self.fallback, "subscribed");
        let mut fallback: Option<async_nats::Subscriber> = None;
        let mut policy = FallbackPolicy::new(Instant::now());
        let mut assembler = common::chunk::Assembler::new(common::MAX_DECODING_MESSAGE_SIZE);
        let mut dedup = Dedup::default();
        let mut tick = tokio::time::interval(Duration::from_millis(500));

        loop {
            let (msg, is_primary) = tokio::select! {
                Some(msg) = primary.next() => (msg, true),
                Some(msg) = async { fallback.as_mut()?.next().await }, if fallback.is_some() => (msg, false),
                Some(event) = nats_events.recv() => {
                    match event {
                        async_nats::Event::SlowConsumer(_) => {
                            self.metrics.slow_consumer_events.inc();
                            let _ = self.tx.send(Input::GapCheck);
                        }
                        async_nats::Event::Connected => {
                            policy.on_connected(Instant::now());
                            let _ = self.tx.send(Input::GapCheck);
                        }
                        _ => {}
                    }
                    continue;
                }
                _ = tick.tick() => {
                    let connected = self.nats.connection_state() == async_nats::connection::State::Connected;
                    match policy.decide(Instant::now(), connected, fallback.is_some()) {
                        Some(true) => {
                            tracing::warn!("primary subjects silent; subscribing to fallback {}", self.fallback);
                            fallback = Some(self.nats.subscribe(self.fallback.clone()).await?);
                            self.metrics.fallback_active.set(1);
                        }
                        Some(false) => {
                            tracing::info!("primary subjects healthy; dropping fallback");
                            if let Some(mut sub) = fallback.take() {
                                let _ = sub.unsubscribe().await;
                            }
                            self.metrics.fallback_active.set(0);
                        }
                        None => {}
                    }
                    continue;
                }
            };

            if is_primary {
                policy.on_primary(Instant::now());
            }
            self.handle(msg, &mut assembler, &mut dedup);
        }
    }

    fn handle(
        &self,
        msg: async_nats::Message,
        assembler: &mut common::chunk::Assembler,
        dedup: &mut Dedup,
    ) {
        let Some(parsed) = parse_message(&msg) else {
            self.metrics.malformed.inc();
            return;
        };
        let subject: Arc<str> = msg.subject.as_str().into();
        let payload = match assembler.feed(&subject, parsed.seq, parsed.chunk, msg.payload) {
            common::chunk::Feed::Complete(p) => p,
            common::chunk::Feed::Pending => return,
            common::chunk::Feed::Broken => {
                self.metrics.broken_chunks.inc();
                return;
            }
        };
        self.metrics
            .received
            .with_label_values(&[&parsed.source, &parsed.region])
            .inc();
        match dedup.offer(parsed.seq, parsed.digest.as_deref(), self.head.last()) {
            Verdict::New => {
                let len = payload.len();
                if self.input_bytes.load(Ordering::Relaxed) + len > self.max_input_bytes {
                    self.metrics.dropped_overflow.inc();
                    dedup.forget(parsed.seq);
                } else {
                    self.input_bytes.fetch_add(len, Ordering::Relaxed);
                    // Detach from the NATS read buffer: kept in the ring.
                    let payload = Bytes::copy_from_slice(&payload);
                    let _ = self.tx.send(Input::Checkpoint(Incoming {
                        seq: parsed.seq,
                        source: format!("{}.{}", parsed.source, parsed.region).into(),
                        timestamp_ms: parsed.timestamp_ms,
                        payload,
                    }));
                }
            }
            Verdict::Duplicate => {}
            Verdict::DigestMismatch { first } => {
                self.metrics
                    .digest_mismatches
                    .with_label_values(&[&parsed.source, &parsed.region])
                    .inc();
                tracing::error!(
                    seq = parsed.seq,
                    source = %parsed.source,
                    region = %parsed.region,
                    digest = ?parsed.digest,
                    %first,
                    "checkpoint digest differs between sources"
                );
            }
        }
        // After the data, so the sequencer never sees progress past a
        // checkpoint before the checkpoint itself.
        let _ = self.tx.send(Input::Progress {
            subject,
            seq: parsed.seq,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_wins() {
        let mut d = Dedup::default();
        assert_eq!(d.offer(10, Some("a"), None), Verdict::New);
        assert_eq!(d.offer(10, Some("a"), None), Verdict::Duplicate);
        assert_eq!(d.offer(11, Some("b"), None), Verdict::New);
    }

    #[test]
    fn digest_mismatch_detected() {
        let mut d = Dedup::default();
        d.offer(10, Some("a"), None);
        assert_eq!(
            d.offer(10, Some("x"), None),
            Verdict::DigestMismatch { first: "a".into() }
        );
        // Missing digest headers are not mismatches.
        assert_eq!(d.offer(10, None, None), Verdict::Duplicate);
    }

    #[test]
    fn at_or_below_head_is_duplicate() {
        let mut d = Dedup::default();
        assert_eq!(d.offer(5, Some("a"), Some(9)), Verdict::Duplicate);
        assert_eq!(d.offer(10, Some("a"), Some(9)), Verdict::New);
        // Recently emitted digests are still cross-checked.
        assert_eq!(
            d.offer(10, Some("z"), Some(10)),
            Verdict::DigestMismatch { first: "a".into() }
        );
    }

    #[test]
    fn history_is_pruned() {
        let mut d = Dedup::default();
        for s in 0..1000 {
            d.offer(s, Some("d"), Some(s));
            d.offer(s + 1, Some("d"), Some(s));
        }
        assert!(d.seen.len() <= DIGEST_HISTORY as usize + 2);
    }

    #[test]
    fn fallback_policy() {
        let t0 = Instant::now();
        let s = |n: u64| t0 + Duration::from_secs(n);
        let mut p = FallbackPolicy::new(t0);
        p.on_primary(s(1));
        assert_eq!(p.decide(s(2), true, false), None);
        // Primaries go silent.
        assert_eq!(p.decide(s(5), true, false), Some(true));
        // Still silent: stay on fallback (no flapping).
        assert_eq!(p.decide(s(6), true, true), None);
        assert_eq!(p.decide(s(100), true, true), None);
        // Primaries resume; release only after a healthy run.
        for t in 100..=160 {
            p.on_primary(s(t));
            assert_eq!(p.decide(s(t), true, true), None, "t={t}");
        }
        p.on_primary(s(161));
        assert_eq!(p.decide(s(161), true, true), Some(false));
    }

    #[test]
    fn fallback_policy_ignores_disconnects() {
        let t0 = Instant::now();
        let s = |n: u64| t0 + Duration::from_secs(n);
        let mut p = FallbackPolicy::new(t0);
        assert_eq!(p.decide(s(10), false, false), None);
        assert_eq!(p.decide(s(11), true, false), None);
        assert_eq!(p.decide(s(14), true, false), Some(true));
    }

    #[test]
    fn forget_allows_next_copy() {
        let mut d = Dedup::default();
        assert_eq!(d.offer(10, Some("a"), None), Verdict::New);
        d.forget(10);
        assert_eq!(d.offer(10, Some("a"), None), Verdict::New);
    }
}
