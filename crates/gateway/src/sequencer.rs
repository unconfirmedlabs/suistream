//! Turns deduplicated arrivals into a strictly ordered, gap-free emission.
//!
//! Core NATS preserves order per publisher, so each subject's progress tells
//! us which checkpoints it has already passed. Once every active subject has
//! passed checkpoint `N` without it reaching us (dropped by NATS, lost in a
//! reconnect, or never published), `N` is fetched from the ingesters right
//! away; otherwise the head is held at most `max_hold`. If the ingesters
//! cannot fill it (or it is too large for them), it is filled from the
//! checkpoint bucket. Only a hole that the bucket cannot fill either is
//! emitted as an explicit gap, never skipped silently.
//!
//! Nothing carries over a restart: a new gateway starts at the first
//! checkpoint it receives, and older history is the bucket's job.

use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::fetch::Fetcher;
use crate::history::History;
use crate::hub::{Entry, Hub};
use crate::metrics::Metrics;

#[derive(Clone, Debug)]
pub struct Incoming {
    pub seq: u64,
    pub source: Arc<str>,
    pub timestamp_ms: u64,
    pub payload: Bytes,
}

#[derive(Debug)]
pub enum Action {
    Emit(Incoming),
    Fetch(RangeInclusive<u64>),
    /// Fill from the checkpoint bucket.
    BucketFill(RangeInclusive<u64>),
    Gap(RangeInclusive<u64>),
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Longest the head waits for a missing checkpoint before fetching.
    pub max_hold: Duration,
    /// Per-attempt fetch timeout.
    pub fetch_timeout: Duration,
    pub fetch_attempts: u32,
    /// Holes larger than this are emitted as gaps without fetching.
    pub max_hole: u64,
    /// Subjects silent for longer than this are ignored for gap detection.
    pub active_window: Duration,
    /// Whether holes may be filled from the bucket.
    pub bucket: bool,
    /// How long a bucket fill may hold the head.
    pub bucket_timeout: Duration,
    /// Holes larger than this are gaps even with the bucket.
    pub max_bucket_hole: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_hold: Duration::from_millis(500),
            fetch_timeout: Duration::from_secs(2),
            fetch_attempts: 3,
            max_hole: common::MAX_FETCH_RANGE,
            active_window: Duration::from_secs(5),
            bucket: true,
            bucket_timeout: Duration::from_secs(45),
            max_bucket_hole: 1_000,
        }
    }
}

struct InFlight {
    range: RangeInclusive<u64>,
    deadline: Instant,
    attempts: u32,
    /// Filling from the bucket (the last resort).
    bucket: bool,
}

pub struct Sequencer {
    cfg: Config,
    next: Option<u64>,
    pending: BTreeMap<u64, Incoming>,
    progress: HashMap<Arc<str>, (u64, Instant)>,
    missing_since: Option<Instant>,
    fetch: Option<InFlight>,
}

impl Sequencer {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            next: None,
            pending: BTreeMap::new(),
            progress: HashMap::new(),
            missing_since: None,
            fetch: None,
        }
    }

    /// Next checkpoint to emit.
    #[cfg(test)]
    pub fn next(&self) -> Option<u64> {
        self.next
    }

    pub fn on_checkpoint(&mut self, incoming: Incoming, out: &mut Vec<Action>) {
        // A new gateway starts at the tip, like an upstream subscription.
        let next = *self.next.get_or_insert(incoming.seq);
        if incoming.seq < next {
            return;
        }
        self.pending.entry(incoming.seq).or_insert(incoming);
        self.drain(out);
    }

    pub fn on_progress(&mut self, subject: Arc<str>, seq: u64, now: Instant) {
        let entry = self.progress.entry(subject).or_insert((seq, now));
        entry.0 = entry.0.max(seq);
        entry.1 = now;
    }

    /// A fetch finished; if the hole is still there the attempt failed.
    pub fn on_fetch_done(&mut self, range: &RangeInclusive<u64>, now: Instant) {
        if let Some(f) = &mut self.fetch
            && f.range == *range
        {
            f.deadline = now;
        }
    }

    fn drain(&mut self, out: &mut Vec<Action>) {
        let Some(mut next) = self.next else { return };
        while let Some(incoming) = self.pending.remove(&next) {
            out.push(Action::Emit(incoming));
            next += 1;
        }
        self.next = Some(next);
        if self.fetch.as_ref().is_some_and(|f| *f.range.end() < next) {
            self.fetch = None;
        }
    }

    fn skip_to(&mut self, next: u64, out: &mut Vec<Action>) {
        self.next = Some(next);
        self.fetch = None;
        self.missing_since = None;
        self.pending = self.pending.split_off(&next);
        self.drain(out);
    }

    /// `(start, end, every active subject has passed start)` of the hole at
    /// the head, if one is known to exist.
    fn hole(&self, now: Instant) -> Option<(u64, u64, bool)> {
        let next = self.next?;
        let active = || {
            self.progress
                .values()
                .filter(|(_, at)| now.duration_since(*at) <= self.cfg.active_window)
        };
        let progress_max = active().map(|(s, _)| *s).max();
        let end = match self.pending.keys().next() {
            Some(&first) => first - 1,
            None => progress_max.filter(|&m| m >= next)?,
        };
        let all_passed = active().count() > 0 && active().all(|(s, _)| *s >= next);
        Some((next, end, all_passed))
    }

    /// Decide on fetches and gaps. `force` fetches a known hole immediately
    /// (e.g. after a NATS slow-consumer or reconnect event).
    pub fn poll(&mut self, now: Instant, force: bool, out: &mut Vec<Action>) {
        let Some((start, end, all_passed)) = self.hole(now) else {
            self.missing_since = None;
            return;
        };
        let since = *self.missing_since.get_or_insert(now);
        let size = end - start + 1;
        let bucket_range = start..=end.min(start + self.cfg.max_bucket_hole - 1);
        if size > self.cfg.max_hole && !self.fetch.as_ref().is_some_and(|f| f.bucket) {
            if self.cfg.bucket && size <= self.cfg.max_bucket_hole {
                self.start_bucket_fill(bucket_range, now, out);
            } else {
                out.push(Action::Gap(start..=end));
                self.skip_to(end + 1, out);
            }
            return;
        }
        let range = start..=end.min(start + common::MAX_FETCH_RANGE - 1);
        match &mut self.fetch {
            Some(f) if now < f.deadline => {}
            Some(f) if f.bucket || (f.attempts >= self.cfg.fetch_attempts && !self.cfg.bucket) => {
                let gap_end = *f.range.end();
                out.push(Action::Gap(start..=gap_end));
                self.skip_to(gap_end + 1, out);
            }
            Some(f) if f.attempts >= self.cfg.fetch_attempts => {
                let range = start..=end.min(start + self.cfg.max_bucket_hole - 1);
                self.start_bucket_fill(range, now, out);
            }
            Some(f) => {
                f.attempts += 1;
                f.deadline = now + self.cfg.fetch_timeout;
                f.range = range.clone();
                out.push(Action::Fetch(range));
            }
            None => {
                if force || all_passed || now.duration_since(since) >= self.cfg.max_hold {
                    self.fetch = Some(InFlight {
                        range: range.clone(),
                        deadline: now + self.cfg.fetch_timeout,
                        attempts: 1,
                        bucket: false,
                    });
                    out.push(Action::Fetch(range));
                }
            }
        }
    }
}

impl Sequencer {
    fn start_bucket_fill(
        &mut self,
        range: RangeInclusive<u64>,
        now: Instant,
        out: &mut Vec<Action>,
    ) {
        self.fetch = Some(InFlight {
            range: range.clone(),
            deadline: now + self.cfg.bucket_timeout,
            attempts: 1,
            bucket: true,
        });
        out.push(Action::BucketFill(range));
    }
}

pub enum Input {
    Checkpoint(Incoming),
    Progress {
        subject: Arc<str>,
        seq: u64,
    },
    /// NATS reported a slow consumer or reconnected: look for holes now.
    GapCheck,
    FetchDone(RangeInclusive<u64>),
}

/// Shared view of the emitted head.
#[derive(Default)]
pub struct Head {
    /// Last emitted checkpoint + 1 (0 = nothing yet).
    next: AtomicU64,
    pub last_emit_ms: AtomicU64,
}

impl Head {
    /// Last emitted checkpoint.
    pub fn last(&self) -> Option<u64> {
        self.next.load(Ordering::Relaxed).checked_sub(1)
    }

    #[cfg(test)]
    pub fn set_last(&self, seq: u64) {
        self.next.store(seq + 1, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    mut rx: mpsc::UnboundedReceiver<Input>,
    tx: mpsc::UnboundedSender<Input>,
    input_bytes: Arc<AtomicUsize>,
    hub: Arc<Hub>,
    head: Arc<Head>,
    fetcher: Fetcher,
    history: Option<Arc<History>>,
    metrics: Metrics,
) {
    let mut seq = Sequencer::new(Config {
        bucket: history.is_some(),
        ..Config::default()
    });
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut out = Vec::new();
    loop {
        let mut force = false;
        tokio::select! {
            input = rx.recv() => match input {
                Some(Input::Checkpoint(incoming)) => {
                    input_bytes.fetch_sub(incoming.payload.len(), Ordering::Relaxed);
                    seq.on_checkpoint(incoming, &mut out);
                }
                Some(Input::Progress { subject, seq: s }) => seq.on_progress(subject, s, Instant::now()),
                Some(Input::GapCheck) => force = true,
                Some(Input::FetchDone(range)) => seq.on_fetch_done(&range, Instant::now()),
                None => return,
            },
            _ = tick.tick() => {}
        }
        seq.poll(Instant::now(), force, &mut out);
        for action in out.drain(..) {
            match action {
                Action::Emit(incoming) => {
                    metrics.emitted.with_label_values(&[&incoming.source]).inc();
                    if incoming.timestamp_ms > 0 {
                        let lag = common::unix_millis().saturating_sub(incoming.timestamp_ms);
                        metrics.emit_lag.observe(lag as f64 / 1000.0);
                    }
                    metrics.head.set(incoming.seq as i64);
                    head.next.store(incoming.seq + 1, Ordering::Relaxed);
                    head.last_emit_ms
                        .store(common::unix_millis(), Ordering::Relaxed);
                    hub.emit(Arc::new(Entry::new(incoming.seq, incoming.payload)));
                }
                Action::Fetch(range) => {
                    metrics.fetches.inc();
                    tracing::debug!(?range, "gap-fill fetch");
                    fetcher.spawn(range, tx.clone());
                }
                Action::BucketFill(range) => {
                    tracing::warn!(?range, "filling live hole from the bucket");
                    if let Some(history) = &history {
                        spawn_bucket_fill(
                            history.clone(),
                            range,
                            tx.clone(),
                            input_bytes.clone(),
                            metrics.clone(),
                        );
                    }
                }
                Action::Gap(range) => {
                    metrics.gaps.inc();
                    metrics
                        .gap_checkpoints
                        .inc_by(range.end() - range.start() + 1);
                    tracing::error!(?range, "unfillable gap");
                    head.next.store(range.end() + 1, Ordering::Relaxed);
                    hub.gap(*range.start(), *range.end());
                }
            }
        }
    }
}

/// Fill a live hole from the bucket: each checkpoint is rendered into the
/// canonical payload and fed back as an arrival.
fn spawn_bucket_fill(
    history: Arc<History>,
    range: RangeInclusive<u64>,
    tx: mpsc::UnboundedSender<Input>,
    input_bytes: Arc<AtomicUsize>,
    metrics: Metrics,
) {
    use futures::StreamExt;
    tokio::spawn(async move {
        let concurrency = history.cfg.fetch_concurrency.max(1);
        let mut results = futures::stream::iter(range.clone())
            .map(|seq| {
                let history = history.clone();
                async move { (seq, history.live_payload(seq).await) }
            })
            .buffer_unordered(concurrency);
        let (mut filled, mut failed) = (0u64, 0u64);
        while let Some((seq, result)) = results.next().await {
            match result {
                Ok(Some((payload, timestamp_ms))) => {
                    filled += 1;
                    input_bytes.fetch_add(payload.len(), Ordering::Relaxed);
                    let _ = tx.send(Input::Checkpoint(Incoming {
                        seq,
                        source: "bucket".into(),
                        timestamp_ms,
                        payload,
                    }));
                }
                Ok(None) => failed += 1,
                Err(e) => {
                    failed += 1;
                    tracing::warn!(seq, "bucket fill: {e}");
                }
            }
        }
        metrics
            .live_fills
            .with_label_values(&[if failed == 0 {
                "complete"
            } else {
                "incomplete"
            }])
            .inc();
        tracing::info!(?range, filled, failed, "bucket fill done");
        let _ = tx.send(Input::FetchDone(range));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inc(seq: u64) -> Incoming {
        Incoming {
            seq,
            source: "s".into(),
            timestamp_ms: 0,
            payload: Bytes::new(),
        }
    }

    fn emitted(out: &mut Vec<Action>) -> Vec<u64> {
        out.drain(..)
            .filter_map(|a| match a {
                Action::Emit(i) => Some(i.seq),
                _ => None,
            })
            .collect()
    }

    fn cfg() -> Config {
        Config {
            bucket: false,
            ..Config::default()
        }
    }

    #[test]
    fn in_order_and_reordered() {
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(12), &mut out);
        s.on_checkpoint(inc(13), &mut out);
        assert_eq!(emitted(&mut out), vec![10]);
        s.on_checkpoint(inc(11), &mut out);
        assert_eq!(emitted(&mut out), vec![11, 12, 13]);
        // Late duplicates and older checkpoints are ignored.
        s.on_checkpoint(inc(12), &mut out);
        s.on_checkpoint(inc(5), &mut out);
        assert!(out.is_empty());
        assert_eq!(s.next(), Some(14));
    }

    #[test]
    fn fetches_once_all_subjects_passed() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        out.clear();
        s.on_progress("a".into(), 10, t0);
        s.on_progress("b".into(), 10, t0);
        // `a` delivers 12; 11 never arrived from `a`. `b` is behind.
        s.on_checkpoint(inc(12), &mut out);
        s.on_progress("a".into(), 12, t0);
        s.poll(t0, false, &mut out);
        assert!(out.is_empty(), "b may still deliver 11");
        // `b` passes 11 too (its 11 was lost): fetch immediately.
        s.on_progress("b".into(), 12, t0);
        s.poll(t0, false, &mut out);
        assert!(matches!(&out[..], [Action::Fetch(r)] if *r == (11..=11)));
        out.clear();
        // In flight: no duplicate fetch.
        s.poll(t0 + Duration::from_millis(100), false, &mut out);
        assert!(out.is_empty());
        // Fill arrives.
        s.on_checkpoint(inc(11), &mut out);
        assert_eq!(emitted(&mut out), vec![11, 12]);
    }

    #[test]
    fn max_hold_backstop() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(12), &mut out);
        out.clear();
        s.on_progress("slow".into(), 10, t0);
        s.poll(t0, false, &mut out);
        assert!(out.is_empty());
        s.poll(t0 + Duration::from_millis(499), false, &mut out);
        assert!(out.is_empty());
        s.poll(t0 + Duration::from_millis(500), false, &mut out);
        assert!(matches!(&out[..], [Action::Fetch(r)] if *r == (11..=11)));
    }

    #[test]
    fn force_fetches_immediately() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(13), &mut out);
        out.clear();
        s.poll(t0, true, &mut out);
        assert!(matches!(&out[..], [Action::Fetch(r)] if *r == (11..=12)));
    }

    #[test]
    fn hole_detected_from_progress_without_pending() {
        // Everything after 10 was lost, but subjects report progress.
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        out.clear();
        s.on_progress("a".into(), 12, t0);
        s.poll(t0, false, &mut out);
        assert!(matches!(&out[..], [Action::Fetch(r)] if *r == (11..=12)));
    }

    #[test]
    fn failed_fetches_become_a_gap() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(12), &mut out);
        out.clear();
        s.poll(t0, true, &mut out);
        assert_eq!(out.len(), 1);
        out.clear();
        let mut t = t0;
        for _ in 1..3 {
            s.on_fetch_done(&(11..=11), t);
            s.poll(t, false, &mut out);
            assert!(matches!(&out[..], [Action::Fetch(_)]));
            out.clear();
            t += Duration::from_millis(10);
        }
        // Third attempt times out.
        s.poll(t + Duration::from_secs(3), false, &mut out);
        assert!(matches!(&out[0], Action::Gap(r) if *r == (11..=11)));
        assert!(matches!(&out[1], Action::Emit(i) if i.seq == 12));
        assert_eq!(s.next(), Some(13));
    }

    #[test]
    fn failed_fetches_go_to_the_bucket_then_gap() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(Config {
            bucket: true,
            ..cfg()
        });
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(12), &mut out);
        out.clear();
        s.poll(t0, true, &mut out);
        let mut t = t0;
        for _ in 0..3 {
            out.clear();
            t += Duration::from_secs(3);
            s.poll(t, false, &mut out);
        }
        assert!(matches!(&out[..], [Action::BucketFill(r)] if *r == (11..=11)));
        out.clear();
        // The bucket fill holds the head (no gap) until it reports.
        s.poll(t + Duration::from_secs(10), false, &mut out);
        assert!(out.is_empty());
        // Success: the checkpoint arrives as a normal input.
        s.on_checkpoint(inc(11), &mut out);
        assert_eq!(emitted(&mut out), vec![11, 12]);

        // Another hole that the bucket cannot fill either.
        s.on_checkpoint(inc(14), &mut out);
        s.poll(t, true, &mut out);
        for _ in 0..3 {
            out.clear();
            t += Duration::from_secs(3);
            s.poll(t, false, &mut out);
        }
        assert!(matches!(&out[..], [Action::BucketFill(r)] if *r == (13..=13)));
        out.clear();
        s.on_fetch_done(&(13..=13), t);
        s.poll(t, false, &mut out);
        assert!(matches!(&out[0], Action::Gap(r) if *r == (13..=13)));
        assert!(matches!(&out[1], Action::Emit(i) if i.seq == 14));
    }

    #[test]
    fn large_hole_goes_to_the_bucket() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(Config {
            bucket: true,
            ..cfg()
        });
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(500), &mut out);
        out.clear();
        s.poll(t0, false, &mut out);
        assert!(matches!(&out[..], [Action::BucketFill(r)] if *r == (11..=499)));
        out.clear();
        // Still too large for the bucket: a gap right away.
        s.on_checkpoint(inc(5_000), &mut out);
        s.on_fetch_done(&(11..=499), t0);
        for seq in 11..500 {
            s.on_checkpoint(inc(seq), &mut out);
        }
        out.clear();
        s.poll(t0, false, &mut out);
        assert!(matches!(&out[0], Action::Gap(r) if *r == (501..=4_999)));
    }

    #[test]
    fn first_arrival_after_start_is_the_head() {
        // A restarted gateway has no memory: whatever arrives first is the
        // start, even if other subjects report progress far ahead.
        let t0 = Instant::now();
        let mut s = Sequencer::new(Config {
            bucket: true,
            ..cfg()
        });
        let mut out = vec![];
        s.on_progress("a".into(), 1_000, t0);
        s.on_checkpoint(inc(1_000), &mut out);
        assert_eq!(emitted(&mut out), vec![1_000]);
        s.poll(t0, true, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn huge_hole_is_a_gap() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        s.on_checkpoint(inc(500), &mut out);
        out.clear();
        s.poll(t0, false, &mut out);
        assert!(matches!(&out[0], Action::Gap(r) if *r == (11..=499)));
        assert!(matches!(&out[1], Action::Emit(i) if i.seq == 500));
    }

    #[test]
    fn stale_subjects_do_not_block() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.on_checkpoint(inc(10), &mut out);
        out.clear();
        s.on_progress("dead".into(), 3, t0);
        let t1 = t0 + Duration::from_secs(10);
        s.on_progress("a".into(), 12, t1);
        s.on_checkpoint(inc(12), &mut out);
        out.clear();
        s.poll(t1, false, &mut out);
        assert!(matches!(&out[..], [Action::Fetch(r)] if *r == (11..=11)));
    }

    #[test]
    fn no_hole_no_action() {
        let t0 = Instant::now();
        let mut s = Sequencer::new(cfg());
        let mut out = vec![];
        s.poll(t0, true, &mut out);
        s.on_checkpoint(inc(10), &mut out);
        s.on_progress("a".into(), 10, t0);
        out.clear();
        s.poll(t0 + Duration::from_secs(5), true, &mut out);
        assert!(out.is_empty());
    }
}
