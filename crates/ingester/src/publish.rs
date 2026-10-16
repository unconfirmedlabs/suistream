//! Ordering, backfill and NATS publishing.
//!
//! The publisher keeps the published sequence contiguous: a live checkpoint
//! that skips ahead first triggers a bounded `GetCheckpoint` backfill of the
//! hole, published before the live checkpoint. Checkpoints at or below the
//! last published one (rotation overlap, replays) are dropped.

use std::collections::VecDeque;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::HeaderMap;
use futures::StreamExt;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use tokio::sync::mpsc;

use crate::frame::Frame;
use crate::metrics::Metrics;
use crate::upstream::Upstream;

/// Largest hole backfilled after a reconnect; older checkpoints are left to
/// the gateways' gap-fill (served by other ingesters).
pub const MAX_BACKFILL: u64 = 50;
/// Concurrent `GetCheckpoint` calls during a backfill.
const BACKFILL_CONCURRENCY: usize = 8;
/// A backfill gives up after this long so live checkpoints are not held back.
const BACKFILL_BUDGET: Duration = Duration::from_secs(20);

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Duplicate,
    Publish {
        backfill: Option<RangeInclusive<u64>>,
        /// Checkpoints in the hole that are not backfilled.
        skipped: u64,
    },
}

/// Tracks the last published checkpoint.
#[derive(Default)]
pub struct Sequencer {
    last: Option<u64>,
}

impl Sequencer {
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// Decide what to do with a live checkpoint `seq`.
    pub fn on_live(&self, seq: u64) -> Decision {
        match self.last {
            None => Decision::Publish {
                backfill: None,
                skipped: 0,
            },
            Some(last) if seq <= last => Decision::Duplicate,
            Some(last) if seq == last + 1 => Decision::Publish {
                backfill: None,
                skipped: 0,
            },
            Some(last) => {
                let hole = seq - last - 1;
                let start = seq - hole.min(MAX_BACKFILL);
                Decision::Publish {
                    backfill: Some(start..=seq - 1),
                    skipped: hole - hole.min(MAX_BACKFILL),
                }
            }
        }
    }

    pub fn published(&mut self, seq: u64) {
        self.last = Some(self.last.map_or(seq, |l| l.max(seq)));
    }
}

/// Recently published frames, served to gateway fetches.
pub struct Ring {
    frames: VecDeque<Arc<Frame>>,
    bytes: usize,
    max_bytes: usize,
}

impl Ring {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    pub fn push(&mut self, frame: Arc<Frame>) {
        // Frames are pushed in ascending order.
        if self.frames.back().is_some_and(|f| f.seq >= frame.seq) {
            return;
        }
        self.bytes += frame.payload.len();
        self.frames.push_back(frame);
        while self.bytes > self.max_bytes && self.frames.len() > 1 {
            let old = self.frames.pop_front().expect("non-empty");
            self.bytes -= old.payload.len();
        }
    }

    pub fn get(&self, seq: u64) -> Option<Arc<Frame>> {
        let first = self.frames.front()?.seq;
        let idx = usize::try_from(seq.checked_sub(first)?).ok()?;
        // Contiguous in the common case; fall back to search if not.
        match self.frames.get(idx) {
            Some(f) if f.seq == seq => Some(f.clone()),
            _ => self
                .frames
                .binary_search_by_key(&seq, |f| f.seq)
                .ok()
                .map(|i| self.frames[i].clone()),
        }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

#[derive(Clone)]
pub struct Publisher {
    pub nats: async_nats::Client,
    pub subject: String,
    pub source: String,
    pub region: String,
    pub ring: Arc<Mutex<Ring>>,
    pub metrics: Metrics,
}

impl Publisher {
    pub fn headers(&self, frame: &Frame) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(common::HEADER_CHECKPOINT, frame.seq.to_string().as_str());
        headers.insert(common::HEADER_DIGEST, frame.digest.as_str());
        headers.insert(
            common::HEADER_TIMESTAMP,
            frame.timestamp_ms.to_string().as_str(),
        );
        headers.insert(common::HEADER_SOURCE, self.source.as_str());
        headers.insert(common::HEADER_REGION, self.region.as_str());
        headers.insert(common::HEADER_ENCODING, common::ENCODING_ZSTD);
        headers
    }

    /// Publish a frame to `subject`, chunked if it exceeds the NATS limit.
    pub async fn send(&self, subject: &str, frame: &Frame) -> Result<()> {
        let parts = common::chunk::split(&frame.payload, common::MAX_CHUNK_SIZE);
        let n = parts.len();
        if n > 1 {
            self.metrics.chunked.inc();
        }
        for (i, part) in parts.into_iter().enumerate() {
            let mut headers = self.headers(frame);
            if n > 1 {
                headers.insert(
                    common::HEADER_CHUNK,
                    common::chunk::format_header(i, n).as_str(),
                );
            }
            self.nats
                .publish_with_headers(subject.to_owned(), headers, part)
                .await
                .context("publish")?;
        }
        Ok(())
    }

    async fn publish(&self, checkpoint: Checkpoint) -> Result<u64> {
        let started = std::time::Instant::now();
        let frame = tokio::task::spawn_blocking(move || Frame::from_checkpoint(checkpoint))
            .await
            .context("encode task")??;
        self.metrics
            .encode_seconds
            .observe(started.elapsed().as_secs_f64());
        self.metrics.raw_bytes.observe(frame.raw_len as f64);
        self.metrics
            .payload_bytes
            .observe(frame.payload.len() as f64);
        if frame.timestamp_ms > 0 {
            let age = common::unix_millis().saturating_sub(frame.timestamp_ms);
            self.metrics.checkpoint_age.observe(age as f64 / 1000.0);
        }
        let frame = Arc::new(frame);
        if let Err(e) = self.send(&self.subject, &frame).await {
            self.metrics.publish_errors.inc();
            return Err(e);
        }
        self.ring.lock().unwrap().push(frame.clone());
        self.metrics.published.inc();
        self.metrics.latest_seq.set(frame.seq as i64);
        Ok(frame.seq)
    }
}

/// Fetch a checkpoint, retrying transient failures.
pub async fn fetch_with_retry(upstream: &Upstream, seq: u64) -> Result<Option<Checkpoint>> {
    let mut delay = Duration::from_millis(200);
    for attempt in 1..=3 {
        match upstream.get_checkpoint(seq).await {
            Ok(cp) => return Ok(cp),
            Err(status) if attempt < 3 => {
                tracing::debug!(seq, code = ?status.code(), "GetCheckpoint retry: {}", status.message());
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(status) => return Err(status.into()),
        }
    }
    unreachable!()
}

/// Consume live checkpoints in arrival order and publish them contiguously.
pub async fn run(
    publisher: Publisher,
    upstream: Upstream,
    mut live: mpsc::Receiver<Checkpoint>,
    latest: tokio::sync::watch::Sender<Option<u64>>,
) {
    let metrics = publisher.metrics.clone();
    let mut seq_state = Sequencer::default();
    while let Some(checkpoint) = live.recv().await {
        let Some(seq) = checkpoint.sequence_number else {
            continue;
        };
        let (backfill, skipped) = match seq_state.on_live(seq) {
            Decision::Duplicate => {
                metrics.duplicates.inc();
                continue;
            }
            Decision::Publish { backfill, skipped } => (backfill, skipped),
        };
        if skipped > 0 {
            tracing::warn!(
                seq,
                skipped,
                "gap too large to backfill; left to gateway gap-fill"
            );
            metrics.backfill_skipped.inc_by(skipped);
        }
        if let Some(range) = backfill {
            let fetches = futures::stream::iter(range)
                .map(|missing| {
                    let upstream = &upstream;
                    async move { (missing, fetch_with_retry(upstream, missing).await) }
                })
                .buffered(BACKFILL_CONCURRENCY);
            let deadline = tokio::time::sleep(BACKFILL_BUDGET);
            tokio::pin!(fetches, deadline);
            loop {
                let (missing, result) = tokio::select! {
                    next = fetches.next() => match next { Some(r) => r, None => break },
                    _ = &mut deadline => {
                        tracing::warn!(seq, "backfill budget exhausted");
                        metrics.backfill_failures.inc();
                        break;
                    }
                };
                match result {
                    Ok(Some(cp)) => match publisher.publish(cp).await {
                        Ok(s) => {
                            seq_state.published(s);
                            metrics.backfilled.inc();
                        }
                        Err(e) => tracing::warn!(missing, "publish backfill: {e:#}"),
                    },
                    Ok(None) => {
                        metrics.backfill_failures.inc();
                        tracing::warn!(missing, "backfill: source does not have checkpoint");
                    }
                    Err(e) => {
                        metrics.backfill_failures.inc();
                        tracing::warn!(missing, "backfill failed: {e:#}");
                    }
                }
            }
        }
        match publisher.publish(checkpoint).await {
            Ok(s) => {
                seq_state.published(s);
                latest.send_replace(seq_state.last());
            }
            Err(e) => tracing::warn!(seq, "publish failed: {e:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_checkpoint_publishes() {
        let s = Sequencer::default();
        assert_eq!(
            s.on_live(10),
            Decision::Publish {
                backfill: None,
                skipped: 0
            }
        );
    }

    #[test]
    fn consecutive_and_duplicates() {
        let mut s = Sequencer::default();
        s.published(10);
        assert_eq!(
            s.on_live(11),
            Decision::Publish {
                backfill: None,
                skipped: 0
            }
        );
        // Rotation overlap: the replacement stream replays recent frames.
        assert_eq!(s.on_live(10), Decision::Duplicate);
        assert_eq!(s.on_live(3), Decision::Duplicate);
    }

    #[test]
    fn gap_is_backfilled_before_live() {
        let mut s = Sequencer::default();
        s.published(10);
        assert_eq!(
            s.on_live(14),
            Decision::Publish {
                backfill: Some(11..=13),
                skipped: 0
            }
        );
    }

    #[test]
    fn huge_gap_is_bounded() {
        let mut s = Sequencer::default();
        s.published(10);
        let seq = 10 + MAX_BACKFILL + 51;
        assert_eq!(
            s.on_live(seq),
            Decision::Publish {
                backfill: Some(seq - MAX_BACKFILL..=seq - 1),
                skipped: 50
            }
        );
    }

    #[test]
    fn published_is_monotonic() {
        let mut s = Sequencer::default();
        s.published(10);
        s.published(5);
        assert_eq!(s.last(), Some(10));
    }

    fn frame(seq: u64, len: usize) -> Arc<Frame> {
        Arc::new(Frame {
            seq,
            digest: String::new(),
            timestamp_ms: 0,
            raw_len: len,
            payload: vec![0; len].into(),
        })
    }

    #[test]
    fn ring_evicts_by_bytes_and_finds() {
        let mut ring = Ring::new(30);
        for seq in 1..=5 {
            ring.push(frame(seq, 10));
        }
        assert_eq!(ring.len(), 3);
        assert!(ring.get(2).is_none());
        assert_eq!(ring.get(4).unwrap().seq, 4);
        assert!(ring.get(6).is_none());
        // Out-of-order pushes are ignored.
        ring.push(frame(4, 10));
        assert_eq!(ring.len(), 3);
        // Non-contiguous contents still resolve.
        ring.push(frame(9, 10));
        assert_eq!(ring.get(9).unwrap().seq, 9);
        assert!(ring.get(7).is_none());
    }
}
