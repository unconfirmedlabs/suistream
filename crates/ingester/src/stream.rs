//! Upstream subscription supervisor.
//!
//! Keeps one live `SubscribeCheckpoints` stream and hands every checkpoint to
//! the publisher without ever waiting on it: if the publisher is behind, the
//! frame is dropped and later backfilled. Upstream drops subscribers that
//! stop reading, so the read loop must never block.
//!
//! With `reconnect_before`, a replacement stream is opened on a fresh
//! connection once the current one reaches that age; the old stream is
//! dropped as soon as the replacement delivers its first frame
//! (make-before-break). Overlapping frames are deduplicated by the publisher.

use std::time::Duration;

use futures::StreamExt;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::metrics::Metrics;
use crate::upstream::Upstream;

/// Minimum delay before reopening a stream, even after a routine reset.
const MIN_RECONNECT_DELAY: Duration = Duration::from_millis(100);
/// Cap for exponential backoff after repeated quick failures.
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(10);
/// A stream that lived at least this long ended "routinely" (e.g. connection
/// max age); reconnect immediately.
const ROUTINE_AGE: Duration = Duration::from_secs(10);
/// Give up on a replacement stream that has not delivered within this time.
const REPLACEMENT_TIMEOUT: Duration = Duration::from_secs(15);

enum ConnEvent {
    Frame(u64, Box<Checkpoint>),
    Ended(u64, tonic::Status),
}

struct Conn {
    id: u64,
    opened: Instant,
    task: JoinHandle<()>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn run(upstream: Upstream, out: mpsc::Sender<Checkpoint>, metrics: Metrics) {
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let mut next_id = 0u64;
    let mut open = |metrics: &Metrics| {
        next_id += 1;
        metrics.streams_opened.inc();
        Conn {
            id: next_id,
            opened: Instant::now(),
            task: tokio::spawn(read_stream(upstream.clone(), next_id, events_tx.clone())),
        }
    };

    let rotate_after = upstream.source().reconnect_before;
    let mut active = Some(open(&metrics));
    let mut replacement: Option<Conn> = None;
    let mut reopen_at: Option<Instant> = None;
    let mut backoff = MIN_RECONNECT_DELAY;
    let mut tick = tokio::time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            Some(event) = events.recv() => match event {
                ConnEvent::Frame(id, checkpoint) => {
                    if replacement.as_ref().is_some_and(|c| c.id == id) {
                        tracing::debug!(id, "replacement stream live; dropping old stream");
                        active = replacement.take();
                        metrics.rotations.inc();
                    }
                    if active.as_ref().is_some_and(|c| c.id == id) {
                        backoff = MIN_RECONNECT_DELAY;
                        if out.try_send(*checkpoint).is_err() {
                            metrics.handoff_dropped.inc();
                        }
                    }
                }
                ConnEvent::Ended(id, status) => {
                    if replacement.as_ref().is_some_and(|c| c.id == id) {
                        tracing::warn!(code = ?status.code(), "replacement stream failed: {}", status.message());
                        replacement = None;
                        continue;
                    }
                    let Some(conn) = active.take_if(|c| c.id == id) else { continue };
                    let age = conn.opened.elapsed();
                    let routine = age >= ROUTINE_AGE;
                    metrics
                        .stream_ends
                        .with_label_values(&[format!("{:?}", status.code()).as_str(), if routine { "true" } else { "false" }])
                        .inc();
                    if routine {
                        tracing::info!(age = ?age, code = ?status.code(), "stream ended: {}", status.message());
                    } else {
                        tracing::warn!(age = ?age, code = ?status.code(), "stream failed: {}", status.message());
                    }
                    if let Some(next) = replacement.take() {
                        active = Some(next);
                        continue;
                    }
                    let delay = if routine { MIN_RECONNECT_DELAY } else { backoff };
                    if !routine {
                        backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
                    }
                    reopen_at = Some(Instant::now() + delay);
                }
            },
            _ = tick.tick() => {
                let now = Instant::now();
                if active.is_none() && reopen_at.is_some_and(|t| now >= t) {
                    reopen_at = None;
                    active = Some(open(&metrics));
                }
                if replacement.as_ref().is_some_and(|c| c.opened.elapsed() > REPLACEMENT_TIMEOUT) {
                    tracing::warn!("replacement stream did not deliver; retrying later");
                    replacement = None;
                }
                if let (Some(limit), Some(conn), None) = (rotate_after, active.as_ref(), replacement.as_ref())
                    && conn.opened.elapsed() >= limit
                {
                    replacement = Some(open(&metrics));
                }
            }
        }
    }
}

async fn read_stream(upstream: Upstream, id: u64, events: mpsc::UnboundedSender<ConnEvent>) {
    let status = match upstream.subscribe().await {
        Ok(mut stream) => loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    // Unfiltered streams always carry the checkpoint.
                    if let Some(checkpoint) = frame.checkpoint
                        && events
                            .send(ConnEvent::Frame(id, Box::new(checkpoint)))
                            .is_err()
                    {
                        return;
                    }
                }
                Some(Err(status)) => break status,
                None => break tonic::Status::unavailable("stream ended"),
            }
        },
        Err(status) => status,
    };
    let _ = events.send(ConnEvent::Ended(id, status));
}
