//! `GET /v1/checkpoints`: the public SSE stream.
//!
//! - `read_mask=<paths>` (comma-separated, default: full canonical checkpoint)
//! - `from=<seq>` or `Last-Event-ID: <seq>` (resume after it) → served from
//!   the checkpoint bucket for what the ring does not hold (up to the max
//!   span behind the head; older → a `gap` event), then from the ring, then
//!   live, strictly in order without duplicates. The ring is only a cache.
//! - Events: `id: <seq>`, `event: checkpoint`, `data: <proto3 JSON Checkpoint>`.
//!   `event: gap` / `data: {"from":a,"to":b}` reports checkpoints that cannot
//!   be delivered; a live gap is followed by closing the stream.
//! - `Accept-Encoding`: zstd, gzip or identity. Every event is compressed once
//!   and the bytes are shared by all streams (see `encoding`).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::App;
use crate::encoding::Encoding;
use crate::history::{self, Item, MissingRun};
use crate::hub::{HubEvent, gap_event};
use crate::limits::{Admission, Pool, authorize};
use crate::mask::Mask;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// A client that accepts nothing for this long is disconnected.
const SEND_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Deserialize)]
pub struct Params {
    read_mask: Option<String>,
    from: Option<u64>,
    key: Option<String>,
}

const RETRY: &[u8] = b"retry: 1000\n\n";
const HEARTBEAT: &[u8] = b": keepalive\n\n";

/// Fixed pieces of the stream, compressed once per encoding at startup.
pub struct Statics {
    pub retry: Bytes,
    pub heartbeat: Bytes,
}

impl Statics {
    pub fn new(encoding: Encoding) -> Self {
        Self {
            retry: encoding.compress(RETRY),
            heartbeat: encoding.compress(HEARTBEAT),
        }
    }
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

pub async fn checkpoints(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if let Err(reason) = authorize(bearer, params.key.as_deref()) {
        app.metrics
            .streams_rejected
            .with_label_values(&["auth"])
            .inc();
        return error(StatusCode::UNAUTHORIZED, reason);
    }
    let mask = match Mask::parse(params.read_mask.as_deref()) {
        Ok(m) => m,
        Err(e) => {
            app.metrics
                .streams_rejected
                .with_label_values(&["mask"])
                .inc();
            return error(StatusCode::BAD_REQUEST, e);
        }
    };
    let accept_encoding = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok());
    let encoding = Encoding::negotiate(accept_encoding);
    let pool = if encoding == Encoding::Identity {
        Pool::Identity
    } else {
        Pool::Compressed
    };
    let resume_from = match headers
        .get("last-event-id")
        .map(|v| v.to_str().map(str::parse::<u64>))
    {
        Some(Ok(Ok(last))) if last < u64::MAX => Some(last + 1),
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "Last-Event-ID must be a checkpoint number",
            );
        }
        None => params.from,
    };
    if *app.shutdown.borrow() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "shutting down");
    }
    // A resume older than the ring (or any resume while the ring is still
    // empty) backfills from the bucket; those streams are capped per
    // machine. Rejected clients retry (elsewhere).
    let mut backfill = None;
    if let (Some(from), Some(history)) = (resume_from, &app.history)
        && app.hub.first().is_none_or(|first| from < first)
    {
        match history.try_stream_permit() {
            Some(p) => backfill = Some(p),
            None => {
                app.metrics
                    .streams_rejected
                    .with_label_values(&["backfill"])
                    .inc();
                let mut resp = error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "backfill limit reached, retry shortly",
                );
                resp.headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                return resp;
            }
        }
    }
    let admission = match app.limits.admit(pool, mask.key()) {
        Ok(a) => a,
        Err(rejection) => {
            app.metrics
                .streams_rejected
                .with_label_values(&[rejection.label()])
                .inc();
            let mut resp = error(
                StatusCode::TOO_MANY_REQUESTS,
                format!("stream limit reached ({})", rejection.label()),
            );
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            return resp;
        }
    };
    app.metrics
        .masks_active
        .set(app.limits.masks_active() as i64);
    tracing::info!(
        encoding = encoding.label(),
        accept_encoding = accept_encoding.unwrap_or(""),
        mask = mask_label(&mask),
        "stream opened"
    );

    let content_encoding = encoding.content_encoding();
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    let guard = crate::StreamGuard::new(app.clone());
    tokio::spawn(stream_task(
        app.clone(),
        admission,
        guard,
        mask,
        encoding,
        resume_from,
        backfill,
        tx,
    ));

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "text/event-stream")
        // No `no-transform`: Cloudflare may re-encode (e.g. gzip → zstd).
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::VARY, "Accept-Encoding")
        .header("x-accel-buffering", "no");
    if let Some(ce) = content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, ce);
    }
    let body = Body::from_stream(futures::StreamExt::map(
        ReceiverStream::new(rx),
        Ok::<_, std::convert::Infallible>,
    ));
    builder.body(body).expect("valid response")
}

fn mask_label(mask: &Mask) -> String {
    match mask.key() {
        crate::mask::MaskKey::Full => "*".to_owned(),
        crate::mask::MaskKey::Partial(k) => k.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_task(
    app: Arc<App>,
    admission: Admission,
    guard: crate::StreamGuard,
    mask: Mask,
    encoding: Encoding,
    resume_from: Option<u64>,
    backfill: Option<OwnedSemaphorePermit>,
    tx: mpsc::Sender<Bytes>,
) {
    let gauge = app.metrics.streams.with_label_values(&[encoding.label()]);
    gauge.inc();
    let reason = run_stream(&app, &mask, encoding, resume_from, backfill, &tx).await;
    tracing::debug!(reason, "stream closed");
    gauge.dec();
    drop(guard);
    app.metrics
        .streams_closed
        .with_label_values(&[reason])
        .inc();
    drop(admission);
    app.metrics
        .masks_active
        .set(app.limits.masks_active() as i64);
}

/// Returns the close reason.
///
/// `next` is the next checkpoint owed to the client. While it is older than
/// the ring, it is served from the bucket (without holding a live
/// subscription, so a long backfill cannot lag one); then, with a live
/// subscription taken before the ring snapshot, from the ring and live.
/// If the ring moved on meanwhile, the loop backfills again.
async fn run_stream(
    app: &App,
    mask: &Mask,
    encoding: Encoding,
    resume_from: Option<u64>,
    mut backfill: Option<OwnedSemaphorePermit>,
    tx: &mpsc::Sender<Bytes>,
) -> &'static str {
    let statics = &app.statics[&encoding];
    let sent = app
        .metrics
        .bytes_sent
        .with_label_values(&[encoding.label()]);
    macro_rules! send {
        ($bytes:expr) => {{
            let bytes: Bytes = $bytes;
            let len = bytes.len();
            match tokio::time::timeout(SEND_TIMEOUT, tx.send(bytes)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return "client",
                Err(_) => return "slow_consumer",
            }
            sent.inc_by(len as u64);
        }};
    }
    macro_rules! render {
        ($entry:expr) => {
            match $entry.event(mask, encoding).await {
                Ok(b) => b,
                Err(e) => {
                    app.metrics.render_errors.inc();
                    tracing::error!(seq = $entry.seq, "render failed: {e}");
                    return "render_error";
                }
            }
        };
    }
    macro_rules! gap {
        ($from:expr, $to:expr) => {
            encoding.compress(&gap_event($from, $to))
        };
    }

    if let Some(p) = encoding.preamble() {
        send!(p);
    }
    send!(statics.retry.clone());

    let mut shutdown = app.shutdown.clone();
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
        HEARTBEAT_INTERVAL,
    );
    let mut next: Option<u64> = resume_from;
    let mut backfilling = None;
    let mut live = loop {
        if let Some(n) = next {
            // Cold start: wait (with heartbeats) for the first live
            // checkpoint, which is where the bucket hands over.
            let Some(first) = app.hub.first() else {
                let mut rx = app.hub.subscribe();
                if app.hub.first().is_none() {
                    tokio::select! {
                        _ = rx.recv() => {}
                        _ = heartbeat.tick() => send!(statics.heartbeat.clone()),
                        _ = async { let _ = shutdown.wait_for(|s| *s).await; } => return "shutdown",
                        _ = tx.closed() => return "client",
                    }
                }
                continue;
            };
            if n < first {
                let Some(history) = &app.history else {
                    send!(gap!(n, first - 1));
                    next = Some(first);
                    continue;
                };
                if backfill.is_none() {
                    // The ring moved past us mid-stream: wait for a slot.
                    let acquire = history.stream_permit();
                    tokio::pin!(acquire);
                    backfill = loop {
                        tokio::select! {
                            p = &mut acquire => break Some(p),
                            _ = heartbeat.tick() => send!(statics.heartbeat.clone()),
                            _ = async { let _ = shutdown.wait_for(|s| *s).await; } => return "shutdown",
                            _ = tx.closed() => return "client",
                        }
                    };
                }
                if backfilling.is_none() {
                    backfilling = Some(BackfillGauge::new(app));
                }
                let tip = app.head.last().unwrap_or(first - 1).max(first - 1);
                let plan = history::plan(n, first, tip, history.cfg.max_span);
                if let Some((a, b)) = plan.gap {
                    app.metrics
                        .backfill_gaps
                        .with_label_values(&["span"])
                        .inc_by(b - a + 1);
                    send!(gap!(a, b));
                }
                tracing::debug!(from = plan.fetch.start, to = plan.fetch.end, "backfill");
                let events = history.events(plan.fetch.clone(), Some(tip), mask.clone(), encoding);
                tokio::pin!(events);
                let mut missing = MissingRun::default();
                loop {
                    let item = tokio::select! {
                        item = futures::StreamExt::next(&mut events) => match item {
                            Some(item) => item,
                            None => break,
                        },
                        _ = heartbeat.tick() => {
                            send!(statics.heartbeat.clone());
                            continue;
                        }
                        _ = async { let _ = shutdown.wait_for(|s| *s).await; } => return "shutdown",
                        // Stop fetching and rendering for a client that left.
                        _ = tx.closed() => return "client",
                    };
                    match item {
                        Item::Event { seq, bytes } => {
                            tracing::trace!(seq, "backfilled");
                            if let Some((a, b)) = missing.take() {
                                send!(gap!(a, b));
                            }
                            send!(bytes);
                            heartbeat.reset();
                        }
                        Item::Missing(seq) => {
                            app.metrics.backfill_missing.inc();
                            app.metrics
                                .backfill_gaps
                                .with_label_values(&["missing"])
                                .inc();
                            if let Some((a, b)) = missing.push(seq) {
                                send!(gap!(a, b));
                            }
                        }
                        Item::Failed { seq, error } => {
                            // Not declared lost: the client resumes from
                            // its last checkpoint and the bucket is retried.
                            tracing::warn!(seq, "backfill failed: {error}");
                            return "bucket_error";
                        }
                    }
                }
                if let Some((a, b)) = missing.take() {
                    send!(gap!(a, b));
                }
                next = Some(first);
                continue;
            }
        }
        // Far behind within the ring: catch up without a live subscription,
        // which would lag out while thousands of entries are rendered.
        if let Some(n) = next {
            let (first, entries) = app.hub.replay_from(n);
            if first.is_some_and(|f| n >= f) && entries.len() > app.hub.lag_capacity() / 2 {
                drop(backfill.take());
                drop(backfilling.take());
                for entry in entries {
                    send!(render!(entry));
                    app.hub.release_if_stale(&entry);
                    next = Some(entry.seq + 1);
                }
                heartbeat.reset();
                continue;
            }
        }
        // Subscribe before taking the snapshot so nothing falls between.
        let rx = app.hub.subscribe();
        let (first, entries) = app.hub.replay_from(next.unwrap_or(u64::MAX));
        if let Some(n) = next
            && first.is_none_or(|f| n < f)
        {
            // Evicted (or cleared by a gap) since we looked.
            continue;
        }
        drop(backfill.take());
        drop(backfilling.take());
        for entry in entries {
            send!(render!(entry));
            app.hub.release_if_stale(&entry);
            next = Some(entry.seq + 1);
        }
        break rx;
    };

    loop {
        tokio::select! {
            event = live.recv() => match event {
                Ok(HubEvent::Checkpoint(entry)) => {
                    if next.is_some_and(|n| entry.seq < n) {
                        continue;
                    }
                    send!(render!(entry));
                    next = Some(entry.seq + 1);
                    heartbeat.reset();
                }
                Ok(HubEvent::Gap { from, to }) => {
                    send!(gap!(from, to));
                    return "gap";
                }
                Err(broadcast::error::RecvError::Lagged(_)) => return "slow_consumer",
                Err(broadcast::error::RecvError::Closed) => return "shutdown",
            },
            _ = heartbeat.tick() => send!(statics.heartbeat.clone()),
            _ = async { let _ = shutdown.wait_for(|s| *s).await; } => return "shutdown",
            _ = tx.closed() => return "client",
        }
    }
}

/// `backfill_streams` while a stream is backfilling.
struct BackfillGauge(prometheus::IntGauge);

impl BackfillGauge {
    fn new(app: &App) -> Self {
        app.metrics.backfill_streams.inc();
        Self(app.metrics.backfill_streams.clone())
    }
}

impl Drop for BackfillGauge {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::history::tests::{MockBucket, Mode, test_config, test_renderer};
    use crate::history::{self, History};
    use crate::hub::{Entry, Hub};
    use crate::limits::{Limits, LimitsConfig};
    use crate::metrics::Metrics;
    use crate::sequencer::Head;
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    pub async fn test_app(history: Option<Arc<History>>, ring_entries: usize) -> Arc<App> {
        let nats = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("127.0.0.1:1")
            .await
            .unwrap();
        let ring_bytes = crate::hub::tests::payload(1_000).len() * ring_entries;
        Arc::new(App {
            hub: Arc::new(Hub::new(32, ring_bytes)),
            limits: Limits::new(LimitsConfig {
                max_compressed: 10,
                max_identity: 10,
                max_masks: 10,
            }),
            head: Arc::new(Head::default()),
            health_max: AtomicU64::new(0),
            health_at_ms: AtomicU64::new(0),
            ready_max_behind: 20,
            nats,
            metrics: Metrics::new("test"),
            shutdown: {
                let (tx, rx) = tokio::sync::watch::channel(false);
                std::mem::forget(tx);
                rx
            },
            statics: [Encoding::Identity, Encoding::Zstd, Encoding::Gzip]
                .into_iter()
                .map(|e| (e, Statics::new(e)))
                .collect(),
            history,
            active_streams: AtomicUsize::new(0),
            streams_changed_ms: AtomicU64::new(0),
        })
    }

    async fn history_with(bucket: &MockBucket, max_span: u64) -> Arc<History> {
        let cfg = history::Config {
            max_span,
            ..test_config(bucket.serve().await)
        };
        Arc::new(History::with_renderer(cfg, Metrics::new("test"), test_renderer()).unwrap())
    }

    fn emit(app: &App, seq: u64) {
        app.hub
            .emit(Arc::new(Entry::new(seq, crate::hub::tests::payload(seq))));
        app.head.set_last(seq);
    }

    /// Run a stream and return its events (`<seq>` or `gap a-b`) until
    /// `until` was delivered.
    async fn run(app: Arc<App>, resume_from: Option<u64>, until: u64) -> Vec<String> {
        run_slow(app, resume_from, until, Duration::ZERO).await
    }

    /// `run` with a client that takes `read_delay` per chunk.
    async fn run_slow(
        app: Arc<App>,
        resume_from: Option<u64>,
        until: u64,
        read_delay: Duration,
    ) -> Vec<String> {
        let backfill = app.history.as_ref().and_then(|h| h.try_stream_permit());
        let (tx, mut rx) = mpsc::channel(4);
        let task = tokio::spawn({
            let app = app.clone();
            async move {
                run_stream(
                    &app,
                    &Mask::Full,
                    Encoding::Identity,
                    resume_from,
                    backfill,
                    &tx,
                )
                .await
            }
        });
        let mut text = String::new();
        let mut events = vec![];
        let deadline = tokio::time::sleep(Duration::from_secs(20));
        tokio::pin!(deadline);
        'outer: loop {
            let chunk = tokio::select! {
                c = rx.recv() => c.expect("stream ended early"),
                _ = &mut deadline => panic!("timeout; got {events:?}"),
            };
            if !read_delay.is_zero() {
                tokio::time::sleep(read_delay).await;
            }
            text.push_str(std::str::from_utf8(&chunk).unwrap());
            while let Some(end) = text.find("\n\n") {
                let event: String = text.drain(..end + 2).collect();
                if let Some(id) = event.strip_prefix("id: ") {
                    let seq: u64 = id.split('\n').next().unwrap().parse().unwrap();
                    assert!(event.contains(&format!("\"sequenceNumber\":\"{seq}\"")));
                    events.push(seq.to_string());
                    if seq >= until {
                        break 'outer;
                    }
                } else if let Some(data) = event.strip_prefix("event: gap\ndata: ") {
                    let v: serde_json::Value = serde_json::from_str(data.trim()).unwrap();
                    events.push(format!("gap {}-{}", v["from"], v["to"]));
                }
            }
        }
        task.abort();
        events
    }

    fn seqs(range: std::ops::RangeInclusive<u64>) -> Vec<String> {
        range.map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn resume_from_bucket_then_ring_then_live() {
        let bucket = MockBucket::default();
        let app = test_app(Some(history_with(&bucket, 16_000).await), 100).await;
        for seq in 1_000..1_010 {
            emit(&app, seq);
        }
        let live = {
            let app = app.clone();
            tokio::spawn(async move {
                for seq in 1_010..1_020 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    emit(&app, seq);
                }
            })
        };
        let got = run(app.clone(), Some(950), 1_019).await;
        assert_eq!(got, seqs(950..=1_019));
        live.await.unwrap();
        // Only the part older than the ring came from the bucket.
        assert_eq!(bucket.hits.lock().unwrap().len(), 50);
        assert!(!bucket.hits.lock().unwrap().contains_key(&1_000));
    }

    #[tokio::test]
    async fn missing_objects_and_max_span_become_gaps() {
        let bucket = MockBucket::default();
        bucket.set(960, Mode::Gone);
        bucket.set(961, Mode::Gone);
        let app = test_app(Some(history_with(&bucket, 100).await), 100).await;
        for seq in 1_000..1_050 {
            emit(&app, seq);
        }
        // Head 1049, span 100: oldest served is 950.
        let got = run(app, Some(900), 1_049).await;
        let mut want = vec!["gap 900-949".to_owned()];
        want.extend(seqs(950..=959));
        want.push("gap 960-961".into());
        want.extend(seqs(962..=1_049));
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn cold_start_waits_for_live_then_backfills() {
        let bucket = MockBucket::default();
        let app = test_app(Some(history_with(&bucket, 16_000).await), 100).await;
        let live = {
            let app = app.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                for seq in 2_000..2_005 {
                    emit(&app, seq);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        };
        let got = run(app, Some(1_990), 2_004).await;
        assert_eq!(got, seqs(1_990..=2_004));
        live.await.unwrap();
    }

    #[tokio::test]
    async fn ring_eviction_during_backfill_is_backfilled_again() {
        let bucket = MockBucket::default();
        // Slow the backfill so the 3-entry ring moves on meanwhile.
        for seq in 500..520 {
            bucket.set(seq, Mode::Flaky(1));
        }
        let app = test_app(Some(history_with(&bucket, 16_000).await), 3).await;
        for seq in 520..523 {
            emit(&app, seq);
        }
        let live = {
            let app = app.clone();
            tokio::spawn(async move {
                for seq in 523..560 {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    emit(&app, seq);
                }
            })
        };
        let got = run(app, Some(500), 559).await;
        assert_eq!(got, seqs(500..=559));
        live.await.unwrap();
        // 520.. had left the ring and were fetched in a second round.
        assert!(bucket.hits.lock().unwrap().contains_key(&525));
    }

    #[tokio::test]
    async fn deep_ring_replay_does_not_lag_out() {
        // Replaying 300 ring entries takes longer than the live broadcast
        // buffer (32) lasts at this emit rate; the stream must catch up
        // from the ring before subscribing instead of lagging out.
        let app = test_app(None, 1_000).await;
        for seq in 1_000..1_300 {
            emit(&app, seq);
        }
        let live = {
            let app = app.clone();
            tokio::spawn(async move {
                for seq in 1_300..1_400 {
                    tokio::time::sleep(Duration::from_millis(4)).await;
                    emit(&app, seq);
                }
            })
        };
        let got = run_slow(app, Some(1_000), 1_399, Duration::from_millis(1)).await;
        assert_eq!(got, seqs(1_000..=1_399));
        live.await.unwrap();
    }

    #[tokio::test]
    async fn without_history_old_resumes_get_a_gap() {
        let app = test_app(None, 100).await;
        for seq in 1_000..1_005 {
            emit(&app, seq);
        }
        let got = run(app, Some(990), 1_004).await;
        let mut want = vec!["gap 990-999".to_owned()];
        want.extend(seqs(1_000..=1_004));
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn future_resume_skips_older_live() {
        let app = test_app(None, 100).await;
        emit(&app, 100);
        let live = {
            let app = app.clone();
            tokio::spawn(async move {
                for seq in 101..110 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    emit(&app, seq);
                }
            })
        };
        let got = run(app, Some(105), 109).await;
        assert_eq!(got, seqs(105..=109));
        live.await.unwrap();
    }
}
