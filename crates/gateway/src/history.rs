//! History from the public checkpoint bucket
//! (`https://checkpoints.mainnet.sui.io/<seq>.binpb.zst`, ~30 days).
//!
//! The ring buffer is only a cache: anything a stream needs that the ring
//! does not hold is fetched from the bucket, rendered under the canonical
//! mask exactly like a fullnode renders `SubscribeCheckpoints` (sui-rpc-api
//! `render_checkpoint_message`), then masked, serialized and compressed with
//! the same functions as live checkpoints.
//!
//! Bucket objects are compact protobuf `sui.rpc.v2.Checkpoint`s. Rendering
//! them is CPU-heavy (~6–23 ms per checkpoint), so it runs on the blocking
//! pool behind a per-machine semaphore; live streams never wait on it.
//!
//! 404 handling: within `tip_margin` of the head a checkpoint may simply not
//! be uploaded yet (the bucket trails the chain by a few checkpoints), so it
//! is retried for up to `tip_wait`. Further back a 404 means the object is
//! gone (retention) and the checkpoint is reported as missing.

use std::ops::Range;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use prost::Message;
use sui_rpc::field::{FieldMask, FieldMaskTree, FieldMaskUtil};
use sui_rpc::merge::Merge;
use sui_rpc::proto::sui::rpc::v2::{Checkpoint, SubscribeCheckpointsResponse};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::encoding::Encoding;
use crate::hub::render_event;
use crate::mask::Mask;
use crate::metrics::Metrics;

#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL; objects are `<bucket_url>/<seq>.binpb.zst`.
    pub bucket_url: String,
    /// Concurrent bucket GETs per backfill.
    pub fetch_concurrency: usize,
    /// Concurrent renders per machine.
    pub render_concurrency: usize,
    /// Concurrent backfilling streams per machine.
    pub max_streams: usize,
    /// Oldest checkpoint served is `head + 1 - max_span`; older → `gap`.
    pub max_span: u64,
    /// Checkpoints this close to the head may not be uploaded yet.
    pub tip_margin: u64,
    /// How long a 404 near the head is retried.
    pub tip_wait: Duration,
    /// Attempts for other failures (5xx, timeouts, connection errors).
    pub attempts: u32,
    pub request_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bucket_url: "https://checkpoints.mainnet.sui.io".into(),
            fetch_concurrency: 16,
            render_concurrency: 1,
            max_streams: 8,
            max_span: 16_000,
            tip_margin: 200,
            tip_wait: Duration::from_secs(30),
            attempts: 6,
            request_timeout: Duration::from_secs(20),
        }
    }
}

/// Bucket object bytes → canonical checkpoint. Replaceable in tests.
pub type Renderer = Arc<dyn Fn(&[u8]) -> Result<Checkpoint, String> + Send + Sync>;

pub struct History {
    pub cfg: Config,
    client: reqwest::Client,
    renders: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    render: Renderer,
    metrics: Metrics,
}

/// Result of fetching one checkpoint.
#[derive(Debug)]
pub enum Fetched {
    Found(Bytes),
    /// Not in the bucket (below retention, or never uploaded in time).
    Missing,
}

/// One step of an ordered backfill.
#[derive(Debug)]
pub enum Item {
    /// The rendered, encoded SSE event.
    Event {
        seq: u64,
        bytes: Bytes,
    },
    Missing(u64),
    /// The bucket kept failing, or the object did not render.
    Failed {
        seq: u64,
        error: String,
    },
}

/// Which part of a missing range `[from, boundary)` is fetched, and which
/// part is too old to serve.
#[derive(Debug, PartialEq, Eq)]
pub struct Plan {
    pub gap: Option<(u64, u64)>,
    pub fetch: Range<u64>,
}

/// Plan `[from, boundary)` given the current head (`tip`): only the last
/// `max_span` checkpoints up to the head are served from the bucket.
pub fn plan(from: u64, boundary: u64, tip: u64, max_span: u64) -> Plan {
    if from >= boundary {
        return Plan {
            gap: None,
            fetch: boundary..boundary,
        };
    }
    let oldest = (tip + 1).saturating_sub(max_span);
    let start = from.max(oldest).min(boundary);
    Plan {
        gap: (from < start).then(|| (from, start - 1)),
        fetch: start..boundary,
    }
}

static CANONICAL_TREE: LazyLock<FieldMaskTree> = LazyLock::new(|| {
    FieldMaskTree::from_field_mask(&FieldMask::from_paths(common::CANONICAL_READ_MASK))
});

/// The Mysten rendering path: decode the compact proto, convert to the
/// executed-checkpoint form, then render it under the canonical mask the
/// way `SubscribeCheckpoints` does, including balance changes.
pub fn render_bucket_object(bytes: &[u8]) -> Result<Checkpoint, String> {
    let raw = zstd::stream::decode_all(bytes).map_err(|e| format!("zstd: {e}"))?;
    let proto = Checkpoint::decode(&raw[..]).map_err(|e| format!("decode: {e}"))?;
    let checkpoint = sui_types::full_checkpoint_content::Checkpoint::try_from(&proto)
        .map_err(|e| format!("convert: {e}"))?;
    drop(proto);
    let mut out = Checkpoint::merge_from(&checkpoint, &CANONICAL_TREE);
    if out.transactions.len() != checkpoint.transactions.len() {
        return Err("transaction count mismatch after render".into());
    }
    for (message, tx) in out.transactions.iter_mut().zip(&checkpoint.transactions) {
        message.balance_changes = sui_types::balance_change::derive_balance_changes_2(
            &tx.effects,
            &checkpoint.object_set,
        )
        .into_iter()
        .map(Into::into)
        .collect();
    }
    Ok(out)
}

/// A canonical checkpoint as a NATS-style payload (zstd
/// `SubscribeCheckpointsResponse`), for the hub's ring.
pub fn encode_payload(seq: u64, checkpoint: Checkpoint) -> Bytes {
    let mut response = SubscribeCheckpointsResponse::default();
    response.cursor = Some(seq);
    response.checkpoint = Some(checkpoint);
    common::exact_bytes(
        zstd::bulk::compress(&response.encode_to_vec(), 3).expect("zstd of in-memory data"),
    )
}

impl History {
    pub fn new(cfg: Config, metrics: Metrics) -> anyhow::Result<Self> {
        Self::with_renderer(cfg, metrics, Arc::new(render_bucket_object))
    }

    pub fn with_renderer(cfg: Config, metrics: Metrics, render: Renderer) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(64)
            .build()?;
        Ok(Self {
            renders: Arc::new(Semaphore::new(cfg.render_concurrency.max(1))),
            streams: Arc::new(Semaphore::new(cfg.max_streams)),
            client,
            render,
            metrics,
            cfg,
        })
    }

    /// A slot for a backfilling stream, if one is free.
    pub fn try_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.streams.clone().try_acquire_owned().ok()
    }

    pub async fn stream_permit(&self) -> OwnedSemaphorePermit {
        self.streams
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed")
    }

    async fn get_once(&self, seq: u64) -> Result<Option<Bytes>, String> {
        let url = format!("{}/{seq}.binpb.zst", self.cfg.bucket_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        match response.status() {
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            s if s.is_success() => response
                .bytes()
                .await
                .map(Some)
                .map_err(|e| format!("GET {url}: body: {e}")),
            s => Err(format!("GET {url}: HTTP {s}")),
        }
    }

    /// Fetch one object with retries. `tip` is the current head.
    pub async fn fetch(&self, seq: u64, tip: Option<u64>) -> Result<Fetched, String> {
        let near_tip = tip.is_none_or(|t| seq.saturating_add(self.cfg.tip_margin) > t);
        let started = Instant::now();
        let mut backoff = Duration::from_millis(250);
        let (mut errors, mut not_found) = (0u32, 0u32);
        loop {
            let t0 = Instant::now();
            let result = self.get_once(seq).await;
            self.metrics
                .backfill_fetch_seconds
                .observe(t0.elapsed().as_secs_f64());
            match result {
                Ok(Some(bytes)) => {
                    self.metrics.backfill_fetched.inc();
                    return Ok(Fetched::Found(bytes));
                }
                Ok(None) => {
                    not_found += 1;
                    let kind = if near_tip {
                        "not_found_tip"
                    } else {
                        "not_found"
                    };
                    self.metrics.bucket_errors.with_label_values(&[kind]).inc();
                    let give_up = if near_tip {
                        started.elapsed() >= self.cfg.tip_wait
                    } else {
                        // One re-check; then the object is gone.
                        not_found >= 2
                    };
                    if give_up {
                        return Ok(Fetched::Missing);
                    }
                    let wait = if near_tip {
                        backoff.min(Duration::from_secs(2))
                    } else {
                        Duration::from_millis(250)
                    };
                    tokio::time::sleep(wait).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
                Err(e) => {
                    errors += 1;
                    self.metrics
                        .bucket_errors
                        .with_label_values(&["error"])
                        .inc();
                    if errors >= self.cfg.attempts {
                        return Err(e);
                    }
                    tracing::debug!(seq, attempt = errors, "bucket: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(4));
                }
            }
        }
    }

    /// Run CPU-heavy work on the blocking pool, at most
    /// `render_concurrency` at a time per machine.
    pub async fn on_render_pool<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, String> {
        let permit = self
            .renders
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "render pool closed".to_owned())?;
        // The permit lives in the blocking task, so it is released when the
        // work ends even if nobody polls this future meanwhile.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Fetch and render one checkpoint into the canonical form.
    pub async fn canonical(
        &self,
        seq: u64,
        tip: Option<u64>,
    ) -> Result<Option<Checkpoint>, String> {
        let bytes = match self.fetch(seq, tip).await? {
            Fetched::Found(b) => b,
            Fetched::Missing => return Ok(None),
        };
        let render = self.render.clone();
        let metrics = self.metrics.clone();
        let checkpoint = self
            .on_render_pool(move || {
                let t0 = Instant::now();
                let r = render(&bytes).inspect_err(|_| {
                    metrics.bucket_errors.with_label_values(&["render"]).inc();
                });
                metrics
                    .backfill_render_seconds
                    .observe(t0.elapsed().as_secs_f64());
                r
            })
            .await??;
        check_seq(seq, &checkpoint)?;
        self.metrics.backfill_rendered.inc();
        Ok(Some(checkpoint))
    }

    /// A live hole's checkpoint as a hub payload, with its timestamp (unix
    /// ms). The head is near: 404s are retried for `tip_wait`.
    pub async fn live_payload(&self, seq: u64) -> Result<Option<(Bytes, u64)>, String> {
        let Some(checkpoint) = self.canonical(seq, None).await? else {
            return Ok(None);
        };
        let timestamp_ms = checkpoint
            .summary
            .as_ref()
            .and_then(|s| s.timestamp)
            .and_then(|t| sui_rpc::proto::proto_to_timestamp_ms(t).ok())
            .unwrap_or_default();
        let payload = self
            .on_render_pool(move || encode_payload(seq, checkpoint))
            .await?;
        Ok(Some((payload, timestamp_ms)))
    }

    /// One checkpoint as an SSE event under `mask`, in `encoding`.
    async fn event(
        &self,
        seq: u64,
        tip: Option<u64>,
        mask: Mask,
        encoding: Encoding,
    ) -> Result<Option<Bytes>, String> {
        let bytes = match self.fetch(seq, tip).await? {
            Fetched::Found(b) => b,
            Fetched::Missing => return Ok(None),
        };
        let render = self.render.clone();
        let metrics = self.metrics.clone();
        let event = self
            .on_render_pool(move || {
                let t0 = Instant::now();
                let canonical = render(&bytes).inspect_err(|_| {
                    metrics.bucket_errors.with_label_values(&["render"]).inc();
                })?;
                check_seq(seq, &canonical)?;
                let event = match &mask {
                    Mask::Full => render_event(seq, &canonical)?,
                    partial => render_event(seq, &partial.apply(&canonical))?,
                };
                let out = encoding.compress(&event);
                metrics
                    .backfill_render_seconds
                    .observe(t0.elapsed().as_secs_f64());
                Ok::<_, String>(out)
            })
            .await??;
        self.metrics.backfill_rendered.inc();
        Ok(Some(event))
    }

    /// `range` in order: fetched `fetch_concurrency` ahead, rendered on the
    /// shared pool. Each checkpoint is a task of its own, so fetches and
    /// renders progress (and release their permits) while the consumer is
    /// blocked on a slow client; dropping the stream aborts them.
    pub fn events(
        self: &Arc<Self>,
        range: Range<u64>,
        tip: Option<u64>,
        mask: Mask,
        encoding: Encoding,
    ) -> impl Stream<Item = Item> + Send + 'static {
        let this = self.clone();
        let concurrency = self.cfg.fetch_concurrency.max(1);
        futures::stream::iter(range)
            .map(move |seq| {
                let (this, mask) = (this.clone(), mask.clone());
                let task = AbortOnDrop(tokio::spawn(async move {
                    match this.event(seq, tip, mask, encoding).await {
                        Ok(Some(bytes)) => Item::Event { seq, bytes },
                        Ok(None) => Item::Missing(seq),
                        Err(error) => Item::Failed { seq, error },
                    }
                }));
                async move {
                    let mut task = task;
                    (&mut task.0).await.unwrap_or_else(|e| Item::Failed {
                        seq,
                        error: e.to_string(),
                    })
                }
            })
            .buffered(concurrency)
    }
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn check_seq(seq: u64, checkpoint: &Checkpoint) -> Result<(), String> {
    match checkpoint.sequence_number {
        Some(s) if s == seq => Ok(()),
        other => Err(format!("bucket object {seq} holds checkpoint {other:?}")),
    }
}

/// Collapses runs of missing checkpoints into `gap` ranges.
#[derive(Default)]
pub struct MissingRun(Option<(u64, u64)>);

impl MissingRun {
    /// Add a missing checkpoint; returns a finished run if `seq` does not
    /// extend the current one.
    pub fn push(&mut self, seq: u64) -> Option<(u64, u64)> {
        match &mut self.0 {
            Some((_, to)) if *to + 1 == seq => {
                *to = seq;
                None
            }
            run => run.replace((seq, seq)),
        }
    }

    pub fn take(&mut self) -> Option<(u64, u64)> {
        self.0.take()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn plans() {
        // Entirely within the span.
        assert_eq!(
            plan(100, 200, 1_000, 16_000),
            Plan {
                gap: None,
                fetch: 100..200
            }
        );
        // Older part becomes a gap: oldest served = 1001 - 500 = 501.
        assert_eq!(
            plan(100, 900, 1_000, 500),
            Plan {
                gap: Some((100, 500)),
                fetch: 501..900
            }
        );
        // Everything too old.
        assert_eq!(
            plan(100, 300, 10_000, 500),
            Plan {
                gap: Some((100, 299)),
                fetch: 300..300
            }
        );
        // Nothing missing.
        assert_eq!(plan(300, 300, 1_000, 500).fetch, 300..300);
        assert_eq!(plan(400, 300, 1_000, 500).gap, None);
        // Young chain.
        assert_eq!(plan(0, 10, 20, 16_000).gap, None);
    }

    #[test]
    fn missing_runs() {
        let mut r = MissingRun::default();
        assert_eq!(r.push(5), None);
        assert_eq!(r.push(6), None);
        assert_eq!(r.push(9), Some((5, 6)));
        assert_eq!(r.take(), Some((9, 9)));
        assert_eq!(r.take(), None);
    }

    /// A checkpoint object as the test renderer understands it: zstd of a
    /// plain proto `Checkpoint` (the real renderer needs full contents).
    pub fn object(seq: u64) -> Bytes {
        let mut cp = Checkpoint::default();
        cp.sequence_number = Some(seq);
        cp.digest = Some(format!("digest-{seq}"));
        zstd::bulk::compress(&cp.encode_to_vec(), 3).unwrap().into()
    }

    pub fn test_renderer() -> Renderer {
        Arc::new(|bytes: &[u8]| {
            let raw = zstd::stream::decode_all(bytes).map_err(|e| e.to_string())?;
            Checkpoint::decode(&raw[..]).map_err(|e| e.to_string())
        })
    }

    /// Behaviour of the mock bucket per checkpoint.
    #[derive(Clone)]
    pub enum Mode {
        /// 404 for the first `n` requests, then the object.
        LateUpload(u32),
        /// Always 404.
        Gone,
        /// 500 for the first `n` requests.
        Flaky(u32),
        /// Serves another checkpoint's object.
        Wrong(u64),
    }

    #[derive(Clone, Default)]
    pub struct MockBucket {
        pub modes: Arc<Mutex<HashMap<u64, Mode>>>,
        pub hits: Arc<Mutex<HashMap<u64, u32>>>,
        pub requests: Arc<AtomicU32>,
    }

    async fn serve_object(
        State(b): State<MockBucket>,
        Path(name): Path<String>,
    ) -> impl IntoResponse {
        b.requests.fetch_add(1, Ordering::Relaxed);
        let Some(seq) = name
            .strip_suffix(".binpb.zst")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return (StatusCode::BAD_REQUEST, Bytes::new());
        };
        let n = {
            let mut hits = b.hits.lock().unwrap();
            let h = hits.entry(seq).or_default();
            *h += 1;
            *h
        };
        let mode = b.modes.lock().unwrap().get(&seq).cloned();
        let not_found = || {
            (
                StatusCode::NOT_FOUND,
                Bytes::from_static(
                    b"<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchKey</Code></Error>",
                ),
            )
        };
        match mode {
            None => (StatusCode::OK, object(seq)),
            Some(Mode::LateUpload(k)) if n <= k => not_found(),
            Some(Mode::LateUpload(_)) => (StatusCode::OK, object(seq)),
            Some(Mode::Gone) => not_found(),
            Some(Mode::Flaky(k)) if n <= k => (StatusCode::INTERNAL_SERVER_ERROR, Bytes::new()),
            Some(Mode::Flaky(_)) => (StatusCode::OK, object(seq)),
            Some(Mode::Wrong(other)) => (StatusCode::OK, object(other)),
        }
    }

    impl MockBucket {
        /// Serve on an ephemeral port; returns the base URL.
        pub async fn serve(&self) -> String {
            let app = Router::new()
                .route("/{name}", get(serve_object))
                .with_state(self.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            format!("http://{addr}")
        }

        pub fn set(&self, seq: u64, mode: Mode) {
            self.modes.lock().unwrap().insert(seq, mode);
        }
    }

    pub fn test_config(url: String) -> Config {
        Config {
            bucket_url: url,
            tip_margin: 10,
            tip_wait: Duration::from_millis(1_500),
            attempts: 3,
            ..Config::default()
        }
    }

    pub async fn history(bucket: &MockBucket) -> Arc<History> {
        let cfg = test_config(bucket.serve().await);
        Arc::new(History::with_renderer(cfg, Metrics::new("test"), test_renderer()).unwrap())
    }

    async fn collect(h: &Arc<History>, range: Range<u64>, tip: u64) -> Vec<String> {
        h.events(range, Some(tip), Mask::Full, Encoding::Identity)
            .map(|item| match item {
                Item::Event { seq, bytes } => {
                    let text = std::str::from_utf8(&bytes).unwrap().to_owned();
                    assert!(text.starts_with(&format!("id: {seq}\nevent: checkpoint\ndata: {{")));
                    format!("ok {seq}")
                }
                Item::Missing(seq) => format!("missing {seq}"),
                Item::Failed { seq, .. } => format!("failed {seq}"),
            })
            .collect()
            .await
    }

    #[tokio::test]
    async fn ordered_with_404s_and_retries() {
        let bucket = MockBucket::default();
        // Old checkpoints (tip 1000): 404 = gone, after one re-check.
        bucket.set(101, Mode::Gone);
        bucket.set(102, Mode::Gone);
        // Transient server errors are retried.
        bucket.set(104, Mode::Flaky(2));
        // Delay an early one so later ones finish first: still in order.
        bucket.set(100, Mode::Flaky(1));
        let h = history(&bucket).await;
        let got = collect(&h, 100..106, 1_000).await;
        assert_eq!(
            got,
            [
                "ok 100",
                "missing 101",
                "missing 102",
                "ok 103",
                "ok 104",
                "ok 105"
            ]
        );
        assert_eq!(bucket.hits.lock().unwrap()[&101], 2);
        assert_eq!(bucket.hits.lock().unwrap()[&104], 3);
    }

    #[tokio::test]
    async fn near_tip_404_is_retried_until_uploaded() {
        let bucket = MockBucket::default();
        bucket.set(998, Mode::LateUpload(2));
        bucket.set(999, Mode::Gone);
        let h = history(&bucket).await;
        let got = collect(&h, 997..1_000, 1_000).await;
        // 998 appears after two 404s; 999 never does within tip_wait.
        assert_eq!(got, ["ok 997", "ok 998", "missing 999"]);
        assert!(bucket.hits.lock().unwrap()[&999] > 2);
    }

    #[tokio::test]
    async fn persistent_errors_and_wrong_objects_fail() {
        let bucket = MockBucket::default();
        bucket.set(11, Mode::Flaky(100));
        bucket.set(13, Mode::Wrong(14));
        let h = history(&bucket).await;
        let got = collect(&h, 10..14, 1_000).await;
        assert_eq!(got, ["ok 10", "failed 11", "ok 12", "failed 13"]);
        assert_eq!(bucket.hits.lock().unwrap()[&11], 3);
    }

    #[tokio::test]
    async fn masks_and_encodings_match_live_rendering() {
        let bucket = MockBucket::default();
        let h = history(&bucket).await;
        let mask = Mask::parse(Some("sequence_number")).unwrap();
        let mut s = Box::pin(h.events(5..6, Some(100), mask.clone(), Encoding::Zstd));
        let Some(Item::Event { bytes, .. }) = s.next().await else {
            panic!("no event")
        };
        // The live path: canonical payload in the hub, rendered by `Entry`.
        let canonical = test_renderer()(&object(5)).unwrap();
        let entry = crate::hub::Entry::new(5, encode_payload(5, canonical));
        let live = entry.event(&mask, Encoding::Zstd).await.unwrap();
        assert_eq!(bytes, live);
    }

    /// Normalize the two known differences between a bucket render and a
    /// fullnode: the aggregated checkpoint `signature` (each node keeps its
    /// own certificate) and the order of `transactions[].signatures` for
    /// sponsored transactions (the bucket keeps the stored order).
    pub fn normalize(mut v: serde_json::Value) -> serde_json::Value {
        if let Some(o) = v.as_object_mut() {
            o.remove("signature");
        }
        if let Some(txs) = v.get_mut("transactions").and_then(|t| t.as_array_mut()) {
            for tx in txs {
                if let Some(sigs) = tx.get_mut("signatures").and_then(|s| s.as_array_mut()) {
                    sigs.sort_by_key(|s| s.to_string());
                }
            }
        }
        v
    }

    /// Paths (array indices collapsed) where two JSON values differ.
    pub fn diff_paths(
        a: &serde_json::Value,
        b: &serde_json::Value,
        path: &str,
        out: &mut Vec<String>,
    ) {
        use serde_json::Value;
        match (a, b) {
            (Value::Object(x), Value::Object(y)) => {
                let keys: std::collections::BTreeSet<_> = x.keys().chain(y.keys()).collect();
                for k in keys {
                    let p = format!("{path}.{k}");
                    match (x.get(k), y.get(k)) {
                        (Some(u), Some(v)) => diff_paths(u, v, &p, out),
                        _ => out.push(p),
                    }
                }
            }
            (Value::Array(x), Value::Array(y)) if x.len() == y.len() => {
                for (u, v) in x.iter().zip(y) {
                    diff_paths(u, v, &format!("{path}[]"), out);
                }
            }
            _ if a != b => out.push(path.to_owned()),
            _ => {}
        }
    }

    /// Opt-in: render recent bucket checkpoints and compare them with
    /// fullnode `ListCheckpoints` under the canonical mask.
    /// `PARITY_N` [20] checkpoints, `PARITY_BACK` [2000] behind the
    /// fullnode's latest, `PARITY_FULLNODE` [https://fullnode.mainnet.sui.io].
    #[tokio::test]
    #[ignore]
    async fn parity_with_fullnode() {
        use sui_rpc::proto::sui::rpc::v2::{GetServiceInfoRequest, ListCheckpointsRequest};
        let n: u64 = std::env::var("PARITY_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);
        let back: u64 = std::env::var("PARITY_BACK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000);
        let url = std::env::var("PARITY_FULLNODE")
            .unwrap_or_else(|_| "https://fullnode.mainnet.sui.io".into());
        let mut client = sui_rpc::Client::new(url).unwrap();
        let info = client
            .ledger_client()
            .get_service_info(GetServiceInfoRequest::default())
            .await
            .unwrap()
            .into_inner();
        let start = info.checkpoint_height.unwrap() - back;
        let mut fullnode: Vec<Checkpoint> = vec![];
        // Pages: each request continues after the last checkpoint received.
        while (fullnode.len() as u64) < n {
            let next = start + fullnode.len() as u64;
            let mut req = ListCheckpointsRequest::default();
            req.read_mask = Some(FieldMask::from_paths(common::CANONICAL_READ_MASK));
            req.start_checkpoint = Some(next);
            req.end_checkpoint = Some(start + n);
            let mut stream = client
                .ledger_client()
                .list_checkpoints(req)
                .await
                .unwrap()
                .into_inner();
            let before = fullnode.len();
            while let Some(frame) = stream.next().await {
                let frame = frame.unwrap();
                if let Some(cp) = frame.checkpoint {
                    fullnode.push(cp);
                }
                if frame.end.is_some() {
                    break;
                }
            }
            assert!(fullnode.len() > before, "no progress at {next}");
        }
        assert_eq!(
            fullnode.len() as u64,
            n,
            "fullnode returned all checkpoints"
        );
        let h = History::new(Config::default(), Metrics::new("test")).unwrap();
        let (mut identical, mut normalized_equal, mut txs) = (0, 0, 0);
        let mut raw_diffs = std::collections::BTreeMap::<String, usize>::new();
        let mut total_ms = 0.0;
        for expected in fullnode {
            let seq = expected.sequence_number.unwrap();
            let Fetched::Found(bytes) = h.fetch(seq, Some(seq + 10_000)).await.unwrap() else {
                panic!("{seq} not in bucket")
            };
            let t0 = Instant::now();
            let rendered = render_bucket_object(&bytes).unwrap();
            total_ms += t0.elapsed().as_secs_f64() * 1e3;
            txs += rendered.transactions.len();
            let (a, b) = (
                serde_json::to_value(&rendered).unwrap(),
                serde_json::to_value(&expected).unwrap(),
            );
            if a == b {
                identical += 1;
            }
            let mut paths = vec![];
            diff_paths(&a, &b, "", &mut paths);
            for p in paths {
                *raw_diffs.entry(p).or_default() += 1;
            }
            let (na, nb) = (normalize(a), normalize(b));
            if na == nb {
                normalized_equal += 1;
            } else {
                let mut paths = vec![];
                diff_paths(&na, &nb, "", &mut paths);
                panic!("checkpoint {seq}: unexpected differences at {paths:?}");
            }
        }
        eprintln!(
            "parity: {n} checkpoints {start}..{} ({txs} txs): {identical} byte-identical JSON, \
             {normalized_equal} equal after ignoring the known diffs; render {:.1} ms/checkpoint",
            start + n - 1,
            total_ms / n as f64
        );
        eprintln!("raw diff paths (occurrences): {raw_diffs:?}");
        // The proto3 JSON the gateway sends for the fullnode copy and for
        // the bucket copy is the same encoder; equality above is on it.
        assert_eq!(normalized_equal, n as usize);
    }

    /// Opt-in: a checkpoint a few ahead of the fullnode's latest is not in
    /// the bucket yet (404); near the tip it is retried until uploaded.
    #[tokio::test]
    #[ignore]
    async fn near_tip_404_against_real_bucket() {
        use sui_rpc::proto::sui::rpc::v2::GetServiceInfoRequest;
        let mut client = sui_rpc::Client::new("https://fullnode.mainnet.sui.io").unwrap();
        let height = client
            .ledger_client()
            .get_service_info(GetServiceInfoRequest::default())
            .await
            .unwrap()
            .into_inner()
            .checkpoint_height
            .unwrap();
        let metrics = Metrics::new("test");
        let h = History::new(Config::default(), metrics.clone()).unwrap();
        let seq = height + 8;
        let t0 = Instant::now();
        let fetched = h.fetch(seq, Some(height)).await.unwrap();
        let not_found = metrics
            .bucket_errors
            .with_label_values(&["not_found_tip"])
            .get();
        eprintln!(
            "{seq} (height {height}): found={} after {:?}, {not_found} tip 404s",
            matches!(fetched, Fetched::Found(_)),
            t0.elapsed()
        );
        assert!(matches!(fetched, Fetched::Found(_)));
        assert!(not_found > 0, "expected the object to be missing at first");
        // Far below the tip a 404 is final after one re-check.
        let gone = h.fetch(1, Some(height)).await.unwrap();
        assert!(matches!(gone, Fetched::Missing));
    }

    /// The real renderer on a bucket object stored in the repository.
    #[test]
    fn renders_real_bucket_object() {
        let bytes = include_bytes!("../testdata/checkpoint.binpb.zst");
        let cp = render_bucket_object(bytes).unwrap();
        let seq = cp.sequence_number.unwrap();
        assert!(cp.digest.is_some() && cp.summary.is_some() && cp.contents.is_some());
        assert!(!cp.transactions.is_empty());
        for tx in &cp.transactions {
            assert!(tx.digest.is_some() && tx.effects.is_some());
            assert_eq!(tx.checkpoint, Some(seq));
            // Never filled for checkpoints; dropped from the canonical mask.
            assert!(tx.objects.is_none() && tx.transaction_index.is_none());
        }
        assert!(cp.objects.is_some());
        let json = serde_json::to_string(&cp).unwrap();
        assert!(json.contains("\"balanceChanges\""));
    }
}
